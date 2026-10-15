"""
Employee Attendance & Analytics API
===================================

FastAPI + MongoDB (PyMongo). All application code lives in this single file.

Run:   uvicorn app.main:app --port 8000
Env:   MONGO_URI, MONGO_DB  (real environment variables win over a local .env)

Design notes you can defend in the walkthrough
-----------------------------------------------
* PyMongo is run synchronously. FastAPI dispatches sync path operations to a
  threadpool, so parallel requests are served concurrently. Correctness under
  concurrency is therefore NOT provided by async/await; it is provided by the
  database: a unique index + a single atomic write per request (insert /
  find_one_and_update), never read-then-write.
* The Mongo client is created with tz_aware=True, so datetimes come back as
  timezone-aware UTC instants instead of PyMongo's default *naive* datetimes.
  We convert UTC<->IST ourselves for every rule that is defined in IST (R1-R3).
* Every instant crossing the HTTP boundary is an integer of epoch milliseconds.
  We truncate to whole seconds on the way in (R1) and store BSON datetimes.
* Derived fields (work_hours, late_minutes, overtime_minutes, half_day) are
  written only by the API and are AUTHORITATIVE; analytics read the stored
  values and never recompute them from punch times.
* Rounding is half-up everywhere (R4/R8). Mongo's $round and Python's round()
  are banker's rounding, so we never use them for reported numbers: we use
  Decimal(ROUND_HALF_UP) in Python and an explicit floor(x*10^n + 0.5)/10^n
  expression in the pipelines (all reported numbers here are non-negative).
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone, date as date_cls
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Annotated, Any, Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query
from pydantic import BaseModel, Field, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")
IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
PRESENCE_STATUSES = ["PRESENT", "WFH", "ON_DUTY"]
HALF_DAY_THRESHOLD = Decimal("4.50")
GRACE_SECONDS = 10 * 60          
OVERTIME_MIN_MINUTES = 30        
EPOCH_MS_MIN = 100_000_000_000
EPOCH_MS_MAX = 4_102_444_800_000
client = MongoClient(MONGO_URI, tz_aware=True)
db = client[MONGO_DB]
def ms_to_dt(ms: int) -> datetime:
    secs = ms // 1000
    return datetime.fromtimestamp(secs, tz=UTC)
def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    """Aware (or naive-UTC) datetime -> epoch ms. Naive is assumed UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp()) * 1000
def now_utc() -> datetime:
    return datetime.now(tz=UTC).replace(microsecond=0)
def to_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(IST)
def round_half_up(value: float | Decimal, places: int) -> float:
    q = Decimal(1).scaleb(-places)  
    return float(Decimal(str(value)).quantize(q, rounding=ROUND_HALF_UP))
def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start
def _ist_at(day: date_cls, hhmm: str) -> datetime:
    h, m = map(int, hhmm.split(":"))
    return datetime(day.year, day.month, day.day, h, m, tzinfo=IST)
def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    pi = to_ist(punch_in)
    day = pi.date()
    if is_overnight(shift_start, shift_end):
        eh, em = map(int, shift_end.split(":"))
        if (pi.hour, pi.minute, pi.second) < (eh, em, 0):
            day = day - timedelta(days=1)
    return day.isoformat()
def compute_late_minutes(punch_in: datetime, date_str: str, shift_start: str) -> int:
    day = date_cls.fromisoformat(date_str)
    start = _ist_at(day, shift_start)
    elapsed = int((to_ist(punch_in) - start).total_seconds())
    if elapsed > GRACE_SECONDS:
        return elapsed // 60
    return 0
def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: (out - in) seconds / 3600, rounded to 2 decimals half-up."""
    seconds = int((punch_out - punch_in).total_seconds())
    hours = Decimal(seconds) / Decimal(3600)
    return float(hours.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
def compute_overtime_minutes(
    punch_out: datetime, date_str: str, shift_start: str, shift_end: str
) -> int:
    day = date_cls.fromisoformat(date_str)
    end_day = day + timedelta(days=1) if is_overnight(shift_start, shift_end) else day
    end = _ist_at(end_day, shift_end)
    seconds = int((to_ist(punch_out) - end).total_seconds())
    if seconds <= 0:
        return 0
    minutes = seconds // 60
    return minutes if minutes >= OVERTIME_MIN_MINUTES else 0
def compute_half_day(work_hours: Optional[float]) -> bool:
    """R5: true when the rounded work_hours < 4.50."""
    if work_hours is None:
        return False
    return Decimal(str(work_hours)) < HALF_DAY_THRESHOLD
def _history_to_api(history: list[dict]) -> list[dict]:
    out = []
    for h in history or []:
        changes = {}
        for field, ch in (h.get("changes") or {}).items():
            if field in ("punch_in", "punch_out"):
                changes[field] = {
                    "from": dt_to_ms(ch.get("from")),
                    "to": dt_to_ms(ch.get("to")),
                }
            else:
                changes[field] = {"from": ch.get("from"), "to": ch.get("to")}
        out.append(
            {
                "at": dt_to_ms(h.get("at")),
                "by": h.get("by"),
                "reason": h.get("reason"),
                "changes": changes,
            }
        )
    return out
def record_to_api(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": int(doc.get("late_minutes", 0) or 0),
        "overtime_minutes": int(doc.get("overtime_minutes", 0) or 0),
        "half_day": bool(doc.get("half_day", False)),
        "history": _history_to_api(doc.get("history", [])),
    }
def employee_to_api(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": dt_to_ms(doc.get("created_at")),
    }
EmpCode = Annotated[str, Field(pattern=r"^EMP\d{4,6}$")]
HHMM = Annotated[str, Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")]
IsoDate = Annotated[str, Field(pattern=r"^\d{4}-\d{2}-\d{2}$")]
EpochMillis = Annotated[int, Field(strict=True, ge=EPOCH_MS_MIN, le=EPOCH_MS_MAX)]
class Status(str, Enum):
    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    LEAVE = "LEAVE"
    WFH = "WFH"
    ON_DUTY = "ON_DUTY"
class PresenceStatus(str, Enum):
    PRESENT = "PRESENT"
    WFH = "WFH"
    ON_DUTY = "ON_DUTY"
class EmployeeIn(BaseModel):
    emp_code: EmpCode
    name: Annotated[str, Field(min_length=1, max_length=100)]
    email: Annotated[str, Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[str, Field(min_length=1, max_length=50)]
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: IsoDate
    @model_validator(mode="after")
    def _shifts_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        date_cls.fromisoformat(self.joined_on)
        return self
class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None
    status: PresenceStatus = PresenceStatus.PRESENT
class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None
class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMillis] = None
    punch_out: Optional[EpochMillis] = None
    reason: Annotated[str, Field(min_length=5, max_length=200)]
    regularized_by: Annotated[str, Field(min_length=1, max_length=50)]
@asynccontextmanager
async def lifespan(app: FastAPI):
    db.employees.create_index([("emp_code", ASCENDING)], unique=True, name="uniq_emp_code")
    db.employees.create_index(
        [("department", ASCENDING), ("joined_on", ASCENDING)], name="dept_joined"
    )
    db.employees.create_index([("joined_on", ASCENDING)], name="joined_on")
    db.attendance_logs.create_index(
        [("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="uniq_emp_date"
    )
    db.attendance_logs.create_index(
        [("date", DESCENDING), ("emp_code", ASCENDING)], name="date_emp"
    )
    yield
app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)
@app.get("/health")
def health():
    try:
        db.command("ping")
    except Exception:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}
@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_to_api(doc)
@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q: dict = {}
    if department:
        q["department"] = department
    total = db.employees.count_documents(q) 
    cursor = (
        db.employees.find(q, {"_id": 0})
        .sort("emp_code", ASCENDING)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [employee_to_api(d) for d in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(404, "unknown employee")
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    d = attendance_date(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": d,
        "status": body.status.value,
        "punch_in": ts,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(ts, d, emp["shift_start"]),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_to_api(doc)
@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(404, "unknown employee")
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_out": None, "punch_in": {"$lte": ts}},
        sort=[("punch_in", DESCENDING)],
    )
    if rec is None:
        raise HTTPException(404, "no open punch-in found")
    pi = rec["punch_in"]
    if ts <= pi:
        raise HTTPException(422, "punch_out must be after punch_in")
    if (ts - pi) > timedelta(hours=24):
        raise HTTPException(422, "punch_out is more than 24 hours after punch_in")
    wh = compute_work_hours(pi, ts)
    ot = compute_overtime_minutes(ts, rec["date"], emp["shift_start"], emp["shift_end"])
    hd = compute_half_day(wh)
    updated = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None},
        {"$set": {"punch_out": ts, "work_hours": wh, "overtime_minutes": ot, "half_day": hd}},
        return_document=True,
    )
    if updated is None:
        raise HTTPException(409, "record already punched out")
    return record_to_api(updated)
def build_attendance_query(
    emp_code: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
    status: Optional[str],
    page: int,
    page_size: int,
):
    if date_from and date_to and date_from > date_to:
        raise HTTPException(422, "date_from must be <= date_to")
    q: dict = {}
    if emp_code:
        q["emp_code"] = emp_code
    if date_from or date_to:
        rng: dict = {}
        if date_from:
            rng["$gte"] = date_from
        if date_to:
            rng["$lte"] = date_to
        q["date"] = rng
    if status:
        q["status"] = status
    sort = [("date", DESCENDING), ("emp_code", ASCENDING)]
    skip = (page - 1) * page_size
    return q, sort, skip, page_size
@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[IsoDate] = None,
    date_to: Optional[IsoDate] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q, sort, skip, limit = build_attendance_query(
        emp_code, date_from, date_to, status.value if status else None, page, page_size
    )
    total = db.attendance_logs.count_documents(q)
    cursor = db.attendance_logs.find(q).sort(sort).skip(skip).limit(limit)
    items = [record_to_api(d) for d in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}
@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: IsoDate, body: RegularizeIn):
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "unknown employee")
    rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if rec is None:
        raise HTTPException(404, "no record for that date")
    new_status = body.status.value if body.status is not None else rec["status"]
    going_absent = new_status in ("ABSENT", "LEAVE")
    if going_absent:
        if body.punch_in is not None or body.punch_out is not None:
            raise HTTPException(422, "ABSENT/LEAVE cannot carry punch times")
        new_pi = None
        new_po = None
    else:
        new_pi = ms_to_dt(body.punch_in) if body.punch_in is not None else rec.get("punch_in")
        new_po = ms_to_dt(body.punch_out) if body.punch_out is not None else rec.get("punch_out")
        if new_pi is None:
            raise HTTPException(422, "a presence status requires a punch_in")
        if attendance_date(new_pi, emp["shift_start"], emp["shift_end"]) != date:
            raise HTTPException(422, "punch_in must stay on the record's attendance date")
        if new_po is not None:
            if new_po <= new_pi:
                raise HTTPException(422, "punch_out must be after punch_in")
            if (new_po - new_pi) > timedelta(hours=24):
                raise HTTPException(422, "punch_out is more than 24 hours after punch_in")
    if going_absent:
        new_late, new_wh, new_ot, new_hd = 0, None, 0, False
    else:
        new_late = compute_late_minutes(new_pi, date, emp["shift_start"])
        if new_po is not None:
            new_wh = compute_work_hours(new_pi, new_po)
            new_ot = compute_overtime_minutes(new_po, date, emp["shift_start"], emp["shift_end"])
            new_hd = compute_half_day(new_wh)
        else:
            new_wh, new_ot, new_hd = None, 0, False
    final = {
        "status": new_status,
        "punch_in": new_pi,
        "punch_out": new_po,
        "work_hours": new_wh,
        "late_minutes": new_late,
        "overtime_minutes": new_ot,
        "half_day": new_hd,
    }
    changes: dict[str, Any] = {}
    for field, new_val in final.items():
        old_val = rec.get(field)
        if field in ("late_minutes", "overtime_minutes"):
            old_val = int(old_val or 0)
        if field == "half_day":
            old_val = bool(old_val or False)
        if old_val != new_val:
            changes[field] = {"from": old_val, "to": new_val}
    if not changes:
        raise HTTPException(422, "request changes nothing")
    entry = {
        "at": now_utc(),
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": changes,
    }
    updated = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "history": {"$size": len(rec.get("history", []))}},
        {"$set": final, "$push": {"history": entry}},
        return_document=True,
    )
    if updated is None:
        raise HTTPException(409, "record was modified concurrently")
    return record_to_api(updated)
def month_bounds(month: str) -> tuple[str, str, date_cls, date_cls]:
    """'YYYY-MM' -> (first_day_str, last_day_str, first_day, last_day)."""
    y, m = int(month[:4]), int(month[5:7])
    first = date_cls(y, m, 1)
    last = date_cls(y + 1, 1, 1) - timedelta(days=1) if m == 12 else date_cls(y, m + 1, 1) - timedelta(days=1)
    return first.isoformat(), last.isoformat(), first, last
def count_working_days(start: date_cls, end: date_cls) -> int:
    """Mon-Fri inclusive. This is a pure calendar calculation, not a scan of
    documents, so computing it in Python does not violate 'no Python loop over
    the data'."""
    if end < start:
        return 0
    days = 0
    d = start
    while d <= end:
        if d.weekday() < 5:  
            days += 1
        d += timedelta(days=1)
    return days
def _round_half_up_expr(expr, places: int):
    scale = 10 ** places
    return {"$divide": [{"$floor": {"$add": [{"$multiply": [expr, scale]}, 0.5]}}, scale]}
def build_monthly_pipeline(emp_code: str, month: str):
    first_s, last_s, _, _ = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first_s, "$lte": last_s}}},
        {
            "$addFields": {
                "_dow": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}},
            }
        },
        {"$addFields": {"_weekday": {"$in": ["$_dow", [2, 3, 4, 5, 6]]}}},
        {
            "$group": {
                "_id": None,
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {"$and": [{"$in": ["$status", PRESENCE_STATUSES]}, "$_weekday"]},
                            {"$cond": [{"$eq": ["$half_day", True]}, 0.5, 1]},
                            0,
                        ]
                    }
                },
                "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "late_count": {"$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}},
                "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
    ]
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$")):
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "unknown employee")
    _, _, first, last = month_bounds(month)
    joined = date_cls.fromisoformat(emp["joined_on"])
    working_days = count_working_days(max(first, joined), last)
    agg = list(db.attendance_logs.aggregate(build_monthly_pipeline(emp_code, month)))
    row = agg[0] if agg else {}
    present_days = float(row.get("present_days", 0) or 0)
    attendance_pct = (
        round_half_up(present_days / working_days * 100, 2) if working_days > 0 else None
    )
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present_days,
        "leave_days": int(row.get("leave_days", 0) or 0),
        "late_count": int(row.get("late_count", 0) or 0),
        "total_late_minutes": int(row.get("total_late_minutes", 0) or 0),
        "total_overtime_minutes": int(row.get("total_overtime_minutes", 0) or 0),
        "attendance_pct": attendance_pct,
    }
def build_department_summary_pipeline(month: str, department: Optional[str]):
    first_s, last_s, _, _ = month_bounds(month)
    match: dict = {"joined_on": {"$lte": last_s}}
    if department:
        match["department"] = department
    in_month = {"$and": [{"$gte": ["$$l.date", first_s]}, {"$lte": ["$$l.date", last_s]}]}
    weekday_expr = {"$in": [{"$dayOfWeek": {"$dateFromString": {"dateString": "$$l.date"}}}, [2, 3, 4, 5, 6]]}
    present_expr = {"$in": ["$$l.status", PRESENCE_STATUSES]}
    weight_expr = {
        "$cond": [
            {"$and": [in_month, present_expr, weekday_expr]},
            {"$cond": [{"$eq": ["$$l.half_day", True]}, 0.5, 1]},
            0,
        ]
    }
    has_wh_expr = {"$and": [in_month, present_expr, {"$ne": ["$$l.work_hours", None]}]}
    return [
        {"$match": match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "as": "logs",
            }
        },
        {
            "$addFields": {
                "e_present_days": {"$sum": {"$map": {"input": "$logs", "as": "l", "in": weight_expr}}},
                "e_late_count": {
                    "$size": {"$filter": {"input": "$logs", "as": "l", "cond": {"$and": [in_month, {"$gt": ["$$l.late_minutes", 0]}]}}}
                },
                "e_late_minutes": {
                    "$sum": {"$map": {"input": "$logs", "as": "l", "in": {"$cond": [in_month, {"$ifNull": ["$$l.late_minutes", 0]}, 0]}}}
                },
                "e_leave_count": {
                    "$size": {"$filter": {"input": "$logs", "as": "l", "cond": {"$and": [in_month, {"$eq": ["$$l.status", "LEAVE"]}]}}}
                },
                "e_on_duty_count": {
                    "$size": {"$filter": {"input": "$logs", "as": "l", "cond": {"$and": [in_month, {"$eq": ["$$l.status", "ON_DUTY"]}]}}}
                },
                "e_wh_sum": {
                    "$sum": {"$map": {"input": {"$filter": {"input": "$logs", "as": "l", "cond": has_wh_expr}}, "as": "l", "in": "$$l.work_hours"}}
                },
                "e_wh_count": {
                    "$size": {"$filter": {"input": "$logs", "as": "l", "cond": has_wh_expr}}
                },
            }
        },
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": {"$sum": "$e_present_days"},
                "late_count": {"$sum": "$e_late_count"},
                "total_late_minutes": {"$sum": "$e_late_minutes"},
                "leave_count": {"$sum": "$e_leave_count"},
                "on_duty_count": {"$sum": "$e_on_duty_count"},
                "wh_sum": {"$sum": "$e_wh_sum"},
                "wh_count": {"$sum": "$e_wh_count"},
            }
        },
        {"$sort": {"_id": ASCENDING}},
    ]
@app.get("/analytics/departments/summary")
def department_summary(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = None,
):
    rows = list(db.employees.aggregate(build_department_summary_pipeline(month, department)))
    items = []
    for r in rows:
        wh_count = int(r.get("wh_count", 0) or 0)
        avg_wh = round_half_up(r["wh_sum"] / wh_count, 2) if wh_count > 0 else None
        items.append(
            {
                "department": r["_id"],
                "headcount": int(r["headcount"]),
                "present_days": float(r.get("present_days", 0) or 0),
                "avg_work_hours": avg_wh,
                "late_count": int(r.get("late_count", 0) or 0),
                "total_late_minutes": int(r.get("total_late_minutes", 0) or 0),
                "leave_count": int(r.get("leave_count", 0) or 0),
                "on_duty_count": int(r.get("on_duty_count", 0) or 0),
            }
        )
    return {"month": month, "items": items}
def build_leaderboard_pipeline(month: str, limit: int, department: Optional[str]):
    first_s, last_s, _, _ = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": first_s, "$lte": last_s}}},
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                "late_count": {"$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}},
            }
        },
        {"$match": {"total_late_minutes": {"$gt": 0}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$match": {"emp": {"$ne": []}}},
        {"$addFields": {"emp": {"$arrayElemAt": ["$emp", 0]}}},
    ]
    if department:
        pipeline.append({"$match": {"emp.department": department}})
    pipeline += [
        {
            "$setWindowFields": {
                "sortBy": {"total_late_minutes": DESCENDING},
                "output": {"rank": {"$rank": {}}},
            }
        },
        {"$match": {"$expr": {"$lte": ["$rank", limit]}}},
        {"$sort": {"total_late_minutes": DESCENDING, "_id": ASCENDING}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return pipeline
@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    items = list(db.attendance_logs.aggregate(build_leaderboard_pipeline(month, limit, department)))
    return {"month": month, "items": items}
def build_trend_pipeline(department: str, frm: str, to: str):
    from_d = date_cls.fromisoformat(frm)
    to_d = date_cls.fromisoformat(to)
    from_dt = datetime(from_d.year, from_d.month, from_d.day, tzinfo=UTC)
    to_dt = datetime(to_d.year, to_d.month, to_d.day, tzinfo=UTC)
    upper_dt = to_dt + timedelta(days=1)
    present_expr = {"$in": ["$status", PRESENCE_STATUSES]}
    weight_expr = {"$cond": [present_expr, {"$cond": [{"$eq": ["$half_day", True]}, 0.5, 1]}, 0]}
    return [
        {"$match": {"date": {"$gte": frm, "$lte": to}}},
        {"$lookup": {"from": "employees", "localField": "emp_code", "foreignField": "emp_code", "as": "_e"}},
        {"$match": {"_e.department": department}},
        {
            "$group": {
                "_id": {"$dateFromString": {"dateString": "$date"}},
                "present_count": {"$sum": weight_expr},
                "late_count": {"$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}},
            }
        },
        {
            "$unionWith": {
                "coll": "employees",
                "pipeline": [
                    {"$match": {"department": department}},
                    {"$limit": 1},
                    {"$project": {"_b": [from_dt, to_dt]}},
                    {"$unwind": "$_b"},
                    {"$project": {"_id": "$_b", "present_count": {"$literal": 0}, "late_count": {"$literal": 0}}},
                ],
            }
        },
        {
            "$group": {
                "_id": "$_id",
                "present_count": {"$sum": "$present_count"},
                "late_count": {"$sum": "$late_count"},
            }
        },
        {"$densify": {"field": "_id", "range": {"step": 1, "unit": "day", "bounds": [from_dt, upper_dt]}}},
        {
            "$set": {
                "present_count": {"$ifNull": ["$present_count", 0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
                "date": {"$dateToString": {"date": "$_id", "format": "%Y-%m-%d"}},
                "is_working_day": {"$in": [{"$dayOfWeek": "$_id"}, [2, 3, 4, 5, 6]]},
                "_dept": department,
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "_dept",
                "foreignField": "department",
                "let": {"ds": "$date"},
                "pipeline": [
                    {"$match": {"$expr": {"$lte": ["$joined_on", "$$ds"]}}},
                    {"$count": "c"},
                ],
                "as": "_hc",
            }
        },
        {"$set": {"headcount": {"$ifNull": [{"$arrayElemAt": ["$_hc.c", 0]}, 0]}}},
        {
            "$set": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        _round_half_up_expr({"$divide": ["$present_count", "$headcount"]}, 4),
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"_id": ASCENDING},
                "output": {"_mavg": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}},
            }
        },
        {
            "$set": {
                "moving_avg_7d": {
                    "$cond": [{"$eq": ["$_mavg", None]}, None, _round_half_up_expr("$_mavg", 4)]
                }
            }
        },
        {"$sort": {"_id": ASCENDING}},
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": 1,
                "late_count": 1,
                "attendance_rate": 1,
                "moving_avg_7d": 1,
            }
        },
    ]
@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    frm: str = Query(..., alias="from", pattern=r"^\d{4}-\d{2}-\d{2}$"),
    to: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
):
    from_d = date_cls.fromisoformat(frm)
    to_d = date_cls.fromisoformat(to)
    if to_d < from_d:
        raise HTTPException(422, "to must be >= from")
    if (to_d - from_d).days + 1 > 92:
        raise HTTPException(422, "range exceeds 92 days")
    if db.employees.count_documents({"department": department}) == 0:
        raise HTTPException(404, "unknown department")
    items = list(db.attendance_logs.aggregate(build_trend_pipeline(department, frm, to)))
    return {"department": department, "items": items}
def explain_endpoint(
    endpoint: str = Path(...),
    emp_code: Optional[str] = None,
    month: Optional[str] = None,
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    frm: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    def require(val, name):
        if val is None:
            raise HTTPException(422, f"{name} is required for {endpoint}")
        return val
    if endpoint == "attendance_list":
        q, sort, skip, limit_ = build_attendance_query(
            emp_code, date_from, date_to, status, page, page_size
        )
        explain = db.command(
            "explain",
            {"find": "attendance_logs", "filter": q, "sort": dict(sort), "skip": skip, "limit": limit_},
            verbosity="executionStats",
        )
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": explain}
    if endpoint == "employee_monthly":
        pipeline = build_monthly_pipeline(require(emp_code, "emp_code"), require(month, "month"))
        coll = "attendance_logs"
    elif endpoint == "department_summary":
        pipeline = build_department_summary_pipeline(require(month, "month"), department)
        coll = "employees"
    elif endpoint == "late_leaderboard":
        pipeline = build_leaderboard_pipeline(require(month, "month"), limit, department)
        coll = "attendance_logs"
    elif endpoint == "department_trend":
        pipeline = build_trend_pipeline(require(department, "department"), require(frm, "from"), require(to, "to"))
        coll = "attendance_logs"  # indexed date-range $match is the main access
    else:
        raise HTTPException(422, "unknown endpoint")
    explain = db.command(
        "explain",
        {"aggregate": coll, "pipeline": pipeline, "cursor": {}},
        verbosity="executionStats",
    )
    return {"endpoint": endpoint, "collection": coll, "explain": explain}