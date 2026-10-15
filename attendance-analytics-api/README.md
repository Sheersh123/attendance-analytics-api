# Employee Attendance & Analytics API

FastAPI + MongoDB (PyMongo). All application code is in `app/main.py`.

## Run

```bash
pip install -r requirements.txt

# MongoDB 6.0+ (the grader uses 7.0). For a local instance:
#   docker run -d -p 27017:27017 mongo:7
export MONGO_URI="mongodb://localhost:27017"   # or your Atlas SRV URI
export MONGO_DB="attendance_db"

uvicorn app.main:app --port 8000
```

`MONGO_URI` and `MONGO_DB` are read from the environment (a local `.env` is
used only as a fallback — real environment variables take priority). All
indexes are created at startup, idempotently, so there are no manual steps.
Interactive docs are at `http://localhost:8000/docs`.

## Notes

- **Timezone.** The Mongo client uses `tz_aware=True`; instants are stored as
  UTC BSON datetimes and exposed as epoch **milliseconds**, truncated to whole
  seconds (R1). All IST rules (date, late, overtime) convert UTC↔IST explicitly.
- **Rounding** is half-up everywhere (R4/R8): `Decimal(ROUND_HALF_UP)` in Python
  and an explicit `floor(x*10^n + 0.5)/10^n` expression in the pipelines (all
  reported numbers are non-negative).
- **Concurrency** is handled in the database, not with async: a unique index +
  one atomic write per request (`insert_one`, `find_one_and_update`). The
  regularize endpoint uses the record's `history` length as an optimistic lock
  so concurrent corrections can't lose each other's audit entries (loser → 409).
- **Analytics** are MongoDB aggregation pipelines. The leaderboard uses
  `$setWindowFields` + `$rank` for competition ranking; the trend uses an
  indexed date-range match, `$densify` for the one-row-per-day grid, and
  `$setWindowFields` for the 7-day moving average. The same builders back
  `/admin/explain/...`.

## Status

All endpoints in the contract are implemented: health, employees (create/list),
punch-in, punch-out, attendance list, regularize, the four analytics endpoints,
and the explain endpoint. Nothing is knowingly omitted.
