# DECISIONS.md
1. I added a unique index on emp_code in the employees collection so no two employees can share a code and a unique index on emp_code plus date in attendance_logs so a person can only have one record per day Those unique indexes also take care of the race conditions For reads I added a date and emp_code index on attendance_logs since that matches how the listing and the analytics sort and filter and a department and joined_on index on employees for the headcount queries I considered indexing status but dropped it since it only has five values and would barely help

2. I never check whether a record exists and then insert it I just insert and let the unique index decide If two identical punch-ins arrive together the database lets only one insert succeed and returns 201 and the second one fails with a duplicate key error that I turn into a 409

3. I use the rank window function so tied people share a rank and the next rank is skipped I apply the limit after ranking so if two people are tied right at the cutoff they both show up even if that returns more rows than the limit Rows are sorted by total late minutes then by emp_code

4. I build the summary from the employees collection instead of the logs then look up each person attendance That way an employee with no logs still appears and counts as one in the headcount with zeros everywhere else I also only include people who joined on or before the end of the month.

5. One thing for 100x data The per-day work in the trend and joining every employee to their logs in the summary would get slow I would precompute daily totals per department into a separate collection update it as new records arrive and read the summary and trend from that instead of recalculating each time