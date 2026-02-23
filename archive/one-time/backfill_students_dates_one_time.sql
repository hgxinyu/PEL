-- One-time backfill for 8 Jan-2026 student records.
-- Purpose: populate pel.students.dob and pel.students.enrollment_date from raw text columns.
-- Safe scope: only the exact (full_name, email) pairs listed below.

BEGIN;

WITH targets(full_name, email) AS (
    VALUES
        ('Arora, Ivaan', 'richa.tauras5@gmail.com'),
        ('Balamurugan, Varshith', 'balamr0105@gmail.com'),
        ('Coodnapur, Samarth', 'rashmihholla@gmail.com'),
        ('Golla, Kanishk', 'harish.golla@gmail.com'),
        ('Kapadia, Arya', 'manank@gmail.com'),
        ('Lin, Ian (Enyu)', 'rachelenruixu@outlook.com'),
        ('Singhal, Ivaan', 'aditiarya2001@gmail.com'),
        ('Singhal, Rohan', 'aditiarya2001@gmail.com')
),
updated AS (
    UPDATE pel.students s
    SET
        dob = CASE
            WHEN s.dob_raw IS NULL OR btrim(s.dob_raw) = '' THEN NULL
            WHEN s.dob_raw ~ '^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?$' THEN (s.dob_raw::timestamp)::date
            WHEN s.dob_raw ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN to_date(s.dob_raw, 'MM/DD/YY')
            WHEN s.dob_raw ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(s.dob_raw, 'MM/DD/YYYY')
            ELSE s.dob
        END,
        enrollment_date = CASE
            WHEN s.enrollment_date_raw IS NULL OR btrim(s.enrollment_date_raw) = '' THEN NULL
            WHEN s.enrollment_date_raw ~ '^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?$' THEN (s.enrollment_date_raw::timestamp)::date
            WHEN s.enrollment_date_raw ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN to_date(s.enrollment_date_raw, 'MM/DD/YY')
            WHEN s.enrollment_date_raw ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(s.enrollment_date_raw, 'MM/DD/YYYY')
            ELSE s.enrollment_date
        END
    FROM targets t
    WHERE s.full_name = t.full_name
      AND s.email = t.email
    RETURNING s.full_name, s.email, s.dob, s.enrollment_date
)
SELECT COUNT(*) AS rows_updated FROM updated;

-- Optional verification
SELECT s.full_name, s.email, s.dob_raw, s.dob, s.enrollment_date_raw, s.enrollment_date
FROM pel.students s
JOIN (
    VALUES
        ('Arora, Ivaan', 'richa.tauras5@gmail.com'),
        ('Balamurugan, Varshith', 'balamr0105@gmail.com'),
        ('Coodnapur, Samarth', 'rashmihholla@gmail.com'),
        ('Golla, Kanishk', 'harish.golla@gmail.com'),
        ('Kapadia, Arya', 'manank@gmail.com'),
        ('Lin, Ian (Enyu)', 'rachelenruixu@outlook.com'),
        ('Singhal, Ivaan', 'aditiarya2001@gmail.com'),
        ('Singhal, Rohan', 'aditiarya2001@gmail.com')
) AS t(full_name, email)
ON s.full_name = t.full_name
AND s.email = t.email
ORDER BY s.full_name, s.email;

COMMIT;
