ALTER TABLE pel.students
ADD COLUMN IF NOT EXISTS tel text;

ALTER TABLE pel.students
ADD COLUMN IF NOT EXISTS source text;

COPY pel.students (first_name, last_name, dob_raw, address, tel, source, email, enrollment_date_raw)
FROM 'b:/iCloud/iCloudDrive/Documents/XY Documents/Study/pel/PEL/students.csv'
WITH (FORMAT csv, HEADER true);

COPY pel.progress (first_name, last_name, email, subject, pel_wks_level, pel_wks_no, progress_date)
FROM 'b:/iCloud/iCloudDrive/Documents/XY Documents/Study/pel/PEL/progress.csv'
WITH (FORMAT csv, HEADER true);
