CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    title TEXT,
    company TEXT,
    location TEXT,
    salary_min NUMERIC,
    salary_max NUMERIC,
    avg_salary NUMERIC,
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS skills (
   job_id TEXT REFERENCES jobs(job_id),
   skill TEXT,
);