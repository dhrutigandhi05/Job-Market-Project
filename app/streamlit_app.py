from datetime import date, timedelta
import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

@st.cache_resource
def get_engine():
    cfg = st.secrets["db"]
    url = (f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
           f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
           f"?sslmode=require&connect_timeout=20")
    
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=2, pool_recycle=1800)

@st.cache_data(ttl=60)
def run_df(sql, params=None):
    with get_engine().begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def filters(keyword, start_d, end_d, smin, smax):
    parts = ["j.date_posted BETWEEN :start_d AND :end_d", "j.avg_salary BETWEEN :smin AND :smax"] # filter by date and salary
    params = {"start_d": start_d, "end_d": end_d, "smin": smin, "smax": smax}
    
    if keyword:
        params["kw"] = f"%{keyword}%"
        parts.append("(j.title ILIKE :kw OR j.company ILIKE :kw OR j.location ILIKE :kw)") # filter by keyword
    
    return " AND ".join(parts), params

# data loading functions
def load_jobs(keyword, start_d, end_d, smin, smax, limit=500):
    where, params = filters(keyword, start_d, end_d, smin, smax)
    params["limit"] = limit
    sql = f"""
        SELECT j.job_id, j.title, j.company, j.location, j.salary_min, j.salary_max, j.avg_salary, j.date_posted
        FROM jobs j
        WHERE {where}
        ORDER BY j.date_posted DESC, j.job_id DESC
        LIMIT :limit;
    """
    return run_df(sql, params)

# trend data
def load_trend(keyword, start_d, end_d, smin, smax):
    where, params = filters(keyword, start_d, end_d, smin, smax)
    sql = f"""
        SELECT j.date_posted::date AS d, COUNT(DISTINCT j.job_id) AS jobs
        FROM jobs j
        WHERE {where}
        GROUP BY 1
        ORDER BY 1;
    """
    return run_df(sql, params)

# top companies
def load_top_companies(keyword, start_d, end_d, smin, smax, topn=15):
    where, params = filters(keyword, start_d, end_d, smin, smax)
    params["topn"] = topn
    sql = f"""
        SELECT j.company, COUNT(DISTINCT j.job_id) AS c
        FROM jobs j
        WHERE {where}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT :topn;
    """
    return run_df(sql, params)

# skill categories 
def load_skill_categories(keyword, start_d, end_d, smin, smax):
    where_sql, params = filters(keyword, start_d, end_d, smin, smax)
    sql = f"""
    WITH base AS (
        SELECT j.job_id,
               LOWER(REGEXP_REPLACE(js.skill, '[^a-z0-9+#. /-]', ' ', 'g')) AS s
        FROM job_skills js
        JOIN jobs j ON j.job_id = js.job_id
        WHERE {where_sql}
    ),
    norm AS ( -- map skills to categories
        SELECT job_id,
               CASE
                   WHEN s ~ '(python|pandas|numpy|sklearn)' THEN 'Programming'
                   WHEN s ~ '(sql|postgres|mysql|snowflake)' THEN 'Databases'
                   WHEN s ~ '(tableau|power bi|excel)' THEN 'BI and viz'
                   WHEN s ~ '(aws|azure|gcp)' THEN 'Cloud'
                   WHEN s ~ '(spark|airflow|kubernetes|docker|hadoop)' THEN 'Data platform'
                   WHEN s ~ '(nlp|machine learning|deep learning)' THEN 'ML and AI'
                   ELSE 'Other'
               END AS cat
        FROM base
    )
    SELECT cat, COUNT(DISTINCT job_id) AS c
    FROM norm
    GROUP BY 1
    ORDER BY 2 DESC;
    """
    return run_df(sql, params)

# detailed category breakdown with skills
def load_category_breakdown(keyword, start_d, end_d, smin, smax):
    where_sql, params = filters(keyword, start_d, end_d, smin, smax)
    sql = f"""
    WITH scope AS (
        SELECT DISTINCT j.job_id
        FROM jobs j
        WHERE {where_sql}
    ),
    base AS (
        SELECT s.job_id,
               LOWER(REGEXP_REPLACE(js.skill, '[^a-z0-9+#. /-]', ' ', 'g')) AS raw -- cleaning skill text
        FROM job_skills js
        JOIN scope s ON s.job_id = js.job_id
    ),
    clean AS (
        SELECT job_id, TRIM(REGEXP_REPLACE(raw, '\\s+', ' ', 'g')) AS s -- remove extra spaces
        FROM base
    ),
    short AS (
        SELECT job_id, s
        FROM clean
        WHERE s <> ''
          AND array_length(string_to_array(s, ' '), 1) BETWEEN 1 AND 4 -- keep non-empty skills
          AND LENGTH(s) BETWEEN 2 AND 50
          -- filter out common non-skills
          AND s NOT ILIKE '%%clearance%%'
          AND s NOT ILIKE '%%drug%%'
          AND s NOT ILIKE '%%background%%'
          AND s NOT ILIKE '%%screen%%'
          AND s NOT ILIKE '%%communication%%'
          AND s NOT ILIKE '%%writing%%'
          AND s NOT ILIKE '%%years%%'
          AND s NOT ILIKE '%%degree%%'
    ),
    -- same mapping as load_skill_categories so pie and table align
    catmap AS (
        SELECT job_id, s,
               CASE
                 WHEN s ~* '(python|pandas|numpy|sklearn)' THEN 'Programming'
                 WHEN s ~* '(sql|postgres|mysql|snowflake)' THEN 'Databases'
                 WHEN s ~* '(tableau|power ?bi|excel)' THEN 'BI and viz'
                 WHEN s ~* '(aws|azure|gcp)' THEN 'Cloud'
                 WHEN s ~* '(spark|airflow|kubernetes|docker|hadoop)' THEN 'Data platform'
                 WHEN s ~* '(nlp|machine learning|deep learning)' THEN 'ML and AI'
                 ELSE 'Other'
               END AS cat
        FROM short
    ),
    counts AS (
        SELECT cat, COUNT(DISTINCT job_id) AS postings, COUNT(*) AS occurrences
        FROM catmap
        GROUP BY cat
    ),
    skill_counts AS (
        SELECT cat, s AS skill, COUNT(*) AS c
        FROM catmap
        GROUP BY cat, s
    ),
    full_list AS (
        SELECT cat,
               STRING_AGG(skill || ' (' || c || ')', ', ' ORDER BY c DESC, skill) AS skills -- list of skills with counts
        FROM skill_counts
        GROUP BY cat
    )
    SELECT c.cat, c.postings, c.occurrences, COALESCE(f.skills, '') AS included_skills
    FROM counts c
    LEFT JOIN full_list f ON f.cat = c.cat
    ORDER BY c.postings DESC, c.occurrences DESC;
    """
    return run_df(sql, params)

# easy to understand definitions for each category
def category_definitions():
    return {
        "Programming": "Python, R, Java, Scala, C++, Go, JavaScript/TypeScript, Pandas, NumPy, scikit-learn",
        "Databases":   "SQL (Postgres, MySQL, SQL Server, SQLite), Snowflake, Redshift, BigQuery, Oracle/DB2",
        "BI and viz":  "Tableau, Power BI (DAX/Power Query), Excel, Looker, Superset, Plotly/Matplotlib/Seaborn/Qlik",
        "Cloud":       "AWS, Azure, GCP (e.g., SageMaker, Vertex AI, EC2/S3/Lambda, Glue/Athena/EMR, Dataproc/Dataflow)",
        "Data platform":"Spark, Airflow, dbt, Kafka, Hadoop/Hive/Presto/Trino, Kubernetes, Docker, Databricks",
        "ML and AI":   "Machine/Deep Learning, NLP, CV, TensorFlow, PyTorch, XGBoost",
        "Other":       "Items that don't match the other categories"
    }

# salary distribution
def load_salary(keyword, start_d, end_d, smin, smax):
    if smax <= smin:
        return pd.DataFrame(columns=["bin","bin_min","bin_max","c","range"])
    where, params = filters(keyword, start_d, end_d, smin, smax)
    sql = f"""
        SELECT width_bucket(j.avg_salary, :smin, :smax, 20) AS bin,
               MIN(j.avg_salary) AS bin_min,
               MAX(j.avg_salary) AS bin_max,
               COUNT(*) AS c
        FROM jobs j
        WHERE {where}
        GROUP BY 1
        ORDER BY 1;
    """
    df = run_df(sql, params)
    if df.empty:
        return df
    df["range"] = df[["bin_min", "bin_max"]].apply(lambda r: f"{int(r.bin_min)}–{int(r.bin_max)}", axis=1)
    return df

# page layout
st.set_page_config(page_title="Job Trends Dashboard", layout="wide")
st.title("TalentScope")
st.subheader("Job Trends Dashboard")

# sidebar filters
with st.sidebar:
    st.subheader("Filters")
    default_start = date.today() - timedelta(days=30)
    kw = st.text_input("Keyword (title / company / location)", value="data")
    start_d = st.date_input("Start date", default_start)
    end_d = st.date_input("End date", date.today())
    smin, smax = st.slider("Avg salary range", 0, 400000, (0, 250000), step=5000)

# load data
jobs_df = load_jobs(kw, start_d, end_d, smin, smax, limit=1000)
trend_df = load_trend(kw, start_d, end_d, smin, smax)
companies_df = load_top_companies(kw, start_d, end_d, smin, smax)
salary_df = load_salary(kw, start_d, end_d, smin, smax)

# KPIs
col1, col2, col3 = st.columns(3)
col1.metric("Jobs (rows shown)", len(jobs_df))
col2.metric("Unique Companies", jobs_df['company'].nunique() if not jobs_df.empty else 0)
col3.metric("Unique Locations", jobs_df['location'].nunique() if not jobs_df.empty else 0)

integer = alt.Axis(format="d", tickMinStep=1)
c1, c2 = st.columns(2)

# trends - line chart
with c1:
    st.subheader("Job Trends")

    if not trend_df.empty:
        chart = (
            alt.Chart(trend_df)
            .mark_line(point=True) # line with points
            .encode(x="d:T", y="jobs:Q") # x = date, y = job count
            .properties(height=300)
        ).interactive()
        st.altair_chart(chart, use_container_width=True) # make chart
    else:
        st.info("No data available for this time window.")

# companies - bar chart
with c2:
    st.subheader("Top Companies")

    if not companies_df.empty:
        h = max(220, 18 * len(companies_df))
        chart = (
            alt.Chart(companies_df)
            .mark_bar()
            .encode(
                y=alt.Y("company:N", sort="-x", title="Company"), # company on y axis
                x=alt.X("c:Q", title="Jobs", axis=integer), # job count on x axis
                tooltip=[alt.Tooltip("company:N", title="Company"), alt.Tooltip("c:Q", title="Jobs")] # hover tooltip
            )
            .properties(height=h)
        )
        labels = chart.mark_text(align="left", dx=3).encode(text="c:Q")
        st.altair_chart(chart + labels, use_container_width=True)
    else:
        st.info("No companies found.")

c3, c4 = st.columns(2)

# skills - pie chart
with c3:
    st.subheader("Skills overview")
    cats = load_skill_categories(kw, start_d, end_d, smin, smax)

    if not cats.empty:
        pie = (
            alt.Chart(cats)
            .mark_arc()
            .encode(
                theta="c:Q",
                color=alt.Color("cat:N", title="Category"), # color by category
                tooltip=[alt.Tooltip("cat:N", title="Category"), alt.Tooltip("c:Q", title="Postings")]
            )
            .properties(height=300)
        )
        st.altair_chart(pie, use_container_width=True)
    else:
        st.info("No skills found.")

# category breakdown table
with c4:
    st.subheader("Category breakdown")
    bdf = load_category_breakdown(kw, start_d, end_d, smin, smax)

    if not bdf.empty:
        # ensure table shows all categories that appear in the pie and in the same order
        order = cats["cat"].tolist() if not cats.empty else None

        if order:
            bdf = bdf.set_index("cat").reindex(order).reset_index()

        # add definitions
        defs = pd.DataFrame([{"category": k, "category_includes": v} for k, v in category_definitions().items()])
        bdf = bdf.rename(columns={"cat": "category"})
        bdf = defs.merge(bdf, on="category", how="right")  # keep all categories in breakdown

        # show table
        st.dataframe(
            bdf[["category", "postings", "occurrences", "category_includes", "included_skills"]],
            use_container_width=True,
            hide_index=True
        )

        # explanation
        st.caption("‘postings’ = jobs mentioning at least one skill in the category; "
                   "‘occurrences’ = total mentions of all skills in that category; "
                   "‘category_includes’ = what counts for that category; "
                   "‘included_skills’ = the actual skills found (with counts).")
    else:
        st.info("No category details found.")

# salary distribution - bar chart
st.subheader("Salary distribution")
if not salary_df.empty:
    chart = (
        alt.Chart(salary_df)
        .mark_bar()
        .encode(x=alt.X("range:N", sort=None, title="Avg salary distribution"), # bins on x axis
                y=alt.Y("c:Q", title="Count")) # counts on y axis
        .properties(height=320)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("No salary data found.")

# Results
st.subheader("Matching Job Listings")
if not jobs_df.empty:
    show_cols = ["title", "company", "location", "salary_min", "salary_max", "avg_salary", "date_posted"]
    st.dataframe(jobs_df[show_cols], use_container_width=True, hide_index=True)
else:
    st.info("No job listings found.")