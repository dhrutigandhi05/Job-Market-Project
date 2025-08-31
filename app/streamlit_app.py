from datetime import date, timedelta
from pathlib import Path
import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# create engine 
def get_engine():
    cfg = st.secrets["db"]
    url = (f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
           f"{cfg['host']}:{cfg['port']}//{cfg['database']}")
    return create_engine(url, pool_pre_ping=True)

# run a query and return a dataframe result
@st.cache_data(ttl=60)
def run_df(sql, params=None):
    with get_engine().begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

# build sql queries for search
def filters(keyword, start_d, end_d, smin, smax):
    parts = ["j.date_posted BETWEEN :start_d AND :end_d", "j.avg_salary BETWEEN :smin AND :smax"]
    params = {"start_d": start_d, "end_d": end_d, "smin": smin, "smax": smax}
    
    if keyword:
        params["kw"] = f"%{keyword}%"
        parts.append("(j.title ILIKE :kw OR j.company ILIKE :kw OR j.location ILIKE :kw)")

    return " AND ".join(parts), params

# load jobs from the database
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

# load job trends from the database
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

# load top companies from the database
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

# load top skills from the database
def load_top_skills(keyword, start_d, end_d, smin, smax, topn=20):
    where, params = filters(keyword, start_d, end_d, smin, smax)
    params["topn"] = topn
    sql = f"""
        SELECT LOWER(js.skill) AS skill, COUNT(*) AS c
        FROM job_skills js
        JOIN jobs j ON j.job_id = js.job_id
        WHERE {where}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT :topn;
    """
    return run_df(sql, params)

# load salary distribution from the database
def load_salary(keyword, start_d, end_d, smin, smax):
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
    
    # make a salary range
    df["range"] = df[["bin_min", "bin_max"]].apply(lambda r: f"{int(r.bin_min)}–{int(r.bin_max)}", axis=1)
    return df

# main app (UI)
st.set_page_config(page_title="Job Trends Dashboard", layout="wide")
st.title("Job Trends Dashboard")

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
skills_df = load_top_skills(kw, start_d, end_d, smin, smax)
salary_df = load_salary(kw, start_d, end_d, smin, smax)

# kpis
col1, col2, col3 = st.columns(3)
col1.metric("Jobs (rows shown)", len(jobs_df))
col2.metric("Unique Companies", jobs_df['company'].nunique() if not jobs_df.empty else 0)
col3.metric("Unique Locations", jobs_df['location'].nunique() if not jobs_df.empty else 0)

# charts
c1, c2 = st.columns(2)

# job trends 
with c1:
    st.subheader("Job Trends")

    if not trend_df.empty:
        chart = (
            alt.Chart(trend_df) # trends chart
            .mark_line(point=True) # line chart
            .encode(x="d:T", y="jobs:Q")
            .properties(height=300)
        )

        st.altair_chart(chart, use_container_width=True) # display chart
    else:
        st.info("No data available for this time window.")

# top companies
with c2:
    st.subheader("Top Companies")

    if not companies_df.empty:
        chart = (
            alt.Chart(companies_df) # companies chart
            .mark_bar() # bar chart
            .encode(y=alt.Y("company:N", sort="-x"), x="c:Q")
            .properties(height=300)
        )

        st.altair_chart(chart, use_container_width=True) # display chart
    else:
        st.info("No companies found.")
