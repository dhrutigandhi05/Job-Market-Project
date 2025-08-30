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
