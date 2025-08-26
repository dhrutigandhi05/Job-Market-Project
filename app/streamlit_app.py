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
    
def filters(keyword, start_d, end_d, smin, smax):
    parts = ["j.date_posted BETWEEN :start_d AND :end_d", "j.avg_salary BETWEEN :smin AND :smax"]
    params = {"start_d": start_d, "end_d": end_d, "smin": smin, "smax": smax}
    
    if keyword:
        params["kw"] = f"%{keyword}%"
        parts.append("(j.title ILIKE :kw OR j.company ILIKE :kw OR j.location ILIKE :kw)")

    return " AND ".join(parts), params