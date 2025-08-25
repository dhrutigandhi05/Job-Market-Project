from datetime import date, timedelta
from pathlib import Path
import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

def get_engine():
    cfg = st.secrets["db"]
    url = (f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
           f"{cfg['host']}:{cfg['port']}//{cfg['database']}")
    return create_engine(url, pool_pre_ping=True)