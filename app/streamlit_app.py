from datetime import date, timedelta
from pathlib import Path
import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# create engine
@st.cache_resource
def get_engine():
    cfg = st.secrets["db"]
    url = (f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
           f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
           f"?sslmode=require&connect_timeout=5")
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=2, pool_recycle=1800)

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

total_jobs = len(jobs_df)
integer = alt.Axis(format="d", tickMinStep=1)

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
        ).interactive()

        st.altair_chart(chart, use_container_width=True) # display chart
    else:
        st.info("No data available for this time window.")

# top companies
with c2:
    st.subheader("Top Companies")

    if not companies_df.empty:
        # dynamic height so labels do not collide
        h = max(220, 18 * len(companies_df))
        chart = (
            alt.Chart(companies_df)
            .mark_bar()
            .encode(
                y=alt.Y("company:N", sort="-x", title="Company"),
                x=alt.X("c:Q", title="Jobs", axis=integer),
                tooltip=[alt.Tooltip("company:N", title="Company"),
                         alt.Tooltip("c:Q", title="Jobs")]
            )
            .properties(height=h)
        )

        labels = chart.mark_text(align="left", dx=3).encode(text="c:Q")

        st.altair_chart(chart + labels, use_container_width=True)
    else:
        st.info("No companies found.")

c3, c4 = st.columns(2)

# top skills
# with c3:
#     st.subheader("Top skills")

#     if not skills_df.empty:
#         h = max(240, 18 * len(skills_df))
#         chart = (
#             alt.Chart(skills_df)
#             .mark_bar()
#             .encode(
#                 y=alt.Y("skill:N", sort="-x", title="Skill"),
#                 x=alt.X("c:Q", title="Mentions", axis=integer),
#                 tooltip=[alt.Tooltip("skill:N", title="Skill"),
#                          alt.Tooltip("c:Q", title="Mentions")]
#             )
#             .properties(height=h)
#         )

#         labels = chart.mark_text(align="left", dx=3).encode(text="c:Q")

#         st.altair_chart(chart + labels, use_container_width=True)
#     else:
#         st.info("No skills found.")
with c3:
    st.subheader("Skills overview")

    if not skills_df.empty and not jobs_df.empty:
        # compute coverage and cumulative coverage
        total_posts = jobs_df["job_id"].nunique()
        sdf = skills_df.copy()
        sdf["share"] = sdf["c"] / total_posts
        sdf = sdf.sort_values("c", ascending=False)
        sdf["cum_share"] = sdf["share"].cumsum()

        tab_table, tab_heatmap, tab_trend = st.tabs(["Table", "Co-occurrence", "Skill trend"])

        with tab_table:
            # small, readable table instead of bars
            show = sdf.rename(columns={"c": "mentions", "share": "coverage", "cum_share": "cum_coverage"})
            show["coverage"] = (show["coverage"] * 100).round(1)
            show["cum_coverage"] = (show["cum_coverage"] * 100).round(1)
            st.caption(f"Total postings in view: {total_posts:,}")
            st.dataframe(
                show[["skill", "mentions", "coverage", "cum_coverage"]],
                use_container_width=True,
                hide_index=True
            )
            st.caption("Coverage is the percent of postings that include the skill. Cumulative shows how quickly a few skills cover most postings.")

        with tab_heatmap:
            # simple skill co-occurrence heatmap for the top 12 skills
            topn = min(12, len(sdf))
            top_skills = sdf.head(topn)["skill"].tolist()
            # fetch pairs from your existing tables
            sql = """
                SELECT LOWER(a.skill) AS s1, LOWER(b.skill) AS s2, COUNT(DISTINCT a.job_id) AS c
                FROM job_skills a
                JOIN job_skills b ON a.job_id = b.job_id AND LOWER(a.skill) < LOWER(b.skill)
                JOIN jobs j ON j.job_id = a.job_id
                WHERE {where}
                GROUP BY 1,2
            """.format(where=filters(kw, start_d, end_d, smin, smax)[0])
            pairs = run_df(sql, filters(kw, start_d, end_d, smin, smax)[1])
            if not pairs.empty:
                pairs = pairs[pairs["s1"].isin(top_skills) & pairs["s2"].isin(top_skills)]
                if not pairs.empty:
                    # build a square matrix-like dataset
                    left = pairs.rename(columns={"s1": "skill_a", "s2": "skill_b"})
                    right = pairs.rename(columns={"s1": "skill_b", "s2": "skill_a"})
                    grid = pd.concat([left, right], ignore_index=True)

                    heat = (
                        alt.Chart(grid)
                        .mark_rect()
                        .encode(
                            x=alt.X("skill_a:N", title="Skill A", sort=top_skills),
                            y=alt.Y("skill_b:N", title="Skill B", sort=top_skills),
                            color=alt.Color("c:Q", title="Co-mentions"),
                            tooltip=["skill_a:N", "skill_b:N", alt.Tooltip("c:Q", title="Co-mentions")]
                        )
                        .properties(height=320)
                    )
                    st.altair_chart(heat, use_container_width=True)
                else:
                    st.info("Not enough overlap among top skills to draw a heatmap.")
            else:
                st.info("No co-occurrence data available.")

        with tab_trend:
            # trend over time for a chosen skill
            choose_skill = st.selectbox("Pick a skill", sdf["skill"].tolist(), index=0)
            if choose_skill:
                sql = """
                    SELECT j.date_posted::date AS d, COUNT(DISTINCT j.job_id) AS c
                    FROM job_skills js
                    JOIN jobs j ON j.job_id = js.job_id
                    WHERE LOWER(js.skill) = :skill
                      AND {where}
                    GROUP BY 1
                    ORDER BY 1
                """.format(where=filters(kw, start_d, end_d, smin, smax)[0])
                params = filters(kw, start_d, end_d, smin, smax)[1]
                params["skill"] = choose_skill
                s_trend = run_df(sql, params)

                if not s_trend.empty:
                    s_trend["ma7"] = s_trend["c"].rolling(7, min_periods=1).mean()
                    bars = alt.Chart(s_trend).mark_bar().encode(x="d:T", y=alt.Y("c:Q", title="Mentions per day"))
                    line = alt.Chart(s_trend).mark_line(strokeWidth=2).encode(x="d:T", y="ma7:Q")
                    st.altair_chart((bars + line).properties(height=300), use_container_width=True)
                else:
                    st.info("No mentions for that skill in the selected window.")
    else:
        st.info("No skills found.")

# avg salary distribution
with c4:
    st.subheader("Salary distribution")

    if not salary_df.empty:
        chart = (
            alt.Chart(salary_df)
            .mark_bar() # bar chart
            .encode(x=alt.X("range:N", sort=None, title="Avg salary distribution"),
                    y=alt.Y("c:Q", title="Count"))
            .properties(height=320)
        )

        st.altair_chart(chart, use_container_width=True) # display chart
    else:
        st.info("No salary data found.")


# results table
st.subheader("Matching Job Listings")

if not jobs_df.empty:
    show_cols = ["title", "company", "location", "salary_min", "salary_max", "avg_salary", "date_posted"]
    st.dataframe(jobs_df[show_cols], use_container_width=True, hide_index=True)
else:
    st.info("No job listings found.")