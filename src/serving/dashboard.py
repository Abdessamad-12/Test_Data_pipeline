"""Dashboard : visualisation de la couche gold via DuckDB.

Usage local : PYTHONPATH=src streamlit run src/serving/dashboard.py
"""
import duckdb
import plotly.express as px
import streamlit as st
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from serving.sql_engine import GOLD_TABLES, get_connection

st.set_page_config(page_title="Chicago Taxi Trips", layout="wide")


@st.cache_resource
def connection() -> duckdb.DuckDBPyConnection:
    return get_connection()


@st.cache_data(ttl=300)
def query(sql: str):
    return connection().cursor().sql(sql).df()


st.title("Chicago Taxi Trips : couche gold")

try:
    connection()
except duckdb.IOException:
    st.warning("La couche gold est vide. Lance le DAG `taxi_pipeline` dans Airflow (http://localhost:8080).")
    st.stop()

total = query("SELECT SUM(trips) AS trips, SUM(total_revenue) AS revenue FROM daily_revenue").iloc[0]
c1, c2, c3 = st.columns(3)
c1.metric("Trajets", f"{int(total.trips):,}".replace(",", " "))
c2.metric("Revenu total", f"{total.revenue:,.0f} $".replace(",", " "))
c3.metric("Revenu moyen / trajet", f"{total.revenue / total.trips:.2f} $")

tab_rev, tab_dur, tab_zones, tab_sql = st.tabs(["Revenus", "Durées", "Zones et routes", "SQL libre"])

with tab_rev:
    df = query("SELECT trip_date, trips, total_revenue FROM daily_revenue ORDER BY trip_date")
    st.plotly_chart(px.bar(df, x="trip_date", y="total_revenue", title="Revenu par jour ($)"), use_container_width=True)
    st.plotly_chart(px.line(df, x="trip_date", y="trips", title="Trajets par jour"), use_container_width=True)

with tab_dur:
    df = query("SELECT trip_date, avg_duration_min, median_duration_min FROM daily_trip_duration ORDER BY trip_date")
    st.plotly_chart(
        px.line(df, x="trip_date", y=["avg_duration_min", "median_duration_min"], title="Durée des trajets (minutes)"),
        use_container_width=True,
    )

with tab_zones:
    left, right = st.columns(2)
    zones = query("SELECT zone_name, trips FROM top_pickup_zones ORDER BY rank LIMIT 15")
    left.plotly_chart(
        px.bar(zones, x="trips", y="zone_name", orientation="h", title="Top 15 zones de prise en charge")
        .update_layout(yaxis={"autorange": "reversed"}),
        use_container_width=True,
    )
    right.subheader("Top 20 itinéraires")
    right.dataframe(query("SELECT pickup_zone, dropoff_zone, trips, total_revenue FROM top_routes ORDER BY trips DESC"),
                    hide_index=True, use_container_width=True)

with tab_sql:
    st.caption(f"Tables disponibles : {', '.join(GOLD_TABLES)}")
    sql = st.text_area("Requête SQL (SELECT uniquement)", "SELECT * FROM daily_revenue ORDER BY total_revenue DESC LIMIT 10")
    if st.button("Exécuter"):
        if not sql.strip().lower().startswith(("select", "with")):
            st.error("Seules les requêtes SELECT sont autorisées.")
        else:
            try:
                st.dataframe(query(sql), use_container_width=True)
            except duckdb.Error as e:
                st.error(str(e))