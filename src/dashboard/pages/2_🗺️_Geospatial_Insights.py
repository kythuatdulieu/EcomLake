import streamlit as st
from utils import get_spark_session
import pydeck as pdk

st.set_page_config(page_title="Geospatial Insights", page_icon="🗺️", layout="wide")

st.title("🗺️ Geospatial Insights")
st.markdown("Analyzing logistics and customer density across Brazil.")

spark = get_spark_session()
if not spark:
    st.error("Failed to connect to Spark Cluster")
    st.stop()

@st.cache_data(ttl=3600)
def get_geo_data():
    try:
        # Join customers with gold.dimcustomer to get Lat/Lng
        # Aggregating by state/city for performance if needed, but plotting points for now
        query = """
            SELECT 
                c.customer_lat as lat,
                c.customer_lng as lon,
                c.customer_state
            FROM gold.dim_customer c
            WHERE c.customer_lat IS NOT NULL AND c.customer_lng IS NOT NULL
            LIMIT 10000 
        """
        # Return list of dicts directly
        df = spark.sql(query)
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        st.error(f"Error fetching geo data: {e}")
        return []

def get_state_metrics():
    try:
        query = """
            SELECT 
                c.customer_state,
                COUNT(DISTINCT f.order_id) as total_orders,
                AVG(f.freight_value) as avg_freight
            FROM gold.fact_table f
            JOIN gold.dim_customer c ON f.customer_id = c.customer_id
            GROUP BY c.customer_state
            ORDER BY total_orders DESC
        """
        df = spark.sql(query)
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        st.error(f"Error fetching state metrics: {e}")
        return []

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Customer Density Map")
    geo_data = get_geo_data()
    if geo_data:
        # Simple Scatterplot Layer
        st.map(geo_data, size=20, use_container_width=True)
    else:
        st.info("No geospatial data available.")

with col2:
    st.subheader("State Logistics Metrics")
    state_data = get_state_metrics()
    if state_data:
        st.dataframe(
            state_data,
            hide_index=True,
            column_config={
                "total_orders": st.column_config.NumberColumn("Orders"),
                "avg_freight": st.column_config.NumberColumn("Avg Freight", format="$ %.2f")
            },
            use_container_width=True
        )
    else:
        st.info("No state metrics available.")
