import streamlit as st
from utils import get_spark_session
import pyspark.sql.functions as F
from pyspark.sql.functions import sum as _sum, count, desc, col

st.set_page_config(page_title="Executive Overview", page_icon="📊", layout="wide")

st.title("📊 Executive Overview")
st.markdown("Real-time high-level business metrics powered by **Spark** and **Gold Layer** aggregations.")

spark = get_spark_session()
if not spark:
    st.error("Failed to connect to Spark Cluster")
    st.stop()

def get_kpis():
    try:
        # Aggregate on the fly using Spark
        df = spark.sql("""
            SELECT 
                SUM(payment_value) as total_revenue,
                COUNT(DISTINCT order_id) as total_orders,
                AVG(payment_value) as aov
            FROM gold.fact_table
        """)
        return df.collect()[0]
    except Exception as e:
        st.error(f"Error fetching KPIs: {e}")
        return None

def get_monthly_trend():
    try:
        query = """
            SELECT 
                concat(d.year, '-', lpad(d.month, 2, '0')) as Period,
                SUM(f.payment_value) as revenue
            FROM gold.fact_table f
            JOIN gold.dim_date d ON f.dateKey = d.dateKey
            GROUP BY d.year, d.month, Period
            ORDER BY d.year, d.month
        """
        return spark.sql(query)
    except Exception as e:
        st.error(f"Error fetching trend: {e}")
        return None

def get_top_categories():
    try:
        query = """
            SELECT 
                p.product_category_name_english as category,
                SUM(f.payment_value) as revenue
            FROM gold.fact_table f
            JOIN gold.dim_product p ON f.product_id = p.product_id
            GROUP BY p.product_category_name_english
            ORDER BY revenue DESC
            LIMIT 10
        """
        return spark.sql(query)
    except Exception as e:
        st.error(f"Error fetching categories: {e}")
        return None

# --- Layout ---
with st.spinner("Calculating Key Metrics on Spark Cluster..."):
    kpis = get_kpis()
    if kpis:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Revenue", f"R$ {kpis['total_revenue']:,.2f}")
        c2.metric("Total Orders", f"{kpis['total_orders']:,}")
        c3.metric("Avg Order Value", f"R$ {kpis['aov']:,.2f}")

st.divider()

col_charts = st.columns([2, 1])

with col_charts[0]:
    st.subheader("Monthly Revenue Trend")
    trend_spark_df = get_monthly_trend()
    if trend_spark_df:
        # Collect to driver as list of dicts
        data = [row.asDict() for row in trend_spark_df.collect()]
        if data:
            st.line_chart(data, x='Period', y='revenue', use_container_width=True)
        else:
            st.info("No trend data available.")
    else:
        st.info("No trend data available.")

with col_charts[1]:
    st.subheader("Top Categories")
    cat_spark_df = get_top_categories()
    if cat_spark_df:
        data = [row.asDict() for row in cat_spark_df.collect()]
        if data:
            st.dataframe(
                data, 
                hide_index=True, 
                column_config={"revenue": st.column_config.ProgressColumn("Revenue", format="R$ %.2f")}
            )
        else:
            st.info("No category data available.")
    else:
        st.info("No category data available.")
