import streamlit as st

st.set_page_config(
    page_title="EcomLake Dashboard",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.write("# 🌊 EcomLake Dashboard")

st.markdown(
    """
    ## Welcome to the EcomLake Data Platform!

    This dashboard provides insights into the E-commerce Data Lakehouse built with **Spark**, **Delta Lake**, and **Dagster**.

    ### 👈 Select a page from the sidebar to get started

    **Available Modules:**
    - **📊 Executive Overview**: High-level KPIs and business metrics.
    - **🗺️ Geospatial Insights**: interactive maps and location-based analysis.
    
    ---
    *Built with Streamlit & PySpark*
    """
)

st.sidebar.success("Select a demo above.")
