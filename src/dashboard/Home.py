import streamlit as st

st.set_page_config(
    page_title="EcomLake DataLakeHouse",
    page_icon="🏠",
    layout="wide"
)

st.title("Welcome to EcomLake DataLakeHouse")

st.markdown("""
This application demonstrates a comprehensive Data Lakehouse solution using:
- **Spark** for data processing
- **MinIO** for object storage
- **MLflow** for model tracking
- **Streamlit** for the user interface
- **LangChain** and **OpenAI/Groq** for chatbot capabilities

### Navigation
Select a page from the sidebar to explore:
- **Show comments**: Filter and view product reviews.
- **Predict comment**: Sentiment analysis on reviews.
- **Chatbot**: Interact with the database using natural language.
""")