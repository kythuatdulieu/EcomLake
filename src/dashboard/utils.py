import re
import nltk
from nltk.stem import WordNetLemmatizer
import streamlit as st
import mlflow.sklearn
from typing import Tuple, Any

# Ensure wordnet is downloaded
try:
    nltk.data.find("corpora/wordnet.zip")
except LookupError:
    nltk.download("wordnet")

# Constants
MLFLOW_SERVER_URI = "http://mlflow_server:5000"
MODEL_NAME = "classification"
MODEL_VERSION = 1
VECTOR_NAME = "transform"
VECTOR_VERSION = 1

import os
import mysql.connector
from mysql.connector import MySQLConnection

def get_db_config():
    return {
        "host": os.getenv("MYSQL_HOST", "mysql"),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", "admin"),
        "database": os.getenv("MYSQL_DATABASE", "olist"),
        "port": int(os.getenv("MYSQL_PORT", "3306"))
    }

def get_db_connection() -> MySQLConnection:
    """Creates and returns a MySQL database connection."""
    config = get_db_config()
    return mysql.connector.connect(**config)

def get_db_uri() -> str:
    """Returns the SQLAlchemy database URI."""
    config = get_db_config()
    return f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@st.cache_resource
def get_spark_session():
    """Creates and returns a SparkSession with Delta, Hive, and MinIO support."""
    try:
        builder = (
            SparkSession.builder.appName("StreamlitDashboard")
            .master("spark://spark-master:7077")
            .config("spark.jars", 
                    "/opt/spark/jars/delta-core_2.12-2.3.0.jar,"
                    "/opt/spark/jars/hadoop-aws-3.3.2.jar,"
                    "/opt/spark/jars/delta-storage-2.3.0.jar,"
                    "/opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar,"
                    "/opt/spark/jars/s3-2.18.41.jar,"
                    "/opt/spark/jars/mysql-connector-java-8.0.19.jar")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minio")
            .config("spark.hadoop.fs.s3a.secret.key", "minio123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("hive.metastore.uris", "thrift://hive-metastore:9083")
            .config("spark.sql.catalogImplementation", "hive")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        st.error(f"Failed to create Spark Session: {e}")
        return None

# Initialize lemmatizer
lemmatizer = WordNetLemmatizer()

@st.cache_resource
def load_models() -> Tuple[Any, Any]:
    """
    Load the classification model and vectorizer from MLflow.
    
    Returns:
        Tuple[Any, Any]: The loaded model and vectorizer.
    """
    try:
        mlflow.set_tracking_uri(MLFLOW_SERVER_URI)
        model_uri = f"models:/{MODEL_NAME}/{MODEL_VERSION}"
        vector_uri = f"models:/{VECTOR_NAME}/{VECTOR_VERSION}"
        
        loaded_model = mlflow.sklearn.load_model(model_uri=model_uri)
        loaded_vector = mlflow.sklearn.load_model(model_uri=vector_uri)
        return loaded_model, loaded_vector
    except Exception as e:
        st.error(f"Error loading models: {e}")
        return None, None

def preprocess_text(text: str) -> str:
    """
    Preprocess the input text by cleaning, tokenizing, and lemmatizing.

    Args:
        text (str): The input text to process.

    Returns:
        str: The processed text.
    """
    if not isinstance(text, str):
        return ""
        
    text = text.lower()
    
    # Remove URLs
    text = re.sub(r"http\S+", "", text)
    
    # Remove HTML tags
    clean_html = re.compile(r"<.*?>")
    text = clean_html.sub(r"", text)
    
    # Remove punctuation
    punctuations = "@#!?+&*[]-%.:/();$=><|{}^" + "'`" + "_"
    for p in punctuations:
        text = text.replace(p, "")
        
    # Remove emojis
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE,
    )
    text = emoji_pattern.sub(r"", text)
    
    # Tokenize and lemmatize
    words = text.split()
    lemmatized_words = [lemmatizer.lemmatize(word) for word in words]
    
    return " ".join(lemmatized_words)

def predict_sentiment(text: str) -> Tuple[Any, float]:
    """
    Predict the sentiment of the given text.

    Args:
        text (str): The input text to predict.

    Returns:
        Tuple[Any, float]: The prediction result and success probability.
    """
    model, vector = load_models()
    
    if model is None or vector is None:
        return None, 0.0

    text_vectorized = vector.transform([text])
    prediction = model.predict(text_vectorized)
    proba = model.predict_proba(text_vectorized)
    max_proba = max(proba[0])
    
    return prediction, max_proba
