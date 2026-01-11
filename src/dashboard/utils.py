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
