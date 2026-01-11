import streamlit as st
import pandas as pd
import time
from utils import predict_sentiment, preprocess_text


def main():
    st.title("Comment sentiment classification")
    st.markdown("Select input type")
    upload_column = st.columns([2, 1])

    # selection = None
    # file upload
    product_id_input = upload_column[0].text_input("ProductID", "")

    file_upload = upload_column[0].expander(label="Upload a csv file")
    uploaded_file = file_upload.file_uploader("Choose a file", type=["csv"])

    text_select = upload_column[0].expander(label="Text")
    text_input = text_select.text_area("Comment", "")

    selection = upload_column[1].radio("Select input option", ["File", "Text"])
    button = upload_column[1].button("Predict")

    if selection == "File":
        if button:
            st.markdown(
                "___________________________________________________________________"
            )
            with st.spinner(text="Model prediction ...."):
                time.sleep(2)
                df = pd.read_csv(uploaded_file, encoding="latin1")
                predictions = []
                probabilities = []
                for text in df["Comment"]:
                    processed_text = preprocess_text(text)
                    prediction, max_pro = predict_sentiment(processed_text)
                    predictions.append(prediction[0])
                    probabilities.append(max_pro)
                df["Prediction"] = predictions
                df["Probability"] = probabilities
                st.dataframe(df)
    else:
        if button:
            st.markdown(
                "___________________________________________________________________"
            )
            with st.spinner(text="Model prediction ...."):
                time.sleep(2)
                processed_text = preprocess_text(text_input)
                prediction, max_pro = predict_sentiment(processed_text)
                st.write("Prediction:", prediction[0])
                st.write("Probability: ", max_pro)


if __name__ == "__main__":
    main()
