from dagster import asset, AssetIn, Output, AssetExecutionContext
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import re

COMPUTE_KIND = "Mlflow"
LAYER = "ml"

def clean_text(text):
    if text is None:
        return ""
    text = text.lower()
    text = re.sub(r"http\S+", "", text)
    html = re.compile(r"<.*?>")
    text = html.sub(r"", text)
    punctuations = "@#!?+&*[]-%.:/();$=><|{}^" + "'`" + "_"
    for p in punctuations:
        text = text.replace(p, "")
    
    # Simple emoji removal (simplified from original for demo)
    text = text.encode('ascii', 'ignore').decode('ascii')
    return text.strip()

@asset(
    description="extract data from platium for machingleaning",
    ins={
        "silver_cleaned_order_review": AssetIn(key_prefix=["silver", "orderreview"]),
    },
    key_prefix=["ml"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def extract(
    context: AssetExecutionContext,
    silver_cleaned_order_review,
):
    import mlflow
    import mlflow.spark
    # silver_cleaned_order_review should be a Spark DataFrame
    df = silver_cleaned_order_review

    # Select relevant columns 
    df_comments = (
        df.select(
            F.col("review_score").alias("score"),
            F.col("review_comment_message").alias("comment"),
        )
        .dropna()
        .limit(100)   # <-- for demo pipeline
    )
 
    # Define UDF for text cleaning
    clean_text_udf = F.udf(clean_text, StringType())
    df_cleaned = df_comments.withColumn("cleaned_comment", clean_text_udf(F.col("comment")))

    # Map scores to labels
    df_labeled = df_cleaned.withColumn(
        "label_str",
        F.when(F.col("score") <= 3, "negative")
         .when(F.col("score") >= 4, "positive")
         .otherwise(None)
    ).filter(F.col("label_str").isNotNull())

    # Split data
    (train_data, test_data) = df_labeled.randomSplit([0.8, 0.2], seed=42)
    
    # Get data counts for logging
    train_count = train_data.count()
    test_count = test_data.count()

    # ML Pipeline Stages
    tokenizer = Tokenizer(inputCol="cleaned_comment", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=1000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    label_indexer = StringIndexer(inputCol="label_str", outputCol="label")
    lr = LogisticRegression(maxIter=10, regParam=0.001)

    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, label_indexer, lr])

    # MLflow tracking
    mlflow.set_tracking_uri("http://mlflow_server:5000")
    experiment_name = "sentiment_analysis"
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run():
        # Log parameters manually (autolog disabled due to mlflow-spark JAR requirement)
        mlflow.log_param("max_iter", 10)
        mlflow.log_param("reg_param", 0.001)
        mlflow.log_param("num_features", 1000)
        mlflow.log_param("train_test_split", "0.8/0.2")
        mlflow.log_param("random_seed", 42)
        
        # Log dataset info
        mlflow.log_metric("train_count", train_count)
        mlflow.log_metric("test_count", test_count)
        
        # Train model
        context.log.info(f"Training model with {train_count} samples")
        model = pipeline.fit(train_data)
        
        # Make predictions
        predictions = model.transform(test_data)
        
        # Evaluate
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        
        context.log.info(f"Test Accuracy: {accuracy}")
        mlflow.log_metric("test_accuracy", accuracy)
        
        # Log model
        mlflow.spark.log_model(model, "sentiment_model")

    return Output(
        value="Success",
        metadata={
            "accuracy": accuracy,
            "experiment_name": experiment_name,
            "train_count": train_count,
            "test_count": test_count
        }
    )
