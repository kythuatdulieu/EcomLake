from dagster import AssetSelection, define_asset_job

# Define Asset Selections for each layer
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

bronze_assets = AssetSelection.groups(BRONZE)
silver_assets = AssetSelection.groups(SILVER)
gold_assets = AssetSelection.groups(GOLD)

# Define Jobs
bronze_job = define_asset_job(
    name="bronze_job",
    selection=bronze_assets,
    description="Job to ingest data into Bronze layer",
)

silver_job = define_asset_job(
    name="silver_job",
    selection=silver_assets,
    description="Job to transform data into Silver layer",
)

gold_job = define_asset_job(
    name="gold_job",
    selection=gold_assets,
    description="Job to create Gold layer dimensions and facts",
)

etl_pipeline_job = define_asset_job(
    name="etl_pipeline_job",
    selection=AssetSelection.all(),
    description="Full ETL pipeline job",
)


