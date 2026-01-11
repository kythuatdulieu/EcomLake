from dagster import load_assets_from_modules,load_assets_from_package_module, Definitions
from dagstermill import local_output_notebook_io_manager, ConfigurableLocalOutputNotebookIOManager
from . import bronze,silver,gold,platinum,ml,notebook

bronze_layer_assets = load_assets_from_modules([bronze])
silver_layer_assets = load_assets_from_modules([silver])
gold_layer_assets = load_assets_from_modules([gold])
platinum_layer_assets = load_assets_from_modules([platinum])
ml_layer_assets = load_assets_from_modules([ml])
notebook_assets = load_assets_from_modules([notebook])

# defs = Definitions(
#     assets=load_assets_from_package_module(eda),
#     resources={"output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager()},
# )