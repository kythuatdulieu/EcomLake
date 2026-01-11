from dagster import file_relative_path, Definitions
from dagstermill import define_dagstermill_asset, local_output_notebook_io_manager, ConfigurableLocalOutputNotebookIOManager
from dagster import AssetIn
from . import bronze
# Asset được định nghĩa bởi một Jupyter notebook
explore_orders_notebook = define_dagstermill_asset(
    name="explore_orders_notebook",
    notebook_path=file_relative_path(__file__, "..//notebooks//explore_orders.ipynb"),
    group_name="Notebooks",
    io_manager_key="output_notebook_io_manager",
    key_prefix=["notebook", "analysis"],
)


# Định nghĩa các tài nguyên và asset
defs = Definitions(
    assets=[explore_orders_notebook],
    resources={"output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager()}
)