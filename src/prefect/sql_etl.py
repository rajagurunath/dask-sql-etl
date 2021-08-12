from prefect import task, Flow
import datetime
import random
from time import sleep
from prefect.executors import LocalDaskExecutor
from prefect.executors.dask import DaskExecutor
from prefect.tasks.dask.sql import DaskSqlTask,DaskSqlFnRegistry,DaskSqlLoad
import numpy as np

print("started")
create_dataset = DaskSqlTask(
    """CREATE OR REPLACE TABLE iris
        WITH (
            location = 'iris.csv',
            format = 'csv',
            persist = True
        )""",
        name = "create_dataset"
    )
feature_engineering = DaskSqlTask(
    """
    CREATE OR REPLACE TABLE enriched_iris AS (
    SELECT 
        sepal_length, sepal_width, petal_length, petal_width,
        CASE 
            WHEN species = 'setosa' THEN 0 ELSE CASE 
            WHEN species = 'versicolor' THEN 1
            ELSE 2 
        END END AS "species",
        iris_volume(sepal_length, sepal_width) AS volume
    FROM iris 
    )
    
    """,
    name = "feature_engineering"
)

prepare_training_data = DaskSqlTask(
    """
    CREATE OR REPLACE TABLE training_data AS (
    SELECT 
        *
    FROM enriched_iris
    TABLESAMPLE BERNOULLI (50)
    )
    """,
    name="prepare_training_data"
)

train_ml_model = DaskSqlTask(
    """
    CREATE OR REPLACE MODEL my_model WITH (
    model_class = 'sklearn.ensemble.GradientBoostingClassifier',
    wrap_predict = True,
    target_column = 'species'
    ) AS (
    SELECT * FROM training_data
    )
    
    """,
    name = "train_ml_model"
)

prediction = DaskSqlTask(
    """
    CREATE OR REPLACE TABLE results AS
    SELECT
        *
    FROM PREDICT(
        MODEL my_model,
        TABLE enriched_iris
    )
    """,
    name = "prediction"
)
def volume(length, width):
    return (width / 2) ** 2 * np.pi * length

fnregistry = DaskSqlFnRegistry(volume,function_name="iris_volume", 
                    parameters=[("length", np.float64), ("width", np.float64)], 
                    return_type=np.float64)

loadTable = DaskSqlLoad(table_name='results')

with Flow("dask-sql-etl") as flow:
    cd = create_dataset()
    fr = fnregistry(context=cd)
    fe = feature_engineering(context=cd)
    ptd = prepare_training_data(context =fe)
    tm = train_ml_model(context =ptd)
    pr= prediction(context = tm)
    loadTable(context=pr)

executor = DaskExecutor(address="localhost:8786")
flow.register("dask-sql")
flow.run(executor=executor)