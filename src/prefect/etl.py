from prefect import task, Flow
import datetime
import random
from time import sleep
# from dask_sql_operator import DaskSqlExecutor
from prefect.executors import DaskExecutor


@task
def inc(x):
    sleep(random.random() / 10)
    return x + 1


@task
def dec(x):
    sleep(random.random() / 10)
    return x - 1


@task
def add(x, y):
    sleep(random.random() / 10)
    # raise Exception ("Test execption")
    return x + y


@task(name="sum")
def list_sum(arr):
    return sum(arr)


with Flow("dask-example") as flow:
    incs = inc.map(x=range(100))
    # raise Exception("flow error")
    decs = dec.map(x=range(100))
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)
 

executor = DaskExecutor(address="localhost:8786")
flow.register("firsttry")
flow.run(executor=executor)
