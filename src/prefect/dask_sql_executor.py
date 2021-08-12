###### No need to implement executor, implementing Tasks will be sufficient ######
from prefect.executors import DaskExecutor
from prefect.executors.dask import _maybe_run
from dask_sql import Context
import sys
from typing import Iterator
from typing import Any, Callable, Iterator, TYPE_CHECKING, Union, Optional, Dict
from contextlib import contextmanager


class DaskSqlExecutor(DaskExecutor):
    @contextmanager
    def start(self) -> Iterator[None]:
        """
        Context manager for initializing execution.

        Creates a `dask.distributed.Client` and yields it.
        """
        if sys.platform != "win32":
            # Fix for https://github.com/dask/distributed/issues/4168
            import multiprocessing.popen_spawn_posix  # noqa
        from distributed import Client

        try:
            if self.address is not None:
                self.logger.info(
                    "Connecting to an existing Dask cluster at %s", self.address
                )
                with Client(self.address, **self.client_kwargs) as client:
                    self.client = client
                    self.sql_client = Context()

                    try:
                        self._pre_start_yield()
                        yield
                    finally:
                        self._post_start_yield()
            else:
                assert callable(self.cluster_class)  # mypy
                assert isinstance(self.cluster_kwargs, dict)  # mypy
                self.logger.info(
                    "Creating a new Dask cluster with `%s.%s`...",
                    self.cluster_class.__module__,
                    self.cluster_class.__qualname__,
                )
                with self.cluster_class(**self.cluster_kwargs) as cluster:
                    if getattr(cluster, "dashboard_link", None):
                        self.logger.info(
                            "The Dask dashboard is available at %s",
                            cluster.dashboard_link,
                        )
                    if self.adapt_kwargs:
                        cluster.adapt(**self.adapt_kwargs)
                    with Client(cluster, **self.client_kwargs) as client:
                        self.client = client
                        self.sql_client = Context()
                        try:
                            self._pre_start_yield()
                            yield
                        finally:
                            self._post_start_yield()

        finally:
            self.client = None
            self.sql_client = None

        return super().start()
        
    def submit(
        self, fn:callable, *args: Any, extra_context: dict = None, **kwargs: Any
    ) -> "Future":
        """
        Submit a function to the executor for execution. Returns a Future object.

        Args:
            - fn (Callable): function that is being submitted for execution
            - *args (Any): arguments to be passed to `fn`
            - extra_context (dict, optional): an optional dictionary with extra information
                about the submitted task
            - **kwargs (Any): keyword arguments to be passed to `fn`

        Returns:
            - Future: a Future-like object that represents the computation of `fn(*args, **kwargs)`
        """
        
        sql = fn(**kwargs)
        # print(f"Executing",kwargs)
        print(sql)
        dataframe = kwargs.pop("dataframe",None)

        if self.client is None:
            raise ValueError("This executor has not been started.")
        if self.sql_client is None:
            raise ValueError("This dask-sql has not been started.")

        kwargs.update(self._prep_dask_kwargs(extra_context))
        if self._should_run_event is None:
            # fut = self.client.submit(fn, *args, **kwargs)
            fut = self.sql_client.sql(sql,return_futures=True,dataframes=dataframe).to_delayed()[0]
            print(fut)
        else:
            # fut = self.client.submit(
            #     _maybe_run, self._should_run_event.name, fn, *args, **kwargs
            # )
            fut = self.sql_client.sql(sql,return_futures=True,dataframes=dataframe).to_delayed()[0]
            # self._futures.add(fut)
        return fut

    def wait(self, futures: Any) -> Any:
        """
        Resolves the Future objects to their values. Blocks until the computation is complete.

        Args:
            - futures (Any): single or iterable of future-like objects to compute

        Returns:
            - Any: an iterable of resolved futures with similar shape to the input
        """
        print(futures.values())
        if self.client is None:
            raise ValueError("This executor has not been started.")
        return self.client.gather(futures)
        # return [v[0][0].compute() for k,v in futures.items()]
