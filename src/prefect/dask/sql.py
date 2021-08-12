# import dask_sql
from prefect.core.task import Task
from typing import Callable, Iterator
from prefect.utilities.tasks import defaults_from_attrs

class DaskSqlTask(Task):
    def __init__(self, sql:str,dask_sql_kwargs:dict={},**kwargs):
        self.sql =sql
        self.dask_sql_kwargs = dask_sql_kwargs 
        super().__init__(**kwargs)

    @defaults_from_attrs("sql")
    def run(self,sql:str=None,context=None) -> None:
        import dask_sql
        from dask_sql import java
        # print where it gets the class from. That should be the DaskSQL.jar
        print(java.org.codehaus.commons.compiler.CompilerFactoryFactory.class_.getProtectionDomain().getCodeSource().getLocation())
        print(java.org.codehaus.commons.compiler.CompilerFactoryFactory.getDefaultCompilerFactory())

        # print the JVM path, that should be your java installation
        print(java.jvmpath)
        new_context = dask_sql.Context()
        if context is not None:
            new_context.schema = context.schema
            
        print(new_context.schema)

        new_context.sql(sql)
        return new_context

        # return


class DaskSqlFnRegistry(Task):
    def __init__(self,fn:Callable,function_name=None,parameters=[], 
                    return_type=None,**kwargs):
        self.fn = fn 
        self.function_name =function_name
        self.parameters =parameters
        self.return_type =return_type
        super().__init__(**kwargs)
    
    # @defaults_from_attrs("fn")
    def run(self,context=None):
        import dask_sql
        # print(context)
        if context:
            context.register_function(self.fn,
                name=self.function_name,
                parameters=self.parameters,
                return_type=self.return_type)
        else:
            context = dask_sql.Context()
            context.register_function(self.fn,
                name=self.name,
                parameters=self.parameters,
                return_type=self.return_type)
            
        # print(context.functions)

        return context


class DaskSqlLoad(Task):
    def __init__(self, table_name:str,dask_sql_kwargs:dict={},**kwargs):
        self.table_name =table_name
        self.dask_sql_kwargs = dask_sql_kwargs 
        super().__init__(**kwargs)

    @defaults_from_attrs("table_name")
    def run(self,schema_name='root',table_name=None,context=None):
        import dask_sql
        print(context.schema[schema_name].tables[table_name].df.compute())

