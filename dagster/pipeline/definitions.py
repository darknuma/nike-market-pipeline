from dagster_duckdb import DuckDBResource
import dagster as dg

defs = dg.Definitions(
    assets=[],
    resource={"duckdb": DuckDBResource(database="")}
)

