from dagster_duckdb import DuckDBResource
import dagster as dg


# dg.assets(
#     compute_kind="duckdb",
#     group_name="ingestion"
# )
# def products(duckdb: DuckDBResource) -> dg.MaterializeResult:
#     with duckdb.get_connection() as conn:
#         conn.execute(
#             """
#             create or replace table products as (
#                 select * from read_csv_auto('data/products.csv')
#             )
#             """
#         )

#         preview_query = "select * from products limit 10"
#         preview_df = conn.execute(preview_query).fetchdf()
#         row_count = conn.execute("select count(*) from products").fetchone()
#         count = row_count[0] if row_count else 0

#         return dg.MaterializeResult(
#             metadata={
#                 "row_count": dg.MetadataValue.int(count),
#                 "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
#             }
#         )
# defs = dg.Definitions(
#     assets=[],
#     resource={"duckdb": DuckDBResource(database="")}
# )

