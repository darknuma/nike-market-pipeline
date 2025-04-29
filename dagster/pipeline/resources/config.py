from dagster import ConfigurableResource, resource
from typing import Optional

class MinioConfig(ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool = False
    bucket_name: str

@resource
def minio_client(minio_config: MinioConfig):
    """
    Resource that provides a MinIO client for object storage operations.
    """
    from minio import Minio
    
    return Minio(
        minio_config.endpoint,
        access_key=minio_config.access_key,
        secret_key=minio_config.secret_key,
        secure=minio_config.secure
    )

class DatabaseConfig(ConfigurableResource):
    host: str
    port: int
    database: str
    user: str
    password: str

@resource
def database_connection(db_config: DatabaseConfig):
    """
    Resource that provides a database connection.
    """
    # TODO: Implement actual database connection logic
    return None 