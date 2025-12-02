"""
Configuration management for IRIS AMQP connector.
"""

from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING
import os
import logging

if TYPE_CHECKING:
    from pyspark.dbutils import DBUtils

logger = logging.getLogger(__name__)


def get_dbutils() -> "DBUtils":
    """
    Get the Databricks dbutils object.
    
    Works in both notebook and job contexts.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active Spark session")
        
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        # Alternative method for some Databricks environments
        try:
            import IPython
            return IPython.get_ipython().user_ns["dbutils"]
        except (ImportError, KeyError, AttributeError):
            raise RuntimeError(
                "Unable to get dbutils. This function must be run in a Databricks environment."
            )


@dataclass
class IRISConfig:
    """
    Configuration for connecting to Elexon's IRIS AMQP service.
    
    IRIS (Insights Real-time Information Service) provides near real-time
    data via AMQP 1.0 protocol on Azure Service Bus.
    
    Attributes:
        host: IRIS AMQP server hostname (Azure Service Bus)
        port: AMQP port (default 5671 for AMQPS)
        queue: The IRIS queue/entity path to subscribe to
        client_id: OAuth Client ID for IRIS authentication
        client_secret: OAuth Client Secret for IRIS authentication
        subscription_name: Name for the durable subscription
        use_tls: Whether to use TLS/SSL encryption
        connection_timeout: Connection timeout in seconds
        prefetch_count: Number of messages to prefetch
        max_batch_size: Maximum messages per micro-batch
        checkpoint_location: Spark checkpoint directory
    """
    
    # Connection settings - Azure Service Bus endpoint
    host: str = "elexon-insights-iris.servicebus.windows.net"
    port: int = 5671
    queue: str = "iris.5c9f752d-795a-4986-97cc-8a49f5380c02"
    client_id: str = ""
    client_secret: str = ""
    tenant_id: str = ""
    
    # Subscription settings
    subscription_name: str = "pyspark-iris-connector"
    
    # Security settings
    use_tls: bool = True
    verify_ssl: bool = True
    ca_cert_path: Optional[str] = None
    
    # Performance settings
    connection_timeout: int = 30
    prefetch_count: int = 100
    max_batch_size: int = 1000
    idle_timeout: int = 120
    
    # Spark settings - use DBFS path for Databricks persistence
    checkpoint_location: str = "/dbfs/checkpoints/iris"
    
    @classmethod
    def from_env(cls) -> "IRISConfig":
        """
        Create configuration from environment variables.
        
        Environment variables:
            IRIS_HOST: AMQP server hostname (Azure Service Bus)
            IRIS_PORT: AMQP port
            IRIS_QUEUE: IRIS queue/entity path
            IRIS_CLIENT_ID: OAuth Client ID
            IRIS_CLIENT_SECRET: OAuth Client Secret
            IRIS_SUBSCRIPTION_NAME: Durable subscription name
            IRIS_USE_TLS: Whether to use TLS (true/false)
            IRIS_PREFETCH_COUNT: Message prefetch count
            IRIS_MAX_BATCH_SIZE: Max messages per batch
            IRIS_CHECKPOINT_LOCATION: Spark checkpoint directory
        """
        return cls(
            host=os.getenv("IRIS_HOST", cls.host),
            port=int(os.getenv("IRIS_PORT", cls.port)),
            queue=os.getenv("IRIS_QUEUE", cls.queue),
            client_id=os.getenv("IRIS_CLIENT_ID", ""),
            client_secret=os.getenv("IRIS_CLIENT_SECRET", ""),
            tenant_id=os.getenv("IRIS_TENANT_ID", ""),
            subscription_name=os.getenv("IRIS_SUBSCRIPTION_NAME", cls.subscription_name),
            use_tls=os.getenv("IRIS_USE_TLS", "true").lower() == "true",
            prefetch_count=int(os.getenv("IRIS_PREFETCH_COUNT", cls.prefetch_count)),
            max_batch_size=int(os.getenv("IRIS_MAX_BATCH_SIZE", cls.max_batch_size)),
            checkpoint_location=os.getenv("IRIS_CHECKPOINT_LOCATION", cls.checkpoint_location),
        )
    
    @classmethod
    def from_databricks_secrets(
        cls,
        scope: str = "iris",
        client_id_key: str = "iris-client-id",
        client_secret_key: str = "iris-client-secret",
        tenant_id_key: str = "iris-tenant-id",
        **kwargs,
    ) -> "IRISConfig":
        """
        Create configuration with credentials from Databricks secret scope.
        
        This is the recommended method for production Databricks environments.
        Secrets are retrieved securely from the specified Databricks secret scope.
        
        Args:
            scope: Databricks secret scope name (default: "iris")
            client_id_key: Key name for the Client ID secret (default: "iris-client-id")
            client_secret_key: Key name for the Client Secret (default: "iris-client-secret")
            tenant_id_key: Key name for the Tenant ID secret (default: "iris-tenant-id")
            **kwargs: Additional IRISConfig parameters (host, port, queue, use_tls, etc.)
        
        Returns:
            IRISConfig instance with credentials from Databricks secrets
        
        Example:
            ```python
            # Basic usage with default secret names and queue
            config = IRISConfig.from_databricks_secrets()
            
            # Custom secret scope and keys
            config = IRISConfig.from_databricks_secrets(
                scope="my-scope",
                client_id_key="elexon-client-id",
                client_secret_key="elexon-client-secret",
                tenant_id_key="elexon-tenant-id",
            )
            ```
        
        Databricks Secret Setup:
            1. Create a secret scope: databricks secrets create-scope --scope iris
            2. Add Client ID: databricks secrets put --scope iris --key iris-client-id
            3. Add Client Secret: databricks secrets put --scope iris --key iris-client-secret
            4. Add Tenant ID: databricks secrets put --scope iris --key iris-tenant-id
        """
        dbutils = get_dbutils()
        
        logger.info(f"Loading IRIS credentials from Databricks secret scope: {scope}")
        
        try:
            client_id = dbutils.secrets.get(scope=scope, key=client_id_key)
            client_secret = dbutils.secrets.get(scope=scope, key=client_secret_key)
            tenant_id = dbutils.secrets.get(scope=scope, key=tenant_id_key)
        except Exception as e:
            raise ValueError(
                f"Failed to retrieve IRIS credentials from Databricks secrets. "
                f"Scope: '{scope}', Keys: '{client_id_key}', '{client_secret_key}', '{tenant_id_key}'. "
                f"Ensure the secret scope exists and contains the required keys. "
                f"Error: {e}"
            )
        
        if not client_id or not client_secret or not tenant_id:
            raise ValueError(
                f"IRIS credentials from Databricks secrets are empty. "
                f"Scope: '{scope}', Keys: '{client_id_key}', '{client_secret_key}', '{tenant_id_key}'"
            )
        
        return cls(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            **kwargs,
        )
    
    @property
    def amqp_url(self) -> str:
        """Generate AMQP connection URL."""
        protocol = "amqps" if self.use_tls else "amqp"
        auth = ""
        if self.client_id and self.client_secret:
            auth = f"{self.client_id}:{self.client_secret}@"
        return f"{protocol}://{auth}{self.host}:{self.port}"
    
    def validate(self) -> None:
        """Validate configuration."""
        if not self.host:
            raise ValueError("IRIS host is required")
        if not self.queue:
            raise ValueError("IRIS queue path is required")
        if self.prefetch_count < 1:
            raise ValueError("Prefetch count must be positive")
        if self.max_batch_size < 1:
            raise ValueError("Max batch size must be positive")
