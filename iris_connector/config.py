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
    data via AMQP 1.0 protocol.
    
    Attributes:
        host: IRIS AMQP server hostname
        port: AMQP port (default 5671 for AMQPS, 5672 for AMQP)
        username: Authentication username (API key or service account)
        password: Authentication password/secret
        subscription_name: Name for the durable subscription
        topics: List of topics to subscribe to
        use_tls: Whether to use TLS/SSL encryption
        connection_timeout: Connection timeout in seconds
        prefetch_count: Number of messages to prefetch
        max_batch_size: Maximum messages per micro-batch
        checkpoint_location: Spark checkpoint directory
    """
    
    # Connection settings
    host: str = "bmrs-iris.elexon.co.uk"
    port: int = 5671
    username: str = ""
    password: str = ""
    
    # Subscription settings
    subscription_name: str = "pyspark-iris-connector"
    topics: list[str] = field(default_factory=lambda: [
        "bmrs/BOALF",  # Bid Offer Acceptance Level Flagged
        "bmrs/PN",     # Physical Notification
        "bmrs/FREQ",   # System Frequency
        "bmrs/INDDEM", # Indicated Demand
        "bmrs/INDGEN", # Indicated Generation
        "bmrs/MID",    # Market Index Data
        "bmrs/TEMP",   # Temperature Data
    ])
    
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
            IRIS_HOST: AMQP server hostname
            IRIS_PORT: AMQP port
            IRIS_USERNAME: Authentication username
            IRIS_PASSWORD: Authentication password
            IRIS_SUBSCRIPTION_NAME: Durable subscription name
            IRIS_TOPICS: Comma-separated list of topics
            IRIS_USE_TLS: Whether to use TLS (true/false)
            IRIS_PREFETCH_COUNT: Message prefetch count
            IRIS_MAX_BATCH_SIZE: Max messages per batch
            IRIS_CHECKPOINT_LOCATION: Spark checkpoint directory
        """
        topics_str = os.getenv("IRIS_TOPICS", "")
        topics = [t.strip() for t in topics_str.split(",") if t.strip()] if topics_str else None
        
        return cls(
            host=os.getenv("IRIS_HOST", cls.host),
            port=int(os.getenv("IRIS_PORT", cls.port)),
            username=os.getenv("IRIS_USERNAME", ""),
            password=os.getenv("IRIS_PASSWORD", ""),
            subscription_name=os.getenv("IRIS_SUBSCRIPTION_NAME", cls.subscription_name),
            topics=topics if topics else cls.topics,
            use_tls=os.getenv("IRIS_USE_TLS", "true").lower() == "true",
            prefetch_count=int(os.getenv("IRIS_PREFETCH_COUNT", cls.prefetch_count)),
            max_batch_size=int(os.getenv("IRIS_MAX_BATCH_SIZE", cls.max_batch_size)),
            checkpoint_location=os.getenv("IRIS_CHECKPOINT_LOCATION", cls.checkpoint_location),
        )
    
    @classmethod
    def from_databricks_secrets(
        cls,
        scope: str = "iris",
        username_key: str = "iris-username",
        password_key: str = "iris-password",
        topics: Optional[list[str]] = None,
        **kwargs,
    ) -> "IRISConfig":
        """
        Create configuration with credentials from Databricks secret scope.
        
        This is the recommended method for production Databricks environments.
        Secrets are retrieved securely from the specified Databricks secret scope.
        
        Args:
            scope: Databricks secret scope name (default: "iris")
            username_key: Key name for the username/API key secret (default: "iris-username")
            password_key: Key name for the password/API secret (default: "iris-password")
            topics: List of IRIS topics to subscribe to
            **kwargs: Additional IRISConfig parameters (host, port, use_tls, etc.)
        
        Returns:
            IRISConfig instance with credentials from Databricks secrets
        
        Example:
            ```python
            # Basic usage with default secret names
            config = IRISConfig.from_databricks_secrets(
                topics=[IRISTopics.FREQ, IRISTopics.INDDEM],
            )
            
            # Custom secret scope and keys
            config = IRISConfig.from_databricks_secrets(
                scope="my-scope",
                username_key="elexon-api-key",
                password_key="elexon-api-secret",
                topics=[IRISTopics.FREQ],
            )
            ```
        
        Databricks Secret Setup:
            1. Create a secret scope: databricks secrets create-scope --scope iris
            2. Add username: databricks secrets put --scope iris --key iris-username
            3. Add password: databricks secrets put --scope iris --key iris-password
        """
        dbutils = get_dbutils()
        
        logger.info(f"Loading IRIS credentials from Databricks secret scope: {scope}")
        
        try:
            username = dbutils.secrets.get(scope=scope, key=username_key)
            password = dbutils.secrets.get(scope=scope, key=password_key)
        except Exception as e:
            raise ValueError(
                f"Failed to retrieve IRIS credentials from Databricks secrets. "
                f"Scope: '{scope}', Keys: '{username_key}', '{password_key}'. "
                f"Ensure the secret scope exists and contains the required keys. "
                f"Error: {e}"
            )
        
        if not username or not password:
            raise ValueError(
                f"IRIS credentials from Databricks secrets are empty. "
                f"Scope: '{scope}', Keys: '{username_key}', '{password_key}'"
            )
        
        return cls(
            username=username,
            password=password,
            topics=topics if topics else cls.topics,
            **kwargs,
        )
    
    @property
    def amqp_url(self) -> str:
        """Generate AMQP connection URL."""
        protocol = "amqps" if self.use_tls else "amqp"
        auth = ""
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"
        return f"{protocol}://{auth}{self.host}:{self.port}"
    
    def validate(self) -> None:
        """Validate configuration."""
        if not self.host:
            raise ValueError("IRIS host is required")
        if not self.topics:
            raise ValueError("At least one topic is required")
        if self.prefetch_count < 1:
            raise ValueError("Prefetch count must be positive")
        if self.max_batch_size < 1:
            raise ValueError("Max batch size must be positive")


# Common IRIS topic definitions for reference
class IRISTopics:
    """
    Standard IRIS BMRS topics available for subscription.
    
    These represent various data feeds from the Balancing Mechanism
    Reporting Service (BMRS).
    """
    
    # Real-time operational data
    BOALF = "bmrs/BOALF"          # Bid Offer Acceptance Level Flagged
    PN = "bmrs/PN"                # Physical Notification
    QPN = "bmrs/QPN"              # Quiescent Physical Notification
    MEL = "bmrs/MEL"              # Maximum Export Limit
    MIL = "bmrs/MIL"              # Maximum Import Limit
    
    # System data
    FREQ = "bmrs/FREQ"            # System Frequency
    INDDEM = "bmrs/INDDEM"        # Indicated Demand
    INDGEN = "bmrs/INDGEN"        # Indicated Generation
    INDO = "bmrs/INDO"            # Initial Demand Outturn
    ITSDO = "bmrs/ITSDO"          # Initial Transmission System Demand Outturn
    
    # Market data
    MID = "bmrs/MID"              # Market Index Data
    IMBALNGC = "bmrs/IMBALNGC"    # Imbalance NGC
    DISBSAD = "bmrs/DISBSAD"      # Disaggregated BSAD
    NETBSAD = "bmrs/NETBSAD"      # Net BSAD
    
    # Generation and demand
    B1610 = "bmrs/B1610"          # Actual Generation per Type
    B1620 = "bmrs/B1620"          # Actual Aggregated Generation
    B1630 = "bmrs/B1630"          # Actual Or Estimated Wind and Solar
    B0610 = "bmrs/B0610"          # Actual Total Load
    
    # Forecast data
    B0620 = "bmrs/B0620"          # Day-Ahead Total Load Forecast
    B0630 = "bmrs/B0630"          # Week-Ahead Total Load Forecast
    B1440 = "bmrs/B1440"          # Generation Forecasts for Wind and Solar
    
    # Temperature
    TEMP = "bmrs/TEMP"            # Temperature Data
    
    # Balancing costs
    SYSDEM = "bmrs/SYSDEM"        # System Demand
    SYSWARN = "bmrs/SYSWARN"      # System Warnings
    
    @classmethod
    def all_topics(cls) -> list[str]:
        """Return all available topics."""
        return [
            value for name, value in vars(cls).items()
            if not name.startswith("_") and isinstance(value, str)
        ]

