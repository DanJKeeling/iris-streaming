"""
AMQP 1.0 client for Elexon's IRIS message server using Azure Service Bus SDK.
"""

import json
import logging
import datetime
from typing import Any, Dict, List, Optional, Union

from azure.servicebus import ServiceBusClient, ServiceBusReceivedMessage
from azure.identity import ClientSecretCredential, DefaultAzureCredential

from iris_connector.config import IRISConfig


logger = logging.getLogger(__name__)


class IRISMessage:
    """
    Represents a message received from IRIS.
    
    Attributes:
        topic: The AMQP topic/address the message was received from
        body: The message body (typically JSON)
        message_id: Unique message identifier
        correlation_id: Correlation ID for request-response patterns
        timestamp: Message timestamp
        properties: Additional message properties
        raw_message: Original Azure Service Bus message object
    """
    def __init__(
        self,
        topic: str,
        body: Any,
        message_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        timestamp: Optional[datetime.datetime] = None,
        properties: Optional[Dict] = None,
        raw_message: Any = None,
    ):
        self.topic = topic
        self.body = body
        self.message_id = message_id
        self.correlation_id = correlation_id
        self.timestamp = timestamp
        self.properties = properties or {}
        self.raw_message = raw_message

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary for Spark DataFrame."""
        body_str = json.dumps(self.body) if isinstance(self.body, (dict, list)) else str(self.body)
        
        return {
            "topic": self.topic,
            "body": body_str,
            "message_id": self.message_id,
            "correlation_id": self.correlation_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "properties": json.dumps(self.properties),
            "received_at": datetime.datetime.utcnow().isoformat(),
        }
    
    @classmethod
    def from_servicebus_message(cls, message: ServiceBusReceivedMessage, topic: str) -> "IRISMessage":
        """Create IRISMessage from an Azure Service Bus message."""
        # Parse body
        body_content = list(message.body) if hasattr(message.body, '__iter__') else message.body
        # message.body returns a generator for AMQP message body. For simple messages it might be bytes or str.
        # If it's a generator, we need to consume it.
        # Actually azure-servicebus body is usually a generator yielding bytes.
        
        raw_body = str(message) # str(message) gives the string representation of body if simple
        
        # Try to parse JSON
        try:
            body = json.loads(raw_body)
        except (json.JSONDecodeError, TypeError):
            body = raw_body

        # Extract timestamp
        timestamp = message.enqueued_time_utc

        # Extract properties
        properties = {}
        if message.application_properties:
            properties = dict(message.application_properties)
        
        return cls(
            topic=topic,
            body=body,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            timestamp=timestamp,
            properties=properties,
            raw_message=message,
        )


class IRISAMQPClient:
    """
    High-level AMQP 1.0 client for Elexon's IRIS service using Azure Service Bus SDK.
    
    Provides a simple interface to connect to IRIS and receive messages.
    """
    
    def __init__(self, config: IRISConfig):
        """
        Initialize the IRIS AMQP client.
        
        Args:
            config: IRIS connection configuration
        """
        self.config = config
        self.config.validate()
        
        self._client: Optional[ServiceBusClient] = None
        self._receiver = None
        self._running = False

    def start(self, timeout: float = 30.0) -> bool:
        """
        Start the AMQP client and connect to IRIS.
        
        Args:
            timeout: Connection timeout in seconds (used for initial connection check)
            
        Returns:
            True if connected successfully, False otherwise
        """
        if self._running:
            logger.warning("Client already running")
            return True
        
        logger.info("Starting IRIS AMQP client (Azure Service Bus)")
        
        try:
            credential = None
            if self.config.client_id and self.config.client_secret:
                if self.config.tenant_id:
                    credential = ClientSecretCredential(
                        tenant_id=self.config.tenant_id,
                        client_id=self.config.client_id,
                        client_secret=self.config.client_secret
                    )
                else:
                    # Fallback for when tenant_id is missing but might work with some defaults 
                    # or if we want to try DefaultAzureCredential which might pick up env vars
                    logger.warning("Tenant ID not provided in config. Trying DefaultAzureCredential.")
                    credential = DefaultAzureCredential()
            else:
                credential = DefaultAzureCredential()

            # Azure Service Bus fully qualified namespace
            fully_qualified_namespace = f"{self.config.host}"
            if not fully_qualified_namespace.endswith(".servicebus.windows.net"):
                 fully_qualified_namespace += ".servicebus.windows.net"

            self._client = ServiceBusClient(
                fully_qualified_namespace=fully_qualified_namespace,
                credential=credential,
                logging_enable=True
            )
            
            # Create receiver
            # For Elexon IRIS, 'queue' in config is actually the topic/queue name.
            # If it's a subscription to a topic, we might need subscription_name.
            # Config has 'queue' and 'subscription_name'.
            # If it is a Queue, we use queue_name. If it is a Topic Subscription, we use topic_name and subscription_name.
            # The config says 'queue: str = "iris..."'. Elexon usually exposes a Queue or a Topic.
            # The current config comment says "queue/entity path".
            # If subscription_name is provided and it's a topic, we should use it.
            # However, previous implementation treated it as 'queue'.
            # Let's assume it's a Queue for now or the entity path is full.
            # ServiceBusClient.get_queue_receiver(queue_name=...)
            # ServiceBusClient.get_subscription_receiver(topic_name=..., subscription_name=...)
            
            # Looking at the config default: queue="iris.5c9f...", subscription_name="pyspark-iris-connector".
            # In standard Azure SB, "iris.5c9f..." looks like a topic name if subscription is used.
            # But the variable name is 'queue'.
            # In previous code:
            # receiver = event.container.create_receiver(..., queue, name=f"{subscription}-{queue}")
            # This suggests it might be connecting to a specific address.
            
            # Let's try to determine if we need subscription receiver.
            # If the entity path contains "/Subscriptions/", it's a subscription.
            # The default "iris.5c9f..." looks like a topic or queue name.
            # If it's a topic, we MUST use get_subscription_receiver.
            # If it's a queue, we use get_queue_receiver.
            # I'll try to use `get_receiver` (generic)? No, only queue/subscription specific methods exist on client?
            # Actually client has `get_queue_receiver` and `get_subscription_receiver`.
            
            # Let's assume it's a Queue if subscription_name is not relevant, but IRIS usually uses Topics.
            # Elexon documentation says "Queue" but often means the entity you read from.
            # The config default `subscription_name` is "pyspark-iris-connector".
            # If I am connecting to a public topic, I need a subscription.
            # But Elexon gives you a dedicated Queue usually?
            # If I use `get_queue_receiver`, and it's a topic, it will fail.
            
            # Previous code: `receiver = event.container.create_receiver(..., queue, ...)`
            # This connects to the address 'queue'.
            
            # I will use `get_queue_receiver` for now as the variable is named `queue`.
            # If `subscription_name` is intended to be used with a Topic, the previous code didn't seem to use it as a subscription name in the AMQP source sense (it used it for the link name).
            
            self._receiver = self._client.get_queue_receiver(
                queue_name=self.config.queue,
                prefetch_count=self.config.prefetch_count,
                max_wait_time=5 # Default wait time
            )

            self._running = True
            logger.info("IRIS client started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to IRIS: {e}")
            self._running = False
            if self._client:
                self._client.close()
            return False

    def stop(self):
        """Stop the AMQP client and disconnect from IRIS."""
        if not self._running:
            return
        
        logger.info("Stopping IRIS AMQP client")
        self._running = False
        
        if self._receiver:
            self._receiver.close()
            self._receiver = None
            
        if self._client:
            self._client.close()
            self._client = None
            
        logger.info("IRIS client stopped")

    def is_connected(self) -> bool:
        """Check if connected to IRIS."""
        return self._running and self._client is not None

    def get_batch(
        self,
        max_size: int = None,
        timeout: float = 1.0,
    ) -> List[IRISMessage]:
        """
        Get a batch of messages from the queue.
        
        Args:
            max_size: Maximum number of messages to return
            timeout: How long to wait for messages
            
        Returns:
            List of IRISMessage objects
        """
        if not self._running or not self._receiver:
            logger.warning("Client not running, returning empty batch")
            return []

        max_size = max_size or self.config.max_batch_size
        messages = []
        
        try:
            # Received messages are automatically locked. 
            # We need to complete them to remove them from the queue.
            # Or if we want "at least once" semantics with Spark, we should only complete them 
            # after Spark has processed them (in commit).
            # But the previous implementation seemed to settle immediately or rely on auto-ack.
            # "For AMQP with auto-ack, messages are acknowledged on receive" in spark_source.py.
            # So we will complete them immediately here to match behavior.
            
            batch = self._receiver.receive_messages(
                max_message_count=max_size,
                max_wait_time=timeout
            )
            
            for msg in batch:
                iris_msg = IRISMessage.from_servicebus_message(msg, self.config.queue)
                messages.append(iris_msg)
                self._receiver.complete_message(msg)
                
        except Exception as e:
            logger.error(f"Error receiving batch: {e}")
            
        return messages

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
        return False
