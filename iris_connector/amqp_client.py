"""
AMQP 1.0 client for Elexon's IRIS message server.

Uses Apache Qpid Proton for AMQP 1.0 protocol support.
"""

import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional
from queue import Queue, Empty

try:
    from proton import Message, SSLDomain
    from proton.handlers import MessagingHandler
    from proton.reactor import Container, Selector
    PROTON_AVAILABLE = True
except ImportError:
    PROTON_AVAILABLE = False
    Message = None
    MessagingHandler = object
    Container = None
    SSLDomain = None

from iris_connector.config import IRISConfig


logger = logging.getLogger(__name__)


@dataclass
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
        raw_message: Original AMQP message object
    """
    topic: str
    body: Any
    message_id: Optional[str] = None
    correlation_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    properties: dict = None
    raw_message: Any = None
    
    def __post_init__(self):
        if self.properties is None:
            self.properties = {}
    
    def to_dict(self) -> dict:
        """Convert message to dictionary for Spark DataFrame."""
        return {
            "topic": self.topic,
            "body": json.dumps(self.body) if isinstance(self.body, (dict, list)) else str(self.body),
            "message_id": self.message_id,
            "correlation_id": self.correlation_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "properties": json.dumps(self.properties),
            "received_at": datetime.utcnow().isoformat(),
        }
    
    @classmethod
    def from_proton_message(cls, message: "Message", topic: str) -> "IRISMessage":
        """Create IRISMessage from a Proton AMQP message."""
        body = message.body
        
        # Try to parse JSON body
        if isinstance(body, (bytes, str)):
            try:
                body = json.loads(body if isinstance(body, str) else body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        
        # Extract timestamp
        timestamp = None
        if message.creation_time:
            timestamp = datetime.fromtimestamp(message.creation_time / 1000.0)
        
        # Extract properties
        properties = {}
        if message.properties:
            properties = dict(message.properties)
        
        return cls(
            topic=topic,
            body=body,
            message_id=message.id,
            correlation_id=message.correlation_id,
            timestamp=timestamp,
            properties=properties,
            raw_message=message,
        )


class IRISMessageHandler(MessagingHandler):
    """
    AMQP 1.0 message handler for IRIS connections.
    
    Handles connection lifecycle, message receiving, and error handling.
    """
    
    def __init__(
        self,
        config: IRISConfig,
        message_queue: Queue,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        if PROTON_AVAILABLE:
            super().__init__()
        self.config = config
        self.message_queue = message_queue
        self.on_error = on_error
        self.receivers = {}
        self.connection = None
        self._running = False
        self._connected = threading.Event()
        self._message_count = 0
    
    def on_start(self, event):
        """Called when the container starts."""
        logger.info(f"Connecting to IRIS at {self.config.host}:{self.config.port}")
        
        # Configure SSL if using TLS
        ssl_domain = None
        if self.config.use_tls and PROTON_AVAILABLE:
            ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)
            ssl_domain.set_peer_authentication(
                SSLDomain.VERIFY_PEER if self.config.verify_ssl else SSLDomain.ANONYMOUS_PEER
            )
            if self.config.ca_cert_path:
                ssl_domain.set_trusted_ca_db(self.config.ca_cert_path)
        
        # Build connection URL
        url = self.config.amqp_url
        
        # Create connection with authentication (Client ID and Secret)
        self.connection = event.container.connect(
            url,
            user=self.config.client_id,
            password=self.config.client_secret,
            ssl_domain=ssl_domain,
            heartbeat=self.config.idle_timeout,
        )
        self._running = True
    
    def on_connection_opened(self, event):
        """Called when connection is established."""
        logger.info("Connected to IRIS successfully")
        
        # Create receiver for the IRIS queue
        queue = self.config.queue
        receiver = event.container.create_receiver(
            self.connection,
            queue,
            name=f"{self.config.subscription_name}-{queue.replace('.', '-')}",
            options=Selector(f"TRUE") if PROTON_AVAILABLE else None,
        )
        receiver.flow(self.config.prefetch_count)
        self.receivers[queue] = receiver
        logger.info(f"Subscribed to queue: {queue}")
        
        self._connected.set()
    
    def on_message(self, event):
        """Called when a message is received."""
        try:
            # Determine the topic from the receiver
            topic = event.receiver.source.address
            
            # Parse the message
            iris_message = IRISMessage.from_proton_message(event.message, topic)
            
            # Add to queue for processing
            self.message_queue.put(iris_message)
            self._message_count += 1
            
            # Accept the message
            event.delivery.settle()
            
            if self._message_count % 1000 == 0:
                logger.debug(f"Received {self._message_count} messages")
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            if self.on_error:
                self.on_error(e)
    
    def on_connection_error(self, event):
        """Called on connection error."""
        error = event.connection.remote_condition
        logger.error(f"Connection error: {error}")
        if self.on_error:
            self.on_error(Exception(f"Connection error: {error}"))
    
    def on_transport_error(self, event):
        """Called on transport error."""
        error = event.transport.condition
        logger.error(f"Transport error: {error}")
        if self.on_error:
            self.on_error(Exception(f"Transport error: {error}"))
    
    def on_link_error(self, event):
        """Called on link error."""
        error = event.link.remote_condition
        logger.error(f"Link error: {error}")
        if self.on_error:
            self.on_error(Exception(f"Link error: {error}"))
    
    def on_disconnected(self, event):
        """Called when disconnected."""
        logger.warning("Disconnected from IRIS")
        self._connected.clear()
        if self._running and self.on_error:
            self.on_error(Exception("Disconnected from IRIS"))
    
    def stop(self):
        """Stop the handler and close connections."""
        self._running = False
        if self.connection:
            self.connection.close()
            logger.info("Closed IRIS connection")


class IRISAMQPClient:
    """
    High-level AMQP 1.0 client for Elexon's IRIS service.
    
    Provides a simple interface to connect to IRIS and receive messages
    asynchronously, with support for batching for PySpark integration.
    
    Example:
        ```python
        config = IRISConfig.from_databricks_secrets(
            scope="iris",
            topics=["bmrs/FREQ", "bmrs/INDDEM"],
        )
        
        client = IRISAMQPClient(config)
        client.start()
        
        # Get a batch of messages
        messages = client.get_batch(max_size=100, timeout=5.0)
        
        client.stop()
        ```
    """
    
    def __init__(self, config: IRISConfig):
        """
        Initialize the IRIS AMQP client.
        
        Args:
            config: IRIS connection configuration
        """
        if not PROTON_AVAILABLE:
            raise ImportError(
                "python-qpid-proton is required for AMQP 1.0 support. "
                "Install it with: pip install python-qpid-proton"
            )
        
        self.config = config
        self.config.validate()
        
        self._message_queue: Queue = Queue()
        self._handler: Optional[IRISMessageHandler] = None
        self._container: Optional[Container] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._errors: deque = deque(maxlen=100)
    
    def _on_error(self, error: Exception):
        """Handle errors from the message handler."""
        self._errors.append((datetime.utcnow(), error))
        logger.error(f"IRIS client error: {error}")
    
    def _run_container(self):
        """Run the AMQP container in a background thread."""
        try:
            self._handler = IRISMessageHandler(
                self.config,
                self._message_queue,
                on_error=self._on_error,
            )
            self._container = Container(self._handler)
            self._container.run()
        except Exception as e:
            logger.error(f"Container error: {e}")
            self._on_error(e)
        finally:
            self._running = False
    
    def start(self, timeout: float = 30.0) -> bool:
        """
        Start the AMQP client and connect to IRIS.
        
        Args:
            timeout: Connection timeout in seconds
            
        Returns:
            True if connected successfully, False otherwise
        """
        if self._running:
            logger.warning("Client already running")
            return True
        
        logger.info("Starting IRIS AMQP client")
        self._running = True
        
        # Start container in background thread
        self._thread = threading.Thread(target=self._run_container, daemon=True)
        self._thread.start()
        
        # Wait for connection
        if self._handler and self._handler._connected.wait(timeout):
            logger.info("IRIS client started successfully")
            return True
        else:
            logger.error("Failed to connect to IRIS within timeout")
            return False
    
    def stop(self):
        """Stop the AMQP client and disconnect from IRIS."""
        if not self._running:
            return
        
        logger.info("Stopping IRIS AMQP client")
        self._running = False
        
        if self._handler:
            self._handler.stop()
        
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        
        logger.info("IRIS client stopped")
    
    def is_connected(self) -> bool:
        """Check if connected to IRIS."""
        return (
            self._running
            and self._handler is not None
            and self._handler._connected.is_set()
        )
    
    def get_message(self, timeout: float = 1.0) -> Optional[IRISMessage]:
        """
        Get a single message from the queue.
        
        Args:
            timeout: How long to wait for a message
            
        Returns:
            IRISMessage if available, None otherwise
        """
        try:
            return self._message_queue.get(timeout=timeout)
        except Empty:
            return None
    
    def get_batch(
        self,
        max_size: int = None,
        timeout: float = 1.0,
    ) -> list[IRISMessage]:
        """
        Get a batch of messages from the queue.
        
        Args:
            max_size: Maximum number of messages to return (default: config.max_batch_size)
            timeout: How long to wait for the first message
            
        Returns:
            List of IRISMessage objects
        """
        max_size = max_size or self.config.max_batch_size
        messages = []
        
        # Wait for first message with timeout
        try:
            first_message = self._message_queue.get(timeout=timeout)
            messages.append(first_message)
        except Empty:
            return messages
        
        # Drain queue up to max_size (non-blocking)
        while len(messages) < max_size:
            try:
                message = self._message_queue.get_nowait()
                messages.append(message)
            except Empty:
                break
        
        return messages
    
    def pending_count(self) -> int:
        """Get the number of pending messages in the queue."""
        return self._message_queue.qsize()
    
    def get_errors(self) -> list[tuple[datetime, Exception]]:
        """Get recent errors."""
        return list(self._errors)
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
        return False

