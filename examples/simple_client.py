"""
Simple IRIS Client Example (without PySpark)

This example demonstrates using the IRIS AMQP client directly
without PySpark, useful for testing connectivity and exploring
the message format.

Databricks Setup:
    1. Install cluster library: python-qpid-proton (via PyPI)
    2. Set up secrets (run once via Databricks CLI):
        databricks secrets create-scope --scope iris
        databricks secrets put --scope iris --key iris-username
        databricks secrets put --scope iris --key iris-password

Usage in Databricks notebook:
    client = test_iris_connection()
    # ... view messages ...
    client.stop()
"""

import json
from datetime import datetime

from iris_connector import IRISConfig, IRISAMQPClient, IRISTopics


def format_message(msg):
    """Format an IRIS message for display."""
    print(f"\n{'─'*60}")
    print(f"📨 Topic: {msg.topic}")
    print(f"⏰ Received: {msg.timestamp or 'N/A'}")
    print(f"🆔 Message ID: {msg.message_id or 'N/A'}")
    
    if isinstance(msg.body, dict):
        print("📄 Body:")
        print(json.dumps(msg.body, indent=2, default=str))
    else:
        print(f"📄 Body: {msg.body}")
    
    if msg.properties:
        print(f"🏷️  Properties: {msg.properties}")


def test_iris_connection(num_messages: int = 10) -> IRISAMQPClient:
    """
    Test IRIS connectivity and receive a sample of messages.
    
    This function is designed to be run in a Databricks notebook cell.
    It connects to IRIS, receives a specified number of messages, 
    and returns the client for further interaction.
    
    Args:
        num_messages: Number of messages to receive before returning
        
    Returns:
        IRISAMQPClient instance (remember to call client.stop() when done)
        
    Example:
        >>> client = test_iris_connection(num_messages=5)
        >>> # View more messages
        >>> for msg in client.get_batch(max_size=10, timeout=5.0):
        ...     format_message(msg)
        >>> # When done
        >>> client.stop()
    """
    # Configure IRIS connection with credentials from Databricks secrets
    config = IRISConfig.from_databricks_secrets(
        scope="iris",
        username_key="iris-username",
        password_key="iris-password",
        topics=[
            IRISTopics.FREQ,           # System Frequency (updates every 2 seconds)
        ],
        use_tls=True,
        prefetch_count=10,
    )
    
    print("🔌 IRIS Simple Client")
    print(f"📡 Connecting to: {config.host}:{config.port}")
    print(f"📋 Topics: {config.topics}")
    print()
    
    # Connect to IRIS
    client = IRISAMQPClient(config)
    
    print("⏳ Connecting to IRIS...")
    
    if not client.start(timeout=30):
        print("❌ Failed to connect to IRIS")
        raise ConnectionError("Failed to connect to IRIS")
    
    print("✅ Connected successfully!")
    print(f"📥 Receiving {num_messages} messages...\n")
    
    message_count = 0
    start_time = datetime.now()
    
    # Receive specified number of messages
    while message_count < num_messages:
        messages = client.get_batch(max_size=10, timeout=5.0)
        
        if not messages:
            print("⏳ Waiting for messages...")
            continue
            
        for msg in messages:
            message_count += 1
            format_message(msg)
            
            if message_count >= num_messages:
                break
    
    # Print stats
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n{'─'*60}")
    print(f"📊 Received {message_count} messages in {elapsed:.1f}s")
    if elapsed > 0:
        print(f"   Rate: {message_count/elapsed:.2f} msg/s")
    
    print("\n💡 Client is still connected. To continue receiving messages:")
    print("   >>> messages = client.get_batch(max_size=10, timeout=5.0)")
    print("   >>> for msg in messages: format_message(msg)")
    print("\n   When done, call: client.stop()")
    
    return client


# When run in a notebook, this provides a quick connectivity test
# Run: client = test_iris_connection()
# Stop: client.stop()

