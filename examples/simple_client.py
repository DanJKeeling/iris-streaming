"""
Simple IRIS Client Example (without PySpark)

This example demonstrates using the IRIS AMQP client directly
without PySpark, useful for testing connectivity and exploring
the message format.

Note: This example runs in Databricks and retrieves credentials
from Databricks secret scope.

Databricks Secret Setup (run once via Databricks CLI):
    databricks secrets create-scope --scope iris
    databricks secrets put --scope iris --key iris-username
    databricks secrets put --scope iris --key iris-password
"""

import json
import signal
import sys
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


def main():
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
    
    # Setup graceful shutdown
    running = True
    
    def signal_handler(sig, frame):
        nonlocal running
        print("\n\n🛑 Shutdown requested...")
        running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Connect and receive messages
    client = IRISAMQPClient(config)
    
    try:
        print("⏳ Connecting to IRIS...")
        
        if not client.start(timeout=30):
            print("❌ Failed to connect to IRIS")
            sys.exit(1)
        
        print("✅ Connected successfully!")
        print("📥 Waiting for messages (Ctrl+C to stop)...\n")
        
        message_count = 0
        start_time = datetime.now()
        
        while running:
            # Get messages with a 1-second timeout
            messages = client.get_batch(max_size=10, timeout=1.0)
            
            for msg in messages:
                message_count += 1
                format_message(msg)
            
            # Print periodic stats
            if message_count > 0 and message_count % 50 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = message_count / elapsed
                print(f"\n📊 Stats: {message_count} messages in {elapsed:.0f}s ({rate:.1f} msg/s)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    
    finally:
        print("\n🔌 Disconnecting...")
        client.stop()
        
        # Print final stats
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n📊 Final Stats:")
        print(f"   Messages received: {message_count}")
        print(f"   Duration: {elapsed:.1f} seconds")
        if elapsed > 0:
            print(f"   Average rate: {message_count/elapsed:.2f} msg/s")
        
        # Check for errors
        errors = client.get_errors()
        if errors:
            print(f"\n⚠️  Errors encountered: {len(errors)}")
            for timestamp, error in errors[-5:]:  # Show last 5 errors
                print(f"   [{timestamp}] {error}")


if __name__ == "__main__":
    main()

