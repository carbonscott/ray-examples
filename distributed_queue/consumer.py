import ray
import time
import sys
import signal

def signal_handler(sig, frame):
    print("Ctrl+C pressed. Shutting down...")
    ray.shutdown()
    exit(0)

def consume_data(consumer_id):
    # Initialize Ray (connecting to the existing cluster)
    try:
        ray.init(address='auto')
        # Get the shared queue actor
        queue = ray.get_actor("shared_queue", namespace='my')
    except Exception as e:
        print(f"Error initializing Ray or getting queue actor: {e}")
        return

    while True:
        try:
            # Try to get data from queue
            data = ray.get(queue.get.remote())

            if data is not None:
                # Process data
                print(f"Consumer {consumer_id} processed: {data}")

                # Simulate processing time
                time.sleep(0.5)
            else:
                print(f"Consumer {consumer_id} waiting for data...")
                time.sleep(1)
        except ray.exceptions.RayActorError:
            print("Queue actor is dead. Exiting...")
            break
        except Exception as e:
            print(f"Error in consume_data: {e}")
            time.sleep(1)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    consumer_id = sys.argv[1] if len(sys.argv) > 1 else 1
    try:
        consume_data(consumer_id)
    except Exception as e:
        print(f"Unhandled exception in main: {e}")
    finally:
        ray.shutdown()
