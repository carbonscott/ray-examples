import ray
import time
import random
from shared_queue import create_queue
import signal

# Initialize Ray and create the Queue actor
try:
    ray.init(namespace='my')
    queue = create_queue(maxsize=100)
except Exception as e:
    print(f"Error initializing Ray or creating queue: {e}")
    exit(1)

def signal_handler(sig, frame):
    print("Ctrl+C pressed. Shutting down...")
    ray.shutdown()
    exit(0)

def produce_data():
    while True:
        try:
            # Produce data
            data = random.randint(1, 1000)

            # Add to queue
            success = ray.get(queue.put.remote(data))
            if success:
                print(f"Produced: {data}")
            else:
                print("Queue is full, waiting...")
                time.sleep(1)

            time.sleep(0.1)  # Small delay to prevent flooding
        except ray.exceptions.RayActorError:
            print("Queue actor is dead. Exiting...")
            break
        except Exception as e:
            print(f"Error in produce_data: {e}")
            time.sleep(1)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    try:
        produce_data()
    except Exception as e:
        print(f"Unhandled exception in main: {e}")
    finally:
        ray.shutdown()
