import json
from google.cloud import pubsub_v1
import time
start_time = time.time()

project_id = "introspec-duale-duale"
topic_id = "my-topic"
filename = "bcsample.json"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

count = 0
with open(filename, "r") as f:
    while True:
        line = f.readline()
        if not line:
            break 

        line = line.strip()
        if line.startswith("--- Vehicle ID:"):
            vehicle_id = line.split(":")[1].strip().strip(" -")
            
            json_lines = []
            # Start reading the array
            while True:
                json_line = f.readline()
                if not json_line or json_line.strip().startswith("--- Vehicle ID:"):
                    break
                json_lines.append(json_line)

            if json_line.strip().startswith("--- Vehicle ID:"):
                f.seek(f.tell() - len(json_line))
            # Parse the JSON array
            try:
                records = json.loads("".join(json_lines))
            except json.JSONDecodeError as e:
                continue

            for record in records:
                data = json.dumps(record).encode("utf-8")
                future = publisher.publish(
                    topic_path,
                    data,
                    vehicle_id=vehicle_id  # metadata as Pub/Sub attribute
                )
                count += 1

print(f"\nTotal records published: \033[33m{count}\033[0m")
print(f"\nProducer took {time.time() - start_time:.2f} seconds")
