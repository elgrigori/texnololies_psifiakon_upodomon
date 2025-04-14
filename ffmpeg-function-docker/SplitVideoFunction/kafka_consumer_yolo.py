import uuid  # Για τη δημιουργία μοναδικού ID
from kafka import KafkaConsumer
import json
import os
import cv2
from datetime import datetime
from datetime import timezone
from ultralytics import YOLO
from deep_sort_realtime.deepsort_tracker import DeepSort
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubProducerClient, EventData
from azure.cosmos import CosmosClient, PartitionKey
from dotenv import load_dotenv

load_dotenv()

print("AZURE_CONNECTION_STRING:", os.getenv("AZURE_CONNECTION_STRING"))
print("EVENT_HUB_CONN_STR:", os.getenv("EVENT_HUB_CONN_STR"))
print("COSMOS_ENDPOINT:", os.getenv("COSMOS_ENDPOINT"))
print("COSMOS_KEY:", os.getenv("COSMOS_KEY"))
# Azure Blob Storage
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = "videos"

# Azure Event Hub
EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONN_STR")
EVENT_HUB_NAME = "speed-alerts"

# Cosmos DB
COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
DATABASE_NAME = "vehicledata"
CONTAINER_NAME = "detections"

# Kafka
KAFKA_TOPIC = "video-chunks"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SPEED_THRESHOLD_KMH = 130
PIXEL_TO_METER = 0.05
FPS = 30

# Azure Clients
event_producer = EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONN_STR, eventhub_name=EVENT_HUB_NAME)
cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
db = cosmos_client.create_database_if_not_exists(DATABASE_NAME)
container = db.create_container_if_not_exists(id=CONTAINER_NAME, partition_key=PartitionKey(path="/id"))

model = YOLO("yolov8n.pt")
tracker = DeepSort(max_age=15)

# Event Hub client
event_producer = EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONN_STR, eventhub_name=EVENT_HUB_NAME)

def send_alert_to_event_hub(data: dict):
    try:
        event_data_batch = event_producer.create_batch()
        event_data_batch.add(EventData(json.dumps(data)))
        event_producer.send_batch(event_data_batch)
        print("Sent alert to Event Hub: {data}")
    except Exception as e:
        print("Failed to send alert to Event Hub: {e}")

def save_to_cosmos(data: dict):
    try:
        container.upsert_item(data)
        print("Data saved to Cosmos DB: {data['track_id']}")
    except Exception as e:
        print("Failed to save data to CosmosDB with error: {e}")

def download_blob_secure(container_name, blob_name, local_filename):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(local_filename, "wb") as f:
        download_stream = blob_client.download_blob()
        f.write(download_stream.readall())

def calculate_speed(prev, curr, fps):
    dx = curr[0] - prev[0]
    dy = curr[1] - prev[1]
    distance_pixels = (dx**2 + dy**2)**0.5
    distance_m = distance_pixels * PIXEL_TO_METER
    time_sec = 1 / fps
    speed_mps = distance_m / time_sec
    speed_kmh = speed_mps * 3.6
    return round(speed_kmh, 2)

def calculate_traffic_direction(prev_pos, curr_pos):
    """
    Υπολογίζει την κατεύθυνση της κίνησης (inbound ή outbound).
    Αν το όχημα κινείται προς τα δεξιά (positive x), είναι inbound.
    Αν κινείται προς τα αριστερά (negative x), είναι outbound.
    """
    if curr_pos > prev_pos:  
        return "inbound"
    else:
        return "outbound"

def run_yolo_deepsort_speed(video_path):
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS) or FPS
    track_history = {}

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        results = model(frame, verbose=False)[0]
        detections = []

        for result in results.boxes:
            cls = int(result.cls[0])
            vehicle_type = model.names[cls]
            
            if model.names[cls] in ["car", "truck"]:
                x1, y1, x2, y2 = map(int, result.xyxy[0])
                conf = float(result.conf[0])
                detections.append(([x1, y1, x2 - x1, y2 - y1], conf, cls))

        tracks = tracker.update_tracks(detections, frame=frame)

        for track in tracks:
            if not track.is_confirmed():
                continue
            track_id = track.track_id
            x, y, w, h = track.to_ltrb()
            cx = int((x + w) / 2)
            cy = int((y + h) / 2)

            if track_id not in track_history:
                track_history[track_id] = []

            track_history[track_id].append((cx, cy))

            if len(track_history[track_id]) >= 2:
                prev_pos = track_history[track_id][-2]
                curr_pos = track_history[track_id][-1]
                speed = calculate_speed(prev_pos, curr_pos, fps)

                # Υπολογισμός κατεύθυνσης κίνησης
                traffic_direction = calculate_traffic_direction(prev_pos, curr_pos)

                alert_data = {
                        "id": str(uuid.uuid4()),  
                        "track_id": track_id,
                        "speed": speed,
                        "video_chunk": video_path,
                        "timestamp": str(datetime.now(timezone.utc)),
                        "traffic_direction": traffic_direction,
                        "vehicle_type": vehicle_type
                    }
                save_to_cosmos(alert_data)

                if speed > SPEED_THRESHOLD_KMH:
                    print("ALERT! Track vehicle with {track_id} moving at {speed} km/h in direction: {traffic_direction}")

                    send_alert_to_event_hub(alert_data)

    cap.release()

# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    group_id='video-processor',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    metadata = message.value
    blob_name = metadata['chunk_name']
    filename = blob_name

    try:
        download_blob_secure(BLOB_CONTAINER_NAME, blob_name, filename)
        run_yolo_deepsort_speed(filename)
        os.remove(filename)
    except Exception as e:
        print("Error processing chunk video with filename {filename}: {e}")
