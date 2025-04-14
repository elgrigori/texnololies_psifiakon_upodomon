import cv2
import os
import json
from azure.storage.blob import BlobServiceClient
from kafka import KafkaProducer
load_dotenv()

print("AZURE_CONNECTION_STRING:", os.getenv("AZURE_CONNECTION_STRING"))
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = "videos"
KAFKA_TOPIC = "video-chunks"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

def split_video_and_upload(video_path, chunk_duration=60):
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print("Error: Could not open video file.")
        return

    fps = int(cap.get(cv2.CAP_PROP_FPS))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    # total_frames = 2
    chunk_size = fps * chunk_duration
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")

    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    frame_counter = 0
    chunk_counter = 1

    while frame_counter < total_frames:
        chunk_filename = f"chunk_{chunk_counter}.mp4"
        out = cv2.VideoWriter(chunk_filename, fourcc, fps, (frame_width, frame_height))

        for _ in range(chunk_size):
            ret, frame = cap.read()
            if not ret:
                break
            out.write(frame)
            frame_counter += 1

        out.release()
        print("Saved chunk: {chunk_filename}")

        # Upload to Azure
        with open(chunk_filename, "rb") as data:
            blob_client = blob_service.get_blob_client(container=BLOB_CONTAINER_NAME, blob=chunk_filename)
            blob_client.upload_blob(data, overwrite=True)
            print("Uploaded {chunk_filename} to Azure")

        # Send metadata to Kafka
        metadata = {
            "chunk_name": chunk_filename,
            "blob_url": f"https://videocarstorage.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{chunk_filename}"
        }
        producer.send(KAFKA_TOPIC, metadata)
        print("Chunk video metadata: {metadata}")

        # if chunk_counter ==1: 
        #     break
        
        # os.remove(chunk_filename)
        chunk_counter += 1
        

    cap.release()
    producer.flush()
    print("Video split completed!")

# 🔁 Τρέξε το
split_video_and_upload("videocar.mp4")
