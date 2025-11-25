#!/bin/sh

echo "Waiting for MinIO server to be ready..."
sleep 2

mc alias set local $MINIO_SERVER_URL $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD

echo "Creating bucket: $MINIO_BUCKET_NAME"

# Check if the bucket already exists before trying to create it
# This prevents errors if you run docker compose up multiple times
if mc ls "local/$MINIO_BUCKET_NAME"; then
  echo "Bucket '$MINIO_BUCKET_NAME' already exists."
else
  if mc mb "local/$MINIO_BUCKET_NAME"; then
    echo "Bucket '$MINIO_BUCKET_NAME' created successfully."
  else
    echo "Failed to create bucket '$MINIO_BUCKET_NAME'."
    exit 1 # Exit with error if bucket creation fails
  fi
fi

echo "Setting public access policy for bucket: $MINIO_BUCKET_NAME"
if mc anonymous set public "local/$MINIO_BUCKET_NAME"; then
  echo "Public access policy set for bucket '$MINIO_BUCKET_NAME'."
else
  echo "Failed to set public access policy for bucket '$MINIO_BUCKET_NAME'."
  exit 1
fi

echo "MinIO setup script finished."
