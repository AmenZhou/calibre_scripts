#!/bin/bash
# Build and start MyBookshelf2 correctly

set -e

echo "=========================================="
echo "Building and Starting MyBookshelf2"
echo "=========================================="

cd /home/haimengzhou/calibre_automation_scripts

# Build base image
echo "Building base image (this may take several minutes)..."
cd mybookshelf2/deploy
sudo docker build -t mbs2-ubuntu -f Dockerfile .

# Build backend image
echo "Building backend image..."
sudo docker build -t mybookshelf2-backend -f Dockerfile-backend .

# Build app image  
echo "Building app image..."
sudo docker build -t mybookshelf2-app -f Dockerfile-app --build-arg MBS2_ENVIRONMENT=production .

cd ../..

# Stop old containers
sudo docker stop mybookshelf2_backend mybookshelf2_app 2>/dev/null || true
sudo docker rm mybookshelf2_backend mybookshelf2_app 2>/dev/null || true

# Start backend
echo "Starting backend..."
sudo docker run -d \
  --name mybookshelf2_backend \
  --link mybookshelf2_db:db \
  -v /home/haimengzhou/calibre_automation_scripts/mybookshelf2:/code \
  -v mybookshelf2_data:/data \
  -e MBS2_DB_HOST=mybookshelf2_db \
  -e MBS2_DB_NAME=ebooks \
  -e MBS2_DB_USER=ebooks \
  -e MBS2_DB_PASSWORD=ebooks_password \
  -e MBS2_DATA_DIR=/data \
  -p 9080:9080 \
  mybookshelf2-backend /loop.sh python3 engine/backend.py --delegated-addr 0.0.0.0

sleep 5

# Start web app
echo "Starting web app..."
sudo docker run -d \
  --name mybookshelf2_app \
  --link mybookshelf2_db:db \
  --link mybookshelf2_backend:backend \
  -v /home/haimengzhou/calibre_automation_scripts/mybookshelf2:/code \
  -v mybookshelf2_data:/data \
  -e MBS2_DB_HOST=mybookshelf2_db \
  -e MBS2_DB_NAME=ebooks \
  -e MBS2_DB_USER=ebooks \
  -e MBS2_DB_PASSWORD=ebooks_password \
  -e MBS2_DELEGATED_HOST=mybookshelf2_backend \
  -e MBS2_DATA_DIR=/data \
  -e MBS2_ENVIRONMENT=production \
  -e MBS2_DEBUG=false \
  -p 5000:6006 \
  mybookshelf2-app python3 server.py VISIBLE

echo "Waiting for app to start..."
sleep 10

echo "Initializing database..."
sudo docker exec mybookshelf2_app python3 manage.py create_tables -a -c

echo "Creating admin user..."
sudo docker exec mybookshelf2_app python3 manage.py create_user admin admin@example.com
sudo docker exec mybookshelf2_app python3 manage.py change_password admin -p mypassword123

echo ""
echo "=========================================="
echo "✅ MyBookshelf2 is running!"
echo "=========================================="
echo "Access at: http://localhost:5000"
echo "Login: admin / mypassword123"
echo "=========================================="


