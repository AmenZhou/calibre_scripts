#!/bin/bash
# Build the SPA client for MyBookshelf2

set -e

echo "=========================================="
echo "Building MyBookshelf2 SPA Client"
echo "=========================================="

echo "This requires Node.js, npm, jspm, and gulp"
echo ""

cd /home/haimengzhou/calibre_automation_scripts/mybookshelf2/client

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "ERROR: Node.js is not installed"
    echo "Install it with: sudo apt-get install -y nodejs npm"
    exit 1
fi

# Check if gulp is installed
if ! command -v gulp &> /dev/null; then
    echo "Installing gulp globally..."
    sudo npm install -g gulp
fi

# Check if jspm is installed  
if ! command -v jspm &> /dev/null; then
    echo "Installing jspm globally..."
    sudo npm install -g jspm@0.16.53
fi

echo "Installing dependencies..."
npm install

echo "Installing jspm packages..."
jspm install -y

echo "Building SPA client..."
gulp export

echo ""
echo "Copying built files to deploy location..."
cp -av ./export /home/haimengzhou/calibre_automation_scripts/mybookshelf2/deploy/client/

echo ""
echo "=========================================="
echo "SPA Client built successfully!"
echo "=========================================="
echo ""
echo "The client is now in: mybookshelf2/deploy/client/"
echo ""
echo "Restart the app container to serve the SPA:"
echo "sudo docker restart mybookshelf2_app"
echo ""
echo "Then access at: http://localhost:5000/client/"
echo "=========================================="

