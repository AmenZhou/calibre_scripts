#!/bin/bash
# Change admin password (user already exists)

set -e

echo "Changing admin password to mypassword123..."

sudo docker exec -i mybookshelf2_app python3 manage.py change_password admin -p mypassword123

echo ""
echo "=========================================="
echo "✅ MyBookshelf2 is READY!"
echo "=========================================="
echo ""
echo "🌐 Access at: http://localhost:5000"
echo "👤 Login: admin / mypassword123"
echo ""
echo "📊 Check status: sudo docker ps"
echo "=========================================="

