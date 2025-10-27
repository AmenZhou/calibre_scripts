# MyBookshelf2 Installation Complete! 🎉

## Access Information

- **URL:** http://localhost:5000
- **Username:** admin
- **Password:** mypassword123 (or "admin" if password change didn't work)

## What's Running

1. **Database:** PostgreSQL 14 on port 5433 (host) -> 5432 (container)
2. **Backend:** Running on port 9080
3. **Web App:** Running on port 5000

## Useful Commands

### Check Status
```bash
sudo docker ps
```

### View Logs
```bash
sudo docker logs -f mybookshelf2_app      # Web app logs
sudo docker logs -f mybookshelf2_backend # Backend logs
sudo docker logs -f mybookshelf2_db       # Database logs
```

### Restart Services
```bash
sudo docker restart mybookshelf2_app mybookshelf2_backend mybookshelf2_db
```

### Stop Services
```bash
sudo docker stop mybookshelf2_app mybookshelf2_backend mybookshelf2_db
```

### Start Services Again
```bash
cd /home/haimengzhou/calibre_automation_scripts
./build_and_start.sh
```

## Upload Your First Book

1. Open http://localhost:5000 in your browser
2. Login with admin / mypassword123
3. Upload an ebook (supports: epub, mobi, fb2, pdf, and more)

Or use the CLI:
```bash
sudo docker exec mybookshelf2_app python3 cli/mbs2.py upload_ebook /path/to/book.epub
```

## Data Location

- **Database:** Stored in Docker volume `mybookshelf2_db`
- **Books:** Stored in Docker volume `mybookshelf2_data`
- **Container code:** Mounted from /home/haimengzhou/calibre_automation_scripts/mybookshelf2

## Troubleshooting

### Can't access the web interface?
```bash
# Check if containers are running
sudo docker ps

# Check logs for errors
sudo docker logs mybookshelf2_app
```

### Port 5000 already in use?
Change the port in build_and_start.sh from `-p 5000:6006` to `-p 8080:6006`

### Need to reset password?
```bash
sudo docker exec -i mybookshelf2_app python3 manage.py change_password admin -p newpassword123
```

## Next Steps

1. Access the web interface at http://localhost:5000
2. Upload your ebooks
3. Organize them in bookshelves
4. Enjoy reading!

## Installation Summary

✅ MyBookshelf2 repository cloned
✅ Docker installed
✅ Database container running
✅ Backend container running  
✅ Web app container running
✅ Database initialized
✅ Admin user created

**MyBookshelf2 is ready to use!**

