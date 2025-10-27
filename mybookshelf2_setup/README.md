# MyBookshelf2 Setup

This folder contains all MyBookshelf2 installation files.

## Contents

- `mybookshelf2/` - The MyBookshelf2 source code repository
- `build_and_start.sh` - Script to build and start MyBookshelf2
- `change_admin_password.sh` - Script to change admin password
- `build_spa_client.sh` - Script to build the SPA client
- `docker-compose.yml` - Docker Compose configuration
- `INSTALLATION_COMPLETE.md` - Installation documentation

## Quick Start

### Start MyBookshelf2
```bash
cd mybookshelf2_setup
./build_and_start.sh
```

### Access MyBookshelf2
- URL: http://localhost:5000
- Username: admin
- Password: mypassword123

### Useful Commands

**Check Status:**
```bash
sudo docker ps
```

**View Logs:**
```bash
sudo docker logs -f mybookshelf2_app
```

**Stop Services:**
```bash
sudo docker stop mybookshelf2_app mybookshelf2_backend mybookshelf2_db
```

**Restart Services:**
```bash
cd mybookshelf2_setup
./build_and_start.sh
```

**Change Admin Password:**
```bash
cd mybookshelf2_setup
./change_admin_password.sh
```

**Build SPA Client:**
```bash
cd mybookshelf2_setup
./build_spa_client.sh
```

## Documentation

See `INSTALLATION_COMPLETE.md` for detailed documentation.

