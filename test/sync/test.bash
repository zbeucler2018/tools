#!/usr/bin/bash

set -e

docker compose down
docker compose up -d

sleep 10

echo "REPLICA TABLES..."
docker exec -it replica mysql testdb -u admin -padmin -e 'SHOW TABLES;'
echo "MASTER TABLES..."
docker exec -it master mysql testdb -u admin -padmin -e 'SHOW TABLES;'

echo "REPLICA DATA..."
docker exec -it replica mysql testdb -u admin -padmin -e 'INSERT INTO table1 (timestamp, col1) VALUES (1, 0), (2, 0);'
docker exec -it replica mysql testdb -u admin -padmin -e 'SELECT * FROM table1;'

echo "MASTER DATA..."
docker exec -it master mysql testdb -u admin -padmin -e 'INSERT INTO table1 (timestamp, col1) VALUES (1, 0), (2, 0), (3, 0), (4, 0);'
docker exec -it master mysql testdb -u admin -padmin -e 'SELECT * FROM table1;'

python3.10 ./../../tools/sync.py -mh '0.0.0.0' -mu admin -mpw admin -mdb testdb -mp 3306 -rh '0.0.0.0' -ru admin -rpw admin -rdb testdb -rp 3307 
echo "REPLICA DATA..."
docker exec -it replica mysql testdb -u admin -padmin -e 'SELECT * FROM table1;'

docker compose down
