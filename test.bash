#!/usr/bin/bash

set -e

docker compose down
docker compose up -d

sleep 10

echo "SLAVE TABLES..."
docker exec -it slave mysql testdb -u admin -padmin -e 'SHOW TABLES;'
docker exec -it slave mysql testdb -u admin -padmin -e 'DESCRIBE table1;'
echo "MASTER TABLES..."
docker exec -it master mysql testdb -u admin -padmin -e 'SHOW TABLES;'
docker exec -it master mysql testdb -u admin -padmin -e 'DESCRIBE table1;'

echo "SLAVE DATA..."
docker exec -it slave mysql testdb -u admin -padmin -e 'INSERT INTO table1 (timestamp, col1) VALUES (1, 0), (2, 0);'
docker exec -it slave mysql testdb -u admin -padmin -e 'SELECT * FROM table1;'
echo "MASTER DATA..."
docker exec -it master mysql testdb -u admin -padmin -e 'INSERT INTO table1 (timestamp, col1) VALUES (1, 0), (2, 0), (3, 0), (4, 0);'
docker exec -it master mysql testdb -u admin -padmin -e 'SELECT * FROM table1;'

python3.10 tools/sync.py
docker exec -it slave mysql testdb -u admin -padmin -e 'SELECT * FROM table1;'
