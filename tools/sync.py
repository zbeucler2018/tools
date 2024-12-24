import mysql.connector
from mysql.connector import Error, MySQLConnection


# Database configurations
MASTER_DB = {
    'host': '0.0.0.0',
    'user': 'admin',
    'password': 'admin',
    'database': 'testdb',
    'compress': True,
    'port': 3306
}

SLAVE_DB = {
    'host': '0.0.0.0',
    'user': 'admin',
    'password': 'admin',
    'database': 'testdb',
    'compress': True,
    'port': 3307
}

def connect_to_database(config) -> MySQLConnection:
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return

def sync_table(master_conn: MySQLConnection, slave_conn: MySQLConnection, table: str):
    try:
        # master has up-to-date data, slave is out-of-date
        with master_conn.cursor(dictionary=True) as master_cursor, slave_conn.cursor() as slave_cursor:
            # get most recent slave timestamp
            slave_cursor.execute(f"SELECT MAX(timestamp) FROM {table}")
            slave_latest = slave_cursor.fetchone()[0] or "0"

            # grab missing data from master
            master_cursor.execute(f"SELECT * FROM {table} WHERE timestamp > %s", (slave_latest,))
            missing_data = master_cursor.fetchall()

            if missing_data:
                keys = missing_data[0].keys()
                columns = ", ".join(keys)
                placeholders = ", ".join(["%s"] * len(keys))
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                # TODO: Make this iterative, s.t. it only fetches x records at a time
                for row in missing_data:
                    print("Executing: ", query, row.values())
                    slave_cursor.execute(query, tuple(row.values()))
                slave_conn.commit()
                print(f"Synced {len(missing_data)} rows for table: {table}")
            else:
                print(f"No new data to sync for table: {table}")
    except Error as e:
        print(f"Error syncing table {table}: {e}")


def main():
    master_conn = connect_to_database(MASTER_DB)
    slave_conn = connect_to_database(SLAVE_DB)
    
    if not master_conn or not slave_conn:
        print(f"Missing connection. master={master_conn} slave={slave_conn}")
        master_conn.close()
        slave_conn.close()
        return
    
    try:
        tables_to_sync = ['table1']
        for table in tables_to_sync:
            sync_table(master_conn, slave_conn, table)
    
    finally:
        if master_conn.is_connected():
            master_conn.close()
        if slave_conn.is_connected():
            slave_conn.close()

if __name__ == "__main__":
    main()
