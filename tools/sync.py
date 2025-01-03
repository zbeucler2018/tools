import argparse

import mysql.connector
from mysql.connector import Error, MySQLConnection



def get_table_names(connection: MySQLConnection) -> list[str]:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            return cursor.fetchall()[0]
    except Error as e:
        print(f"Error getting table names: {e}")
        return


def connect_to_database(config) -> MySQLConnection:
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return


def sync_table(master_conn: MySQLConnection, replica_conn: MySQLConnection, table: str):
    try:
        # master has up-to-date data, replica is out-of-date
        with master_conn.cursor(dictionary=True) as master_cursor, replica_conn.cursor() as replica_cursor:
            # get most recent replica timestamp
            replica_cursor.execute(f"SELECT MAX(timestamp) FROM {table}")
            replica_latest = replica_cursor.fetchone()[0] or "0"

            # fetch missing data
            master_cursor.execute(f"SELECT * FROM {table} WHERE timestamp > %s", (replica_latest,))
            batch_size = 10_000
            while True:
                missing_data = master_cursor.fetchmany(batch_size)
                if not missing_data:
                    print("Done syncing")
                    break
                keys = missing_data[0].keys()
                columns = ", ".join(keys)
                placeholders = ", ".join(["%s"] * len(keys))
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                for row in missing_data:
                    data = tuple(row.values())
                    print("Executing: ", query, data)
                    replica_cursor.execute(query, data)
                replica_conn.commit()
    except Error as e:
        print(f"Error syncing table {table}: {e}")


def handle_args():
    parser = argparse.ArgumentParser(description="sync databases the hard way")
    parser.add_argument("-mh", "--master_host", type=str, help="IP address or hostname of the master db")
    parser.add_argument("-mu", "--master_user", type=str, help="SQL user to use on the master db")
    parser.add_argument("-mpw", "--master_pass", type=str, help="Password to the SQL user to use on the master db")
    parser.add_argument("-mdb", "--master_db", type=str, help="Name of the database to sync to")
    parser.add_argument("-mp", "--master_port", type=int, default=3306, help="Port to connect to the master db")
    
    parser.add_argument("-rh", "--replica_host", type=str, help="IP address or hostname of the replica db")
    parser.add_argument("-ru", "--replica_user", type=str, help="SQL user to use on the replica db")
    parser.add_argument("-rpw", "--replica_pass", type=str, help="Password to the SQL user to use on the replica db")
    parser.add_argument("-rdb", "--replica_db", type=str, help="Name of the database to sync")
    parser.add_argument("-rp", "--replica_port", type=int, default=3306, help="Port to connect to the replica db")
    return parser.parse_args()


def main():
    args = handle_args()

    master_db_config = {
        "host": args.master_host,
        "user": args.master_user,
        "password": args.master_pass,
        "database": args.master_db,
        "port": args.master_port,
        "compress": True
    }
    replica_db_config = {
        "host": args.replica_host,
        "user": args.replica_user,
        "password": args.replica_pass,
        "database": args.replica_db,
        "port": args.replica_port,
        "compress": True
    }

    # master_db_config = {
    #     'host': '0.0.0.0',
    #     'user': 'admin',
    #     'password': 'admin',
    #     'database': 'testdb',
    #     'compress': True,
    #     'port': 3306
    # }

    # replica_db_config = {
    #     'host': '0.0.0.0',
    #     'user': 'admin',
    #     'password': 'admin',
    #     'database': 'testdb',
    #     'compress': True,
    #     'port': 3307
    # }
    
    print("Master Connection Config")
    print(*master_db_config.items(), sep="\n")
    print("Replica Connection Config")
    print(*replica_db_config.items(), sep="\n")

    master_conn = connect_to_database(master_db_config)
    replica_conn = connect_to_database(replica_db_config)
    
    if master_conn is None or replica_conn is None:
        print(f"Missing connection. master={master_conn} replica={replica_conn}")
        master_conn.close()
        replica_conn.close()
        return
    
    try:
        tables_to_sync = get_table_names(master_conn)
        for table in tables_to_sync:
            sync_table(master_conn, replica_conn, table)

    finally:
        if master_conn.is_connected() and not master_conn is None:
            master_conn.close()
        if replica_conn.is_connected() and not replica_conn is None:
            replica_conn.close()

if __name__ == "__main__":
    main()
