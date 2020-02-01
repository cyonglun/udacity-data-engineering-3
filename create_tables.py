import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """Drop Tables If Exists in Redshift Cluster"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """Create Tables in Redshift Cluster"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    print("Executing create_tables.py")
    
    """Set configurations"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    HOST = config['CLUSTER']['HOST']
    NAME = config['CLUSTER']['DB_NAME']
    USER = config['CLUSTER']['DB_USER']
    PASSWORD = config['CLUSTER']['DB_PASSWORD']
    PORT = config['CLUSTER']['DB_PORT']
    
    print("Configurations:\n host={}\n dbname={}\n user={}\n password={}\n port={}\n"
          .format(HOST, NAME, USER, PASSWORD, PORT))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                        .format(HOST, NAME, USER, PASSWORD, PORT))
    cur = conn.cursor()

    print("Drop Tables If Exists in Redshift Cluster")
    drop_tables(cur, conn)
    
    print("Create Tables in Redshift Cluster")
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()