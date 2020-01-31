import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

print("Run 'copy_table_queries'")
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

print("Run 'insert_table_queries'")
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()