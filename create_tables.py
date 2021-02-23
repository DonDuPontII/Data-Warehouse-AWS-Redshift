import configparser
import psycopg2
from sql_queries import create_table_queries, table_list


def drop_tables(cur, conn):
    """
    Drops each table if they exist for each table in \
    `table_list` from sql_queries.py
    
    INPUTS:
    * cur - the cursor available
    * conn - database connection
    """
    for table in table_list:
        cur.execute("DROP TABLE IF EXISTS " + table)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table if they don't already exist by executing the \
    queries in `create_table_queries` list from sql_queries.py
    
    INPUTS:
    * cur - the cursor available
    * conn - database connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Read in parameters needed for Redshift cluster
    Connect to Redshift cluster and gets cursor to it
    Drops all tables by calling the drop_tables function
    Creates all tables by calling the create_tables function
    Finally, closes the connection
    """
    
    # Read in parameters needed for Redshift cluster
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to Redshift cluster and gets cursor to it
    conn = psycopg2.connect("""host={} 
                               dbname={} 
                               user={} 
                               password={} 
                               port={}""".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drops all tables by calling the drop_tables function
    drop_tables(cur, conn)
    
    # Creates all tables by calling the create_tables function
    create_tables(cur, conn)

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()