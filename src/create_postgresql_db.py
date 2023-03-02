from config_parser import config_parser
import psycopg2


def get_db_connection(db_config_filepath, section):
    """
    Connecting and executing the database

    :param db_config_filepath: Path points to DB config file
    :param section: DB Section in config file
    :return: Database connection and cursor
    """
    db_config = config_parser(config_filepath=db_config_filepath, section=section)
    db_connection = psycopg2.connect(**db_config)
    db_cursor = db_connection.cursor()
    return db_connection, db_cursor


def create_database():
    """
    Create and connect to the stepstone db

    :return: The connection and cursor to stepstone database
    """
    # Connect to default DB
    conn, cur = get_db_connection(db_config_filepath='prj-config.cfg', section='postgresql_default')
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    db_name = config_parser('prj-config.cfg', 'postgresql_gsg')['dbname']

    # Create stepstone DB with UTF8 encoding
    cur.execute('DROP DATABASE IF EXISTS %s' % db_name)
    cur.execute("CREATE DATABASE %s WITH ENCODING 'utf8' TEMPLATE template0" % db_name)

    # Close connection to default DB
    conn.close()


def drop_tables():
    """
    Executing "DROP" all tables
    :return: None
    """
    # Connect to GSG DB
    conn, cur = get_db_connection(db_config_filepath='prj-config.cfg', section='postgresql_gsg')
    cur = conn.cursor()
    query = "DROP TABLE IF EXISTS bikes"
    cur.execute(query)
    conn.commit()


def create_tables():
    """
    Executing "CREATE" all tables
    :return: None
    """
    conn, cur = get_db_connection(db_config_filepath='prj-config.cfg', section='postgresql_gsg')
    query = """
    CREATE TABLE IF NOT EXISTS bikes
    (
        id TEXT PRIMARY KEY NOT NULL,
        registration_updated_at TIMESTAMP,
        api_url TEXT,
        stolen_location TEXT,
        manufacturer_name TEXT,
        frame_model TEXT,
        status TEXT,
        stolen TEXT,
        title TEXT,
        type_of_cycle TEXT
    )
    """
    cur.execute(query)
    conn.commit()


if __name__ == "__main__":
    create_database()
    drop_tables()
    create_tables()
