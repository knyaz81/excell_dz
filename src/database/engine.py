from psycopg2 import connect, extensions, DatabaseError

from core.settings import DB_CONFIG


class PostgreSQL:
    def __init__(self, db_config):
        self.db_config = db_config

        self.db_name = self.db_config.get('NAME')
        self.user = self.owner = self.db_config.get('USER')
        self.password = self.db_config.get('PASSWORD')
        self.db_initname = self.db_config.get('INITDB_NAME')
        self.db_host = self.db_config.get('HOST')
        self.db_port = self.db_config.get('PORT')

    def get_connection(self, init_db=False):
        self.connect_params = {'database': self.db_name}
        if init_db:
            self.connect_params['database'] = self.db_initname
        self.connect_params['user'] = self.user
        self.connect_params['password'] = self.password
        self.connect_params['host'] = self.db_host
        self.connect_params['port'] = self.db_port
        connection = connect(**self.connect_params)
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return connection

    def get_create_db_query(self):
        create_db_query = (
            f'CREATE DATABASE "{self.db_name}"'
            f' WITH OWNER = "{self.owner}" '
            " ENCODING = 'UTF8';"
        )
        return create_db_query

    def get_drop_query(self):
        return f'DROP DATABASE IF EXISTS "{self.db_name}";'

    def get_create_tables_queries(self):
        return (
            """
            CREATE TABLE categories (
                category_id SERIAL PRIMARY KEY,
                category_name VARCHAR(255) NOT NULL
            )
            """,
            """
            CREATE TABLE brands (
                brand_id SERIAL PRIMARY KEY,
                brand_name VARCHAR(255) NOT NULL
            )
            """,
            """
            CREATE TABLE products (
                product_id SERIAL PRIMARY KEY,
                category_id INTEGER NOT NULL,
                brand_id INTEGER NOT NULL,
                FOREIGN KEY (category_id)
                    REFERENCES categories (category_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (brand_id)
                    REFERENCES brands (brand_id)
                    ON DELETE CASCADE,
                article VARCHAR(255) UNIQUE,
                name VARCHAR(255) NOT NULL,
                price MONEY
            )
            """,
            """
            CREATE TABLE attributes (
                attribute_id SERIAL PRIMARY KEY,
                attribute_name VARCHAR(255) UNIQUE
            )
            """,
            """
            CREATE TABLE product_attributes (
                product_id INTEGER NOT NULL,
                attribute_id INTEGER NOT NULL,
                PRIMARY KEY (product_id, attribute_id),
                FOREIGN KEY (product_id)
                    REFERENCES products (product_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (attribute_id)
                    REFERENCES attributes (attribute_id)
                    ON DELETE CASCADE,
                attribute_value VARCHAR(255)
            )
            """,
        )


class DataBase:
    def __init__(self):
        self.engine = PostgreSQL(DB_CONFIG)
        self.connetion = None

    def __enter__(self):
        self.connetion = self.engine.get_connection()
        return self.connetion.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connetion is not None:
            self.connetion.close()

    def init_db(self):
        self.connetion = self.engine.get_connection(init_db=True)
        drop_query = self.engine.get_drop_query()
        create_db_query = self.engine.get_create_db_query()
        cursor = self.connetion.cursor()
        try:
            print(f'Executing... "{drop_query}"')
            cursor.execute(drop_query)
            print(f'Executing... "{create_db_query}"')
            cursor.execute(create_db_query)
        except (Exception, DatabaseError)as error:
            print(f'Error: {error}')
        else:
            print("DONE!")
        finally:
            if self.connetion is not None:
                self.connetion.close()

    def create_tables(self):
        with self as cursor:
            try:
                for query in self.engine.get_create_tables_queries():
                    cursor.execute(query)
            except (Exception, DatabaseError)as error:
                print(f'Error: {error}')
            else:
                print("Create tablse successfully!")

    def insert_categories(self, categories_list):
        query = "INSERT INTO categories(category_name) VALUES (%s)"
        with self as cursor:
            try:
                cursor.executemany(query, categories_list)
            except (Exception, DatabaseError)as error:
                print(f'Error: {error}')
            else:
                print("Insert categories successfully!")


    




