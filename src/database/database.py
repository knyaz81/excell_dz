from psycopg2 import connect, extensions, DatabaseError
import asyncpg

from src.core import settings


class PostgresEngine:
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
            CREATE TABLE {0} (
                {1} INTEGER UNIQUE PRIMARY KEY,
                {2} VARCHAR(255) NOT NULL
            )
            """.format(
                settings.TABLENAME_CATEGORIES,
                *settings.CATEGORY_FIELDS
            ),
            """
            CREATE TABLE {0} (
                {1} INTEGER UNIQUE PRIMARY KEY,
                {2} VARCHAR(255) UNIQUE
            )
            """.format(
                settings.TABLENAME_BRANDS,
                *settings.BRANDS_FIELDS
            ),
            """
            CREATE TABLE {0} (
                {1} INTEGER NOT NULL,
                {2} INTEGER NOT NULL,
                FOREIGN KEY ({1})
                    REFERENCES categories ({1})
                    ON DELETE CASCADE,
                FOREIGN KEY ({2})
                    REFERENCES brands ({2})
                    ON DELETE CASCADE,
                {3} CHAR(10) PRIMARY KEY,
                {4} VARCHAR(255) NOT NULL,
                {5} MONEY
            )
            """.format(
                settings.TABLENAME_PRODUCTS,
                *settings.PRODUCT_FIELDS
            ),
            """
            CREATE TABLE {0} (
                {1} INTEGER UNIQUE PRIMARY KEY,
                {2} VARCHAR(255) UNIQUE
            )
            """.format(
                settings.TABLENAME_ATTRIBUTES,
                *settings.ATTRIBUTE_FIELDS
            ),
            """
            CREATE TABLE {0} (
                {1} CHAR(10) NOT NULL,
                {2} INTEGER NOT NULL,
                PRIMARY KEY ({1}, {2}),
                FOREIGN KEY ({1})
                    REFERENCES products ({1})
                    ON DELETE CASCADE,
                FOREIGN KEY ({2})
                    REFERENCES attributes ({2})
                    ON DELETE CASCADE,
                {3} VARCHAR(255)
            )
            """.format(
                settings.TABLENAME_PRODUCT_ATTRIBUTES,
                *settings.PRODUCT_ATTRIBUTE_FIELDS
            ),
        )


class DataBase:
    def __init__(self):
        self.engine = PostgresEngine(settings.DB_CONFIG)
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
            for query in self.engine.get_create_tables_queries():
                cursor.execute(query)
            print("CREATE TABLES successfully...")

    def bulk_create(self, tablename, fields, values_list):
        sql_query = (
        f'INSERT INTO {tablename} ({", ".join(fields)}) '
        f'VALUES ({", ".join(["%s" for _ in range(len(fields))])})'
        )
        with self as cursor:
            cursor.executemany(sql_query, values_list)
 

class AsyncDataBase:

    def __init__(self):
        self.connect_params = {
            'user': settings.DB_CONFIG['USER'],
            'password': settings.DB_CONFIG['PASSWORD'],
            'database': settings.DB_CONFIG['NAME'],
            'host': settings.DB_CONFIG['HOST'],
            'port': settings.DB_CONFIG['PORT'],
        }
        self.connection = None

    async def __aenter__(self):
        if self.connection is None:
            self.connection = await asyncpg.connect(**self.connect_params)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if  self.connection is not None:
            await self.connection.close()
            self.connection = None

    async def create(self, tablename, fields, values, return_value=None):
        sql_query = (
        f'INSERT INTO {tablename} ({", ".join(fields)}) '
        f'VALUES ({", ".join(self.get_values_placeholder(fields))}) '
        )
        if return_value:
            sql_query += f'RETURNING {return_value}'
        return await self.connection.fetchval(sql_query, *values)

    async def bulk_create(self, tablename, fields, values_list):
        sql_query = (
            f'INSERT INTO {tablename} ({", ".join(fields)})'
            f'VALUES ({", ".join(self.get_values_placeholder(fields))})'
        )
        await self.connection.executemany(sql_query, values_list)

    def get_values_placeholder(self, values, first=1):
        return (f'${i}' for i in range(first, len(values)+first))
