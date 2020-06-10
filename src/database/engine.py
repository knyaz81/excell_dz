from psycopg2 import connect, extensions, DatabaseError
import asyncpg

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
                brand_name VARCHAR(255) UNIQUE
            )
            """,
            """
            CREATE TABLE products (
                category_id INTEGER NOT NULL,
                brand_id INTEGER NOT NULL,
                FOREIGN KEY (category_id)
                    REFERENCES categories (category_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (brand_id)
                    REFERENCES brands (brand_id)
                    ON DELETE CASCADE,
                product_code CHAR(10) PRIMARY KEY,
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
                product_code CHAR(10) NOT NULL,
                attribute_id INTEGER NOT NULL,
                PRIMARY KEY (product_code, attribute_id),
                FOREIGN KEY (product_code)
                    REFERENCES products (product_code)
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
                print("CREATE TABLES successfully...")

    def insert_category(self, category):
        query = """INSERT INTO categories(category_name)
                VALUES(%s) RETURNING category_id"""
        with self as cursor:
            cursor.execute(query, (category,))
            category_id = cursor.fetchone()[0]
            print(f'INSERT category "{category}" successfully...')
        return category_id

    def insert_product_attr(self, attr):
        query = """INSERT INTO attributes(attribute_name)
                VALUES(%s) RETURNING attribute_id"""
        with self as cursor:
            cursor.execute(query, (attr,))
            attr_id = cursor.fetchone()[0]
        return attr_id

    def insert_brand(self, brand):
        query = """INSERT INTO brands(brand_name)
                VALUES(%s) RETURNING brand_id"""
        with self as cursor:
            cursor.execute(query, (brand,))
            brand_id = cursor.fetchone()[0]
        return brand_id

    def pure_insert_product(self, cursor, product, adv_attrs):
        query_product = """INSERT INTO products(category_id, brand_id, product_code, name, price)
                        VALUES(%(category_id)s, %(brand_id)s, %(article)s, %(name)s, %(price)s)"""
        query_add_prod_attr = """INSERT INTO product_attributes(product_code, attribute_id, attribute_value)
                              VALUES(%(article)s, %(attribute_id)s, %(attribute_value)s)"""
        cursor.execute(query_product, product)
        for attr_id in adv_attrs:
            cursor.execute(
                query_add_prod_attr,
                {
                    'article': product['article'],
                    'attribute_id': attr_id,
                    'attribute_value': adv_attrs[attr_id],
                }
            )

    def pure_insert_bulk_products(self, cursor, products, adv_attrs):
        query_product = """INSERT INTO products(category_id, brand_id, product_code, name, price)
                        VALUES(%(category_id)s, %(brand_id)s, %(article)s, %(name)s, %(price)s)"""
        query_add_prod_attrs = """INSERT INTO product_attributes(product_code, attribute_id, attribute_value)
                              VALUES(%(article)s, %(attribute_id)s, %(attribute_value)s)"""
        cursor.executemany(query_product, products)
        cursor.executemany(query_add_prod_attrs, adv_attrs)
 

class AsyncPostgreSQL:

    def __init__(self):
        self.connect_params = {
            'user': DB_CONFIG['USER'],
            'password': DB_CONFIG['PASSWORD'],
            'database': DB_CONFIG['NAME'],
            'host': DB_CONFIG['HOST'],
            'port': DB_CONFIG['PORT'],
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
