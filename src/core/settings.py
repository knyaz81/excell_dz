import os.path

## Program modes
ACTIONS = [
    'generate',
    'parse',
]

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

## Postgres
DB_CONFIG = {
    'USER': 'excellparser',
    'PASSWORD': 'excellparser',
    'NAME': 'excellparser',
    'INITDB_NAME': 'template1',
    'HOST': 'localhost',
    'PORT': 5433,
}
MAX_DB_CONNECTION = 10

## Database contstants
CATEGORY_FIELDS = ['category_id', 'category_name']
BRANDS_FIELDS = ['brand_id', 'brand_name']
ATTRIBUTE_FIELDS = ['attribute_id', 'attribute_name']
PRODUCT_FIELDS = ['category_id', 'brand_id', 'product_code', 'name', 'price']
PRODUCT_ATTRIBUTE_FIELDS = ['product_code', 'attribute_id', 'attribute_value']

TABLENAME_CATEGORIES = 'categories'
TABLENAME_ATTRIBUTES = 'attributes'
TABLENAME_BRANDS = 'brands'
TABLENAME_PRODUCTS = 'products'
TABLENAME_PRODUCT_ATTRIBUTES = 'product_attributes'
