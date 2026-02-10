
import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="InsightHubDB",
        user="postgres",
        password="Cky20040505"
    )
    return conn
