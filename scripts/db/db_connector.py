import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():

    connection = pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection