from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import os
load_dotenv()

DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")

try:
    query = "CREATE TABLE ticker_describe (ticker VARCHAR (50), sector VARCHAR (50), name VARCHAR(70), describe TEXT)"
    with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
except Exception as e:
    print(e)            

try:
    query = "CREATE TABLE price_hour (open REAL NOT NULL, low REAL NOT NULL, high REAL NOT NULL, close REAL NOT NULL, volume INT NOT NULL, date TIMESTAMP NOT NULL, ticker VARCHAR(50) NOT NULL, PRIMARY KEY(ticker, date))"
    with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
except Exception as e:
    print(e)

try:
    query = "CREATE TABLE news (id SERIAL PRIMARY KEY, channel VARCHAR (50), date TIMESTAMP, text TEXT)"
    with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
except Exception as e:
    print(e)

try:
    query = "CREATE TABLE news_embedd (id INT PRIMARY KEY, embedding vector(768))"
    with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
except Exception as e:
    print(e)

try:
    query = "CREATE TABLE news_with_ticker (id SERIAL PRIMARY KEY, channel VARCHAR (50), ticker VARCHAR(50), date TIMESTAMP, text TEXT)"
    with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
except Exception as e:
    print(e)

try:
    query = "CREATE TABLE news_with_ticker_embedd (id SERIAL PRIMARY KEY, embedding vector(768))"
    with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
except Exception as e:
    print(e)            
    