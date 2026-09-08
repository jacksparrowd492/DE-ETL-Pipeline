import os
from dotenv import load_dotenv
from databricks import sql

load_dotenv()


def get_connection():
    connection = sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    )
    return connection


if __name__ == "__main__":
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT current_timestamp()")
        print(cursor.fetchall())

        cursor.close()
        conn.close()

        print("✅ Connected Successfully!")

    except Exception as e:
        print("❌ Connection Failed")
        print(e)
