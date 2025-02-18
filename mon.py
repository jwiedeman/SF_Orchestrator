import sqlite3
import time

DB_FILE = "screaming_frog_results.db"
TABLE_NAME = "crawl_results"

def monitor_db():
    """Monitor DB for new columns, total records, and column names with aligned output."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    last_column_count = 0
    last_record_count = 0

    while True:
        try:
            # Get total records
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            record_count = cursor.fetchone()[0]

            # Get column names
            cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
            columns = [row[1] for row in cursor.fetchall()]
            column_count = len(columns)

            # Print only if something changed
            if column_count != last_column_count or record_count != last_record_count:
                print("\n" + "=" * 60)
                print(f"📊 Total Records: {record_count}")
                print(f"🛠️  Columns ({column_count}):")
                print("-" * 60)

                
                print("=" * 60)

                last_column_count = column_count
                last_record_count = record_count

            # Refresh every 0.1 seconds
            time.sleep(0.1)

        except sqlite3.OperationalError as e:
            print(f"⚠️ Error: {e}. Ensure the table '{TABLE_NAME}' exists.")
            time.sleep(5)  # Wait and retry if the table isn't found

monitor_db()
