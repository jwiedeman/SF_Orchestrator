import sqlite3
import subprocess
import re

# Screaming Frog CLI executable
SF_CLI = "ScreamingFrogSEOSpiderCli.exe"
DB_FILE = "screaming_frog_results.db"
TABLE_NAME = "crawl_results"

# Help commands to scrape column names
HELP_COMMANDS = [
    "--help export-tabs",
    "--help bulk-export",
    "--help save-report",
    "--help export-custom-summary"
]

def create_table_if_not_exists():
    """Ensure the `crawl_results` table exists before adding columns."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL,
            crawl_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    print("✅ Ensured the database and table exist.")

def run_command(command):
    """Runs a Screaming Frog CLI command and returns the output."""
    try:
        result = subprocess.run(
            f"{SF_CLI} {command}",
            shell=True,
            text=True,
            capture_output=True
        )
        return result.stdout if result.returncode == 0 else result.stderr
    except Exception as e:
        return f"Error running {command}: {e}"

def extract_columns():
    """Runs CLI commands and extracts valid column names."""
    columns = set()

    for cmd in HELP_COMMANDS:
        print(f"🔍 Running: {cmd}")
        output = run_command(cmd)

        for line in output.split("\n"):
            line = line.strip()

            # Ignore log lines
            if not line or "User Locale" in line or "INFO" in line or "=====" in line:
                continue

            # Convert column names (replace special characters with underscores)
            cleaned_col = re.sub(r"[^A-Za-z0-9_]", "_", line).strip("_")

            # Ensure valid column names
            if cleaned_col and not re.match(r"^\d", cleaned_col):
                columns.add(cleaned_col)

    return sorted(columns)

def get_existing_columns(cursor):
    """Fetch existing columns in the database."""
    cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
    return {row[1] for row in cursor.fetchall()}  # Extract column names

def ensure_columns_exist():
    """Ensure all extracted columns exist in the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Ensure table exists
    create_table_if_not_exists()

    # Extract column names from CLI
    new_columns = extract_columns()
    existing_columns = get_existing_columns(cursor)

    # Add missing columns dynamically
    added_count = 0
    for column in new_columns:
        if column not in existing_columns:
            alter_sql = f"ALTER TABLE {TABLE_NAME} ADD COLUMN {column} TEXT"
            print(f"🛠️ Adding column: {column}")
            cursor.execute(alter_sql)
            added_count += 1

    conn.commit()
    conn.close()

    if added_count > 0:
        print(f"✅ Successfully added {added_count} new columns.")
    else:
        print("🔍 No new columns needed. DB is already up to date.")

# Run the function to ensure DB schema is updated
ensure_columns_exist()
