import os
import subprocess
import argparse
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from pathlib import Path
import sys
import traceback
import shutil
# Constants
DEFAULT_CONFIG_DIR = Path(os.path.expanduser("~")) / ".screaming-frog"
OUTPUT_DIR = Path("output")
LOG_DIR = Path("logs")

def setup_logging():
    """Setup logging configuration"""
    LOG_DIR.mkdir(exist_ok=True)
    log_file = LOG_DIR / f"sf_wrapper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return log_file

def get_column_type(column_name, sample_value):
    """Determine SQLite column type based on column name and sample value"""
    # Common numeric suffixes that indicate the column should be numeric
    numeric_indicators = ['_ms', '_bytes', '_length', '_size', '_count', '_number', '_code']
    timestamp_indicators = ['_date', '_time', 'timestamp', 'last_modified']
    
    # Check if column name indicates it should be numeric
    if any(column_name.lower().endswith(ind) for ind in numeric_indicators):
        return 'REAL'
    
    # Check if column name indicates it should be timestamp
    if any(ind in column_name.lower() for ind in timestamp_indicators):
        return 'TIMESTAMP'
    
    # Try to infer type from sample value
    if pd.isna(sample_value):
        return 'TEXT'  # Default to TEXT for NULL values
    elif isinstance(sample_value, (int, float)):
        return 'REAL'
    elif isinstance(sample_value, (datetime, pd.Timestamp)):
        return 'TIMESTAMP'
    else:
        return 'TEXT'

class DatabaseManager:
    def __init__(self, db_path='screaming_frog_results.db'):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.table_schemas = {}
        self.setup_database()
    def column_exists(cursor, table, column):
        cursor.execute(f"PRAGMA table_info({table})")
        return any(row[1] == column for row in cursor.fetchall())


    def setup_database(self):
        """Initialize SQLite database with necessary tables"""
        logging.info("Setting up database connection...")
        try:
            self.conn = sqlite3.connect('screaming_frog_results.db', detect_types=sqlite3.PARSE_DECLTYPES)
            self.cursor = self.conn.cursor()

            # Create tables with detailed logging
            self.cursor.executescript('''
                CREATE TABLE IF NOT EXISTS crawl_metadata (
                    id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    crawl_date TIMESTAMP,
                    config_used TEXT,
                    crawl_status TEXT,
                    total_urls INTEGER,
                    completion_time INTEGER,
                    error_message TEXT
                );
            ''')

            # Ensure 'command_executed' column exists
            if not column_exists(self.cursor, "crawl_metadata", "command_executed"):
                logging.info("Adding missing column: command_executed")
                self.cursor.execute("ALTER TABLE crawl_metadata ADD COLUMN command_executed TEXT")

            self.conn.commit()
            logging.info("Database setup completed successfully")
        except sqlite3.Error as e:
            logging.error(f"Database setup failed: {e}")
            raise

    def _create_base_tables(self):
        """Create essential database tables"""
        self.cursor.executescript('''
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS table_schemas (
                table_name TEXT PRIMARY KEY,
                columns JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS maintenance_log (
                id INTEGER PRIMARY KEY,
                operation TEXT,
                details TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status TEXT,
                error_message TEXT
            );
        ''')
        self.conn.commit()

    def _load_existing_schemas(self):
        """Load existing table schemas from database"""
        self.cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
        for table_name, sql in self.cursor.fetchall():
            if sql:  # Some system tables might not have SQL
                self.table_schemas[table_name] = sql

    def ensure_table_for_dataframe(self, table_name, df):
        """Ensure table exists with correct schema for DataFrame"""
        logging.info(f"Ensuring table {table_name} matches DataFrame schema")
        
        # Get sample row for type inference
        sample_row = df.iloc[0] if not df.empty else pd.Series()
        
        # Build column definitions
        columns = []
        for col in df.columns:
            col_type = get_column_type(col, sample_row.get(col))
            col_name = col.replace(' ', '_').replace('-', '_').lower()
            columns.append(f"{col_name} {col_type}")
        
        # Add metadata columns if they don't exist
        metadata_columns = [
            "crawl_id INTEGER",
            "processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "source_file TEXT"
        ]
        columns.extend(metadata_columns)
        
        # Create or alter table
        current_columns = set()
        if table_name in self.table_schemas:
            # Get existing columns
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            current_columns = {row[1] for row in self.cursor.fetchall()}
            
            # Add missing columns
            for col_def in columns:
                col_name = col_def.split()[0]
                if col_name not in current_columns:
                    try:
                        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_def}"
                        logging.debug(f"Adding column: {alter_sql}")
                        self.cursor.execute(alter_sql)
                    except sqlite3.OperationalError as e:
                        logging.warning(f"Could not add column {col_name}: {e}")
        else:
            # Create new table
            create_sql = f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                {', '.join(columns)}
            )
            """
            logging.debug(f"Creating table: {create_sql}")
            self.cursor.execute(create_sql)
        
        self.conn.commit()

    def optimize_database(self):
        """Perform database optimization"""
        logging.info("Starting database optimization")
        try:
            self.cursor.execute("BEGIN")
            
            # Log start of maintenance
            self.cursor.execute('''
                INSERT INTO maintenance_log (operation, details, started_at, status)
                VALUES (?, ?, CURRENT_TIMESTAMP, ?)
            ''', ('optimize', 'Regular database optimization', 'in_progress'))
            log_id = self.cursor.lastrowid
            
            # Perform VACUUM
            self.cursor.execute("VACUUM")
            
            # Analyze tables
            self.cursor.execute("ANALYZE")
            
            # Optimize indexes
            self.cursor.execute("PRAGMA optimize")
            
            # Update maintenance log
            self.cursor.execute('''
                UPDATE maintenance_log 
                SET status = ?, completed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', ('completed', log_id))
            
            self.conn.commit()
            logging.info("Database optimization completed successfully")
            
        except Exception as e:
            self.conn.rollback()
            error_msg = f"Database optimization failed: {str(e)}"
            logging.error(error_msg)
            if log_id:
                self.cursor.execute('''
                    UPDATE maintenance_log 
                    SET status = ?, error_message = ?, completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', ('failed', error_msg, log_id))
                self.conn.commit()

    def cleanup_old_records(self, days_to_keep=30):
        """Clean up old crawl records"""
        logging.info(f"Starting cleanup of records older than {days_to_keep} days")
        try:
            self.cursor.execute("BEGIN")
            
            # Log start of cleanup
            self.cursor.execute('''
                INSERT INTO maintenance_log (operation, details, started_at, status)
                VALUES (?, ?, CURRENT_TIMESTAMP, ?)
            ''', ('cleanup', f'Removing records older than {days_to_keep} days', 'in_progress'))
            log_id = self.cursor.lastrowid
            
            # Delete old records from all crawl-related tables
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            tables_to_clean = ['crawl_results', 'crawl_metadata']
            
            for table in tables_to_clean:
                if table in self.table_schemas:
                    self.cursor.execute(f'''
                        DELETE FROM {table}
                        WHERE crawl_date < ?
                    ''', (cutoff_date,))
                    
                    rows_deleted = self.cursor.rowcount
                    logging.info(f"Deleted {rows_deleted} rows from {table}")
            
            # Update maintenance log
            self.cursor.execute('''
                UPDATE maintenance_log 
                SET status = ?, completed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', ('completed', log_id))
            
            self.conn.commit()
            logging.info("Cleanup completed successfully")
            
        except Exception as e:
            self.conn.rollback()
            error_msg = f"Cleanup failed: {str(e)}"
            logging.error(error_msg)
            if log_id:
                self.cursor.execute('''
                    UPDATE maintenance_log 
                    SET status = ?, error_message = ?, completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', ('failed', error_msg, log_id))
                self.conn.commit()

    def process_screaming_frog_csv(self, csv_file, crawl_id):
        """Process Screaming Frog CSV output and store in database"""
        logging.info(f"Processing Screaming Frog CSV: {csv_file}")
        try:
            # Read CSV file
            df = pd.read_csv(csv_file, low_memory=False)
            
            # Clean column names
            df.columns = [col.strip().replace(' ', '_').replace('-', '_').lower() for col in df.columns]
            
            # Add metadata
            df['crawl_id'] = crawl_id
            df['source_file'] = str(csv_file)
            df['processed_at'] = datetime.now()
            
            # Determine table name based on CSV content or filename
            table_name = self._determine_table_name(csv_file, df)
            
            # Ensure table exists with correct schema
            self.ensure_table_for_dataframe(table_name, df)
            
            # Insert data
            df.to_sql(table_name, self.conn, if_exists='append', index=False)
            
            logging.info(f"Successfully processed {len(df)} rows into {table_name}")
            return len(df)
            
        except Exception as e:
            error_msg = f"Error processing CSV file {csv_file}: {str(e)}"
            logging.error(error_msg)
            logging.error(traceback.format_exc())
            raise

    def _determine_table_name(self, csv_file, df):
        """Determine appropriate table name for the data"""
        # Extract filename without extension
        base_name = Path(csv_file).stem.lower()
        
        # Clean up the name
        clean_name = base_name.replace(' ', '_').replace('-', '_')
        
        # Add prefix if needed
        if not clean_name.startswith('sf_'):
            clean_name = f'sf_{clean_name}'
        
        return clean_name

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")

class ScreamingFrogWrapper:
    def __init__(self, config_dir=DEFAULT_CONFIG_DIR):
        self.config_dir = Path(config_dir)
        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Initialized ScreamingFrogWrapper with config_dir: {config_dir}")
        logging.info(f"Output directory set to: {self.output_dir}")
        self._verify_screaming_frog_installation()
        self.setup_database()

    def _verify_screaming_frog_installation(self):
        """Verify Screaming Frog is installed and accessible"""
        sf_path = shutil.which("ScreamingFrogSEOSpiderCli.exe")
        if not sf_path:
            error_msg = "Screaming Frog CLI not found in PATH"
            logging.error(error_msg)
            raise EnvironmentError(error_msg)
        logging.info(f"Found Screaming Frog CLI at: {sf_path}")

    def get_column_names(self):
        """Retrieve column names from the database dynamically."""
        try:
            conn = sqlite3.connect('screaming_frog_results.db')
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(crawl_results)")
            columns = [row[1] for row in cursor.fetchall()]
            conn.close()
            if not columns:
                logging.warning("No columns found in crawl_results table.")
            return columns
        except sqlite3.Error as e:
            logging.error(f"Error retrieving columns from database: {e}")
            return []
    def setup_database(self):
        """Initialize SQLite database with necessary tables"""
        logging.info("Setting up database connection...")
        try:
            self.conn = sqlite3.connect('screaming_frog_results.db', detect_types=sqlite3.PARSE_DECLTYPES)
            self.cursor = self.conn.cursor()
            
            # Create tables with detailed logging
            logging.debug("Creating database tables if they don't exist...")
            self.cursor.executescript('''
                CREATE TABLE IF NOT EXISTS crawl_metadata (
                    id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    crawl_date TIMESTAMP,
                    config_used TEXT,
                    crawl_status TEXT,
                    total_urls INTEGER,
                    completion_time INTEGER,
                    error_message TEXT,
                    command_executed TEXT
                );
                
                CREATE TABLE IF NOT EXISTS crawl_results (
                    id INTEGER PRIMARY KEY,
                    crawl_id INTEGER,
                    url TEXT,
                    status_code INTEGER,
                    title TEXT,
                    meta_description TEXT,
                    h1 TEXT,
                    content_type TEXT,
                    word_count INTEGER,
                    crawl_depth INTEGER,
                    load_time REAL,
                    FOREIGN KEY (crawl_id) REFERENCES crawl_metadata(id)
                );
            ''')
            self.conn.commit()
            logging.info("Database setup completed successfully")
        except sqlite3.Error as e:
            logging.error(f"Database setup failed: {e}")
            raise

    def build_crawl_command(self, url, config_file=None):
        """Builds and returns the Screaming Frog CLI command with dynamically retrieved columns."""
        logging.info(f"Building crawl command for URL: {url}")

        # Ensure Screaming Frog CLI exists before proceeding
        sf_cli_path = shutil.which("ScreamingFrogSEOSpiderCli.exe")
        if not sf_cli_path:
            logging.error("Screaming Frog CLI not found in PATH.")
            raise FileNotFoundError("Screaming Frog CLI not found. Ensure it's installed and in your PATH.")

        base_cmd = [sf_cli_path]

        # If a config file is provided and exists, use it
        if config_file:
            config_path = Path(config_file)
            if config_path.exists():
                logging.info(f"Using custom config file: {config_path}")
                cmd = base_cmd + [
                    '--config', str(config_path),
                    '--crawl', url,
                    '--output-folder', str(self.output_dir)
                ]
            else:
                logging.warning(f"Config file {config_file} not found. Falling back to default configuration.")
                config_file = None  # Ensures fallback to default settings

        # Get column names dynamically
        columns = self.get_column_names()
        if columns:
            export_tabs = ",".join([f"{col}:All" for col in columns])
        else:
            logging.warning("Falling back to default --export-tabs values.")
            export_tabs = "Internal:All,External:All,Response Codes:All"

        # Default crawl configuration if no valid config file
        if not config_file:
            logging.info("No valid config file found. Using default Screaming Frog settings.")
            cmd = base_cmd + [
                '--crawl', url,
                '--headless',
                '--save-crawl',
                '--create-sitemap',
                '--create-images-sitemap',
                '--use-pagespeed',
                '--timestamped-output',
                '--output-folder', str(self.output_dir),
                '--export-tabs', export_tabs,
                '--bulk-export', 'All Inlinks,All Outlinks,Response Codes:All'
            ]

        # Convert to a readable command string for debugging
        cmd_str = ' '.join(cmd)
        logging.debug(f"Generated Screaming Frog command: {cmd_str}")

        return cmd


    def process_crawl_results(self, crawl_id, output_files):
        """Process and store crawl results in database and Excel"""
        logging.info(f"Processing crawl results for crawl_id: {crawl_id}")
        logging.debug(f"Found {len(output_files)} output files to process")
        
        for file in output_files:
            if not file.endswith('.csv'):
                logging.debug(f"Skipping non-CSV file: {file}")
                continue
                
            try:
                logging.info(f"Processing file: {file}")
                df = pd.read_csv(file)
                logging.debug(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
                
                # Store in SQLite
                logging.info("Storing results in SQLite database...")
                df.to_sql(
                    'crawl_results',
                    self.conn,
                    if_exists='append',
                    index=False
                )
                
                # Export to Excel with multiple sheets
                excel_file = file.replace('.csv', '.xlsx')
                logging.info(f"Exporting to Excel: {excel_file}")
                
                with pd.ExcelWriter(excel_file) as writer:
                    df.to_excel(writer, sheet_name='Raw Data', index=False)
                    
                    # Add summary sheet
                    logging.debug("Creating summary sheet...")
                    summary = pd.DataFrame({
                        'Metric': ['Total URLs', 'Response Codes', 'Average Load Time'],
                        'Value': [
                            len(df),
                            df['Status Code'].value_counts().to_dict(),
                            df.get('Load Time', 0).mean()
                        ]
                    })
                    summary.to_excel(writer, sheet_name='Summary', index=False)
                
                logging.info(f"Successfully processed and exported results to {excel_file}")
                
            except Exception as e:
                logging.error(f"Error processing file {file}: {e}")
                logging.error(f"Traceback: {traceback.format_exc()}")

    def run_crawl(self, url, config_file=None, save_excel=True):
        """Execute a single crawl"""
        crawl_id = None
        start_time = time.time()
        logging.info(f"Starting crawl for URL: {url}")
        
        try:
            # Record crawl start in database
            logging.debug("Recording crawl start in database...")
            cmd = self.build_crawl_command(url, config_file)
            cmd_str = ' '.join(cmd)
            
            self.cursor.execute('''
                INSERT INTO crawl_metadata 
                (url, crawl_date, config_used, crawl_status, command_executed)
                VALUES (?, ?, ?, ?, ?)
            ''', (url, datetime.now(), config_file or 'default', 'started', cmd_str))
            self.conn.commit()
            crawl_id = self.cursor.lastrowid
            logging.info(f"Created crawl record with ID: {crawl_id}")
            
            # Execute crawl command
            logging.info("Executing Screaming Frog command...")
            logging.debug(f"Command: {cmd_str}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True  # This will raise CalledProcessError if return code != 0
            )
            
            logging.debug(f"Command output: {result.stdout}")
            
            # Process output files
            logging.info("Searching for output files...")
            output_files = list(self.output_dir.glob('**/*.csv'))
            logging.debug(f"Found {len(output_files)} CSV files")
            
            if not output_files:
                raise Exception("No output files found after crawl")
            
            self.process_crawl_results(crawl_id, output_files)
            
            # Update crawl status
            completion_time = int(time.time() - start_time)
            logging.info(f"Crawl completed in {completion_time} seconds")
            
            self.cursor.execute('''
                UPDATE crawl_metadata 
                SET crawl_status = ?, completion_time = ?
                WHERE id = ?
            ''', ('completed', completion_time, crawl_id))
            self.conn.commit()
            
            logging.info(f"Crawl completed successfully for {url}")
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Screaming Frog process failed with return code {e.returncode}\nStdout: {e.stdout}\nStderr: {e.stderr}"
            logging.error(error_msg)
            self._update_crawl_failure(crawl_id, error_msg, start_time)
            raise
            
        except Exception as e:
            error_msg = f"Error during crawl: {str(e)}\n{traceback.format_exc()}"
            logging.error(error_msg)
            self._update_crawl_failure(crawl_id, error_msg, start_time)
            raise

    def _update_crawl_failure(self, crawl_id, error_msg, start_time):
        """Update database with crawl failure information"""
        if crawl_id:
            try:
                completion_time = int(time.time() - start_time)
                self.cursor.execute('''
                    UPDATE crawl_metadata 
                    SET crawl_status = ?, completion_time = ?, error_message = ?
                    WHERE id = ?
                ''', ('failed', completion_time, error_msg, crawl_id))
                self.conn.commit()
                logging.debug(f"Updated crawl_id {crawl_id} with failure information")
            except sqlite3.Error as e:
                logging.error(f"Failed to update crawl failure in database: {e}")

def main():
    # Setup logging first
    log_file = setup_logging()
    logging.info("Starting Screaming Frog Wrapper")
    
    parser = argparse.ArgumentParser(description='Screaming Frog SEO Spider Wrapper')
    parser.add_argument('--url', help='Single URL to crawl')
    parser.add_argument('--config', help='Path to configuration file')
    parser.add_argument('--schedule', help='Path to schedule configuration file')
    parser.add_argument('--run-now', action='store_true', help='Run scheduled crawls immediately')
    
    args = parser.parse_args()
    logging.debug(f"Parsed arguments: {args}")

    try:
        wrapper = ScreamingFrogWrapper()
        
        if args.url:
            logging.info(f"Running single URL crawl for: {args.url}")
            wrapper.run_crawl(args.url, args.config)
        elif args.schedule:
            if args.run_now:
                logging.info("Running scheduled crawls immediately")
                with open(args.schedule, 'r') as f:
                    for line in f:
                        if line.strip() and not line.startswith('#'):
                            url = line.strip().split(',')[0].strip()
                            logging.info(f"Running scheduled crawl for: {url}")
                            wrapper.run_crawl(url, args.config)
            else:
                logging.info("Starting scheduled crawl mode")
                wrapper.run_scheduled_crawls(args.schedule)
        else:
            logging.warning("No action specified")
            parser.print_help()
    
    except Exception as e:
        logging.error(f"Critical error in main: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
    
    logging.info("Script completed successfully")
    logging.info(f"Full log available at: {log_file}")

if __name__ == '__main__':
    main()