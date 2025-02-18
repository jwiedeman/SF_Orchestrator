# SF Orchestrator

## Overview
This project is a Python wrapper for the Screaming Frog SEO Spider CLI. It allows you to schedule scans based on a configuration file and manage the scans using PM2.

## Project Structure
```
SF_Orchestrator/
│
├── config/                # Folder for configuration files
│
├── scans/                 # Folder for scan targets (text files)
│
├── output/                # Folder for output files
│
├── main.py                # Main script for the wrapper
│
├── requirements.txt       # Dependencies
│
└── schedule_config.txt    # Configuration for scheduling scans
```

## Requirements
- Python 3.x
- PM2
- Screaming Frog SEO Spider CLI (must be installed separately)
- Python 3.x
- PM2

## Installation
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd SF_Orchestrator
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install PM2 globally (if not already installed):
   ```bash
   npm install -g pm2
   ```

## Configuration
1. Edit the `schedule_config.txt` file to specify the URLs, frequency, and time for scans. The format is:
   ```
   URL, Frequency (e.g., 'weekly', 'daily'), Time (e.g., '01:00')
   ```
   Example:
   ```
   https://www.example.com, weekly, 01:00
   ```

## Running the Script
1. Start the script with PM2:
   ```bash
   pm2 start /workspace/SF_Orchestrator/main.py --interpreter python3 --name SF_Orchestrator
   ```

2. Manage the PM2 process:
   - List all processes:
     ```bash
     pm2 list
     ```
   - Stop the process:
     ```bash
     pm2 stop SF_Orchestrator
     ```
   - Restart the process:
     ```bash
     pm2 restart SF_Orchestrator
     ```
   - View logs:
     ```bash
     pm2 logs SF_Orchestrator
     ```

3. Set PM2 to start on boot:
   ```bash
   pm2 startup
   ```

## Notes
- Ensure that the Screaming Frog SEO Spider CLI is installed and accessible in your system's PATH.
- Modify the SQL generation logic in `main.py` as needed to match your database schema.

## License
This project is licensed under the MIT License.