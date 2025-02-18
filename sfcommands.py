import subprocess
import os

# Define the Screaming Frog CLI executable path (update if needed)
SF_CLI = "ScreamingFrogSEOSpiderCli.exe"

# List of help commands to scrape
commands = {
    "General Help": "--help",
    "Export Tabs": "--help export-tabs",
    "Bulk Export": "--help bulk-export",
    "Save Reports": "--help save-report",
    "Export Custom Summary": "--help export-custom-summary"
}

# Output file
output_file = "screaming_frog_help.txt"

# Function to run a command and capture output
def run_command(command):
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

# Collect outputs
output_data = []
for description, command in commands.items():
    print(f"Running: {command}")
    output = run_command(command)
    output_data.append(f"===== {description} =====\n{output}\n")

# Save to a text file
with open(output_file, "w", encoding="utf-8") as f:
    f.writelines(output_data)

print(f"Scraping completed. Output saved to {output_file}")
