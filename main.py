import subprocess
import shutil
import logging
import time
from pathlib import Path
import argparse

# Constants
SF_CLI = shutil.which("ScreamingFrogSEOSpiderCli.exe")  # Detect SF CLI path
OUTPUT_DIR = Path("output")
CONFIG_FILE = Path("SEOspiderMax.seospiderconfig")  # Ensure config file exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Ensure config file exists
if not CONFIG_FILE.exists():
    raise FileNotFoundError(f"Config file '{CONFIG_FILE}' not found in project root.")

# Full lists of available export options
EXPORT_TABS = [
    "AI:All", "AMP:All", "Accessibility:All", "Analytics:All",
    "Canonicals:All", "Change Detection:All", "Content:All",
    "Custom Extraction:All", "Custom JavaScript:All",
    "Custom Search:All", "Directives:All", "External:All",
    "H1:All", "H2:All", "Hreflang:All", "Images:All",
    "Internal:All", "JavaScript:All", "Link Metrics:All",
    "Links:All", "Meta Description:All", "Meta Keywords:All",
    "Mobile:All", "Page Titles:All", "PageSpeed:All",
    "Pagination:All", "Response Codes:All",
    "Search Console:All", "Security:All", "Sitemaps:All",
    "Structured Data:All", "URL:All", "Validation:All"
]

BULK_EXPORTS = [
    "Links:All Inlinks",
    "Links:All Outlinks",
    "Web:All Page Source",
    "Web:All Page Text",
    "Web:All PDF Documents",
    "Web:All PDF Content",
    "Web:All HTTP Request Headers",
    "Web:All HTTP Response Headers",
    "Web:All Cookies",
    "Content:Exact Duplicates",
    "Content:Near Duplicates",
    "Images:All Image Inlinks",
    "Custom Search:All Inlinks",
    "Custom Extraction:All Inlinks",
    "Accessibility:All Violations",
    "Accessibility:All Incomplete",
    "Issues:All"
]

SAVE_REPORTS = [
    "Crawl Overview",
    "Issues Overview",
    "Segments Overview",
    "Redirects:All Redirects",
    "Canonicals:Canonical Chains",
    "Canonicals:Non-Indexable Canonicals",
    "Structured Data:Validation Errors & Warnings Summary",
    "Structured Data:Validation Errors & Warnings",
    "Structured Data:Google Rich Results Features Summary",
    "Structured Data:Google Rich Results Features",
    "Javascript:Javascript Console Log Summary",
    "PageSpeed:PageSpeed Opportunities Summary",
    "Accessibility:Accessibility Violations Summary",
    "HTTP Headers:HTTP Header Summary",
    "Cookies:Cookie Summary"
]

def run_screaming_frog(target_url):
    """Runs Screaming Frog with max exports using a custom configuration."""
    if not SF_CLI:
        raise FileNotFoundError("Screaming Frog CLI not found. Ensure it's installed and in PATH.")

    cmd = [
        SF_CLI,
        "--config", str(CONFIG_FILE),  # Load your custom config
        "--crawl", target_url,
        "--headless",
        "--save-crawl",
        "--timestamped-output",
        "--output-folder", str(OUTPUT_DIR),
        "--export-tabs", ",".join(EXPORT_TABS),
        "--bulk-export", ",".join(BULK_EXPORTS),
        "--save-report", ",".join(SAVE_REPORTS),
        "--export-format", "xlsx"
    ]

    cmd_str = " ".join(cmd)
    logging.info(f"Executing Screaming Frog Command:\n{cmd_str}")

    try:
        start_time = time.time()

        # Run the command while capturing real-time output
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        # Log the output in real-time
        with process.stdout:
            for line in iter(process.stdout.readline, ""):
                logging.info(line.strip())

        # Wait for the process to finish and capture return code
        process.wait()
        elapsed_time = time.time() - start_time

        if process.returncode == 0:
            logging.info(f"Crawl completed successfully in {elapsed_time:.2f} seconds.")
            logging.info(f"Output saved to: {OUTPUT_DIR}")
        else:
            logging.error(f"Screaming Frog failed with return code {process.returncode}")

    except subprocess.SubprocessError as e:
        logging.error(f"Error running Screaming Frog: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Screaming Frog and export all data using a custom config.")
    parser.add_argument("--url", required=True, help="Target URL to scan.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    run_screaming_frog(args.url)
