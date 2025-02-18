import re

# Input and output file paths
input_file = "screaming_frog_help.txt"
output_file = "screaming_frog_tabs.txt"

def extract_tabs(input_file, output_file):
    """
    Reads a Screaming Frog help file, extracts unique tab names, 
    and writes them to an output file.
    """
    tabs = set()

    # Read the file and extract tab names
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            match = re.match(r"([\w\s]+):", line.strip())  # Match tab before ":"
            if match:
                tabs.add(match.group(1).strip())

    # Sort tabs alphabetically and write to output file
    sorted_tabs = sorted(tabs)
    with open(output_file, "w", encoding="utf-8") as f:
        for tab in sorted_tabs:
            f.write(f"{tab}\n")

    print(f"Extracted {len(sorted_tabs)} unique tabs. Saved to {output_file}.")

# Run the function
extract_tabs(input_file, output_file)
