import pandas as pd
import os
from backend.config import CMAPSS_SCHEMA

def clean_cmapss_file(input_path, output_path):
    """
    Reads a raw CMAPSS file with variable whitespace and saves it as a standard CSV.
    """
    try:
        # Read with variable whitespace separator
        df = pd.read_csv(input_path, sep=r'\s+', header=None, names=CMAPSS_SCHEMA['columns'], engine='python')
        
        # Save as standard CSV
        df.to_csv(output_path, index=False, header=False)
        return True, f"Successfully cleaned {os.path.basename(input_path)}"
    except Exception as e:
        return False, str(e)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        clean_cmapss_file(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python data_cleaner.py <input_file> <output_file>")
