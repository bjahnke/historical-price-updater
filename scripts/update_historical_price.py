"""
Entry point for updating historical price data.
"""
import os
import sys
import argparse

# Get the directory of the script and add the project root to the Python path
script_directory = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_directory, '..'))
sys.path.append(project_root)

import src.historical_price_updater.price_updater as hpu

# Create an argument parser
parser = argparse.ArgumentParser(description='Update historical price data.')

# Add an optional argument to exclude current prices
parser.add_argument('--exclude-current', action='store_true', help='Exclude current prices')

# Parse the command line arguments
args = parser.parse_args()

# Pass the exclude_current argument to the main function
hpu.main(exclude_current_data=args.exclude_current)

if __name__ == '__main__':
    hpu.main()
