"""
Entry point for updating historical price data.
"""
import os
import sys

# Get the directory of the script and add the project root to the Python path
script_directory = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_directory, '..'))
sys.path.append(project_root)

import src.historical_price_updater.price_updater as hpu

if __name__ == '__main__':
    hpu.main()
