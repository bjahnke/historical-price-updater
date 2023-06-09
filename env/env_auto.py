"""
Desc:
env_auto.py is generated from .env by the `invoke buildenvpy` task.
it's purpose is to provide IDE support for environment variables.
"""

import os
from dotenv import load_dotenv
load_dotenv()


NEON_DB_CONSTR = os.environ.get('NEON_DB_CONSTR')
DOCKER_TOKEN = os.environ.get('DOCKER_TOKEN')
DOCKER_USERNAME = os.environ.get('DOCKER_USERNAME')
GCR_PROJECT_ID = os.environ.get('GCR_PROJECT_ID')
