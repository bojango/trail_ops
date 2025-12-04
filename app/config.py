import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_healthfit_dir() -> str:
    """Return the absolute path to the HealthFit export directory."""
    path = os.getenv("HEALTHFIT_DIR")
    if not path:
        raise ValueError("HEALTHFIT_DIR is not set in the .env file.")
    return path

def get_db_path() -> str:
    """Return the absolute path to the SQLite database."""
    path = os.getenv("DB_PATH")
    if not path:
        raise ValueError("DB_PATH is not set in the .env file.")
    return path