from pydantic_settings import BaseSettings
from typing import List
from dotenv import load_dotenv

load_dotenv()


# settings.py

from pydantic_settings import BaseSettings
from typing import List, Optional

class Settings(BaseSettings):
    GOOGLE_APPLICATION_CREDENTIALS:str
    LOCATION: str
    PROJECT_ID: str

    class Config:
        env_file = ".env"  # Optional: if you're using a .env file
        env_file_encoding = "utf-8"

# Instantiate settings
settings = Settings()