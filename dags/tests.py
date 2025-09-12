import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
DB_NAME = os.getenv("DATABASE_NAME")

print(DB_URL)
print(DB_NAME)