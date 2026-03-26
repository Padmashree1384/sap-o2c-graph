# db.py
import os
from motor.motor_asyncio import AsyncIOMotorClient

# Load environment variables
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "sap_o2c")

# Create MongoDB client
client = AsyncIOMotorClient(MONGO_URI)

# Select database
db = client[DB_NAME]

# Optional: Quick test when running this file directly
if __name__ == "__main__":
    import asyncio

    async def test_connection():
        try:
            # List collections to check connection
            collections = await db.list_collection_names()
            print("Connected to DB:", DB_NAME)
            print("Collections:", collections)
        except Exception as e:
            print("Error connecting to DB:", e)

    asyncio.run(test_connection())