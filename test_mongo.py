
import pymongo
import sys

uri = "mongodb://localhost:27017/"
try:
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
    info = client.server_info()
    print("SUCCESS: Connected to MongoDB")
    print(info)
except Exception as e:
    print(f"FAILURE: Could not connect to MongoDB. Error: {e}")
    sys.exit(1)
