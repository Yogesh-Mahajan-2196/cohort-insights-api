from pymongo import AsyncMongoClient
from fastapi import Request
from core.config import settings

client = AsyncMongoClient(settings.MONGO_URI)

database = client[settings.MONGO_DATABASE]

document_collection = database["documents"]

def get_database(request: Request):
    return request.app.state.db

def get_mongo_client(request: Request):
    return request.app.state.mongo_client

