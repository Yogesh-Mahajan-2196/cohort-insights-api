from fastapi import Request


def get_database(request: Request):
    return request.app.state.db


def get_mongo_client(request: Request):
    return request.app.state.mongo_client
