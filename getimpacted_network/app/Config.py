import os


class Local:
    graph_db_uri = 'bolt://localhost:11008'
    graph_db_pwd = 'bcone@4321'


class Prod:
    graph_db_uri = 'bolt://172.17.8.70:7687'
    graph_db_pwd = 'i-097d2a6b8cebaad64'


def get_config(key: str):
    env = os.getenv('ENV', None)
    if env is None:
        print("Environment variable 'ENV' not set, returning local configs")
    if env == 'local':
        return Local.__dict__.get(key)
    elif env == 'prod':
        return Prod.__dict__.get(key)
    else:
        return Prod.__dict__.get(key)
