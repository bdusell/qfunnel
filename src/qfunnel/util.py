import os

def get_current_user():
    return os.environ['USER']

def get_env_vars(names):
    result = {}
    for name in names:
        value = os.environ.get(name)
        if value is not None:
            result[name] = value
    return result
