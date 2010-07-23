import mangler
config = {
        'log_level': 'DEBUG',
        'twanager.tracebacks': True,
        'server_store': ['tiddlywebplugins.sqlalchemy', {
            'db_config': 'sqlite:///test.db'}],
        }
