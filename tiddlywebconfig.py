import mangler
config = {
        'log_level': 'DEBUG',
        'twanager.tracebacks': True,
        'server_store': ['tiddlywebplugins.sqlalchemy2', {
            'db_config': 'sqlite:///test.db'}],
        }
