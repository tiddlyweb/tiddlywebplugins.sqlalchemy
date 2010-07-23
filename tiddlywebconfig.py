import mangler
config = {
        'log_level': 'DEBUG',
        'twanager.tracebacks': True,
        'twanager_plugins': ['tiddlywebplugins.migrate'],
        'server_store': ['tiddlywebplugins.sqlalchemy', {
            'db_config': 'sqlite:///migrate.db'}],
        }
