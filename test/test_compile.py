


def test_compile():
    try:
        import tiddlywebplugins.sqlalchemy2
        assert True
    except ImportError, exc:
        assert False, exc
