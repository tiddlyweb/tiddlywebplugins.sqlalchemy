


def test_compile():
    try:
        import tiddlywebplugins.sqlalchemy3
        assert True
    except ImportError, exc:
        assert False, exc
