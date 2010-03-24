


def test_compile():
    try:
        import tiddlywebplugins.sqlalchemy
        assert True
    except ImportError, exc:
        assert False, exc
