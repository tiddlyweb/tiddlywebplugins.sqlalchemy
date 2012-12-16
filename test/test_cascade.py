
import os

import py.test

from tiddlyweb.config import config
from tiddlyweb.store import Store, NoBagError, NoUserError, NoRecipeError, NoTiddlerError

from tiddlyweb.model.bag import Bag
from tiddlyweb.model.tiddler import Tiddler

from tiddlywebplugins.sqlalchemy3.model import (Base, sText, sTag, sTiddler,
        sRevision, sField)


def setup_module(module):
    module.store = Store(
            config['server_store'][0],
            config['server_store'][1],
            {'tiddlyweb.config': config}
            )
    Base.metadata.drop_all()
    Base.metadata.create_all()

def test_cascade():
    bag = Bag(u'holder')
    store.put(bag)
    tiddler = Tiddler(u'one', u'holder')
    tiddler.text = u'text'
    tiddler.tags = [u'tag']
    tiddler.fields = {u'fieldone': u'valueone'}
    store.put(tiddler)

    def count_em(count, message):
        text_count = store.storage.session.query(sText).count()
        tag_count = store.storage.session.query(sTag).count()
        tiddler_count = store.storage.session.query(sTiddler).count()
        revision_count = store.storage.session.query(sRevision).count()
        field_count = store.storage.session.query(sField).count()
        store.storage.session.commit()

        message = ('%s, but got: text: %s, tag: %s, tiddler: %s, '
            'revision: %s, field: %s') % (message, text_count, tag_count,
                    tiddler_count, revision_count, field_count)

        assert (text_count == tag_count == tiddler_count
                == revision_count == field_count == count), message

    count_em(1, '1 row for the tiddler everywhere')

    store.delete(tiddler)

    count_em(0, '0 rows for the tiddler everywhere')
