
import os

import py.test

from tiddlyweb.config import config
from tiddlyweb.store import Store, NoBagError, NoUserError, NoRecipeError, NoTiddlerError

from tiddlyweb.model.bag import Bag
from tiddlyweb.model.recipe import Recipe
from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.user import User

from base64 import b64encode

#RANGE = 1000
RANGE = 10

def setup_module(module):
    try:
        os.unlink('test.db')
    except OSError:
        pass # s'alright, it's not there
    module.store = Store(
            config['server_store'][0],
            config['server_store'][1],
            {'tiddlyweb.config': config}
            )

def test_make_a_bunch():
    for x in xrange(RANGE):
        bag_name = u'bag%s' % x
        recipe_name = u'recipe%s' % x
        tiddler_name = u'tiddler%s' % x
        recipe_list = [(bag_name, '')]
        tiddler_text = u'hey ho %s' % x
        field_name = u'field%s' % x
        tag_name = u'tag%s' % x
        user_name = u'user%s' % x
        user_pass = u'pass%s' % x
        user_note = u'note%s' % x
        user_roles = [u'rolehold', u'role%s' % x]

        bag = Bag(bag_name)
        bag.policy.owner = u'owner%s' % x
        bag.policy.read = [u'hi%s' % x]
        bag.policy.manage = [u'R:hi%s' % x]
        store.put(bag)
        recipe = Recipe(recipe_name)
        recipe.policy.owner = u'owner%s' % x
        recipe.policy.read = [u'hi%s' % x]
        recipe.policy.manage = [u'R:hi%s' % x]
        recipe.set_recipe(recipe_list)
        store.put(recipe)
        tiddler = Tiddler(tiddler_name, bag_name)
        tiddler.text = tiddler_text
        tiddler.fields[field_name] = field_name
        tiddler.fields['server.host'] = 'gunky'
        tiddler.tags = [tag_name]
        store.put(tiddler)
        user = User(user_name)
        user.set_password(user_pass)
        user.note = user_note
        for role in user_roles:
            user.add_role(role)
        store.put(user)

    bags = [bag.name for bag in store.list_bags()]
    recipes = [recipe.name for recipe in store.list_recipes()]
    users = [user.usersign for user in store.list_users()]
    assert len(bags) == RANGE
    assert len(recipes) == RANGE
    assert len(users) == RANGE
    for x in xrange(RANGE):
        bname = 'bag%s' % x
        rname = 'recipe%s' % x
        uname = 'user%s' % x
        assert bname in bags
        assert rname in recipes
        assert uname in users

    bag = Bag('bag0')
    bag = store.get(bag)
    try:
        tiddlers = list(store.list_bag_tiddlers(bag))
    except AttributeError:
        tiddlers = list(bag.gen_tiddlers())
    assert len(tiddlers) == 1
    assert tiddlers[0].title == 'tiddler0'
    assert tiddlers[0].fields['field0'] == 'field0'
    assert tiddlers[0].tags == ['tag0']
    assert bag.policy.read == ['hi0']
    assert bag.policy.manage == ['R:hi0']
    assert bag.policy.owner == 'owner0'

    user = User('user1')
    user = store.get(user)
    assert user.usersign == 'user1'
    assert user.check_password('pass1')
    assert user.note == 'note1'
    assert 'role1' in user.list_roles()
    assert 'rolehold' in user.list_roles()

    recipe = Recipe('recipe2')
    recipe = store.get(recipe)
    assert recipe.name == 'recipe2'
    bags = [bag_name for bag_name, filter in recipe.get_recipe()]
    assert len(bags) == 1
    assert 'bag2' in bags
    assert recipe.policy.read == ['hi2']
    assert recipe.policy.manage == ['R:hi2']
    assert recipe.policy.owner == 'owner2'

    # delete the above things
    store.delete(bag)
    py.test.raises(NoBagError, 'store.delete(bag)')
    py.test.raises(NoBagError, 'store.get(bag)')
    store.delete(recipe)
    py.test.raises(NoRecipeError, 'store.delete(recipe)')
    py.test.raises(NoRecipeError, 'store.get(recipe)')
    store.delete(user)
    py.test.raises(NoUserError, 'store.delete(user)')
    py.test.raises(NoUserError, 'store.get(user)')

    tiddler = Tiddler('tiddler9', 'bag9')
    store.get(tiddler)
    assert tiddler.bag == 'bag9'
    assert tiddler.text == 'hey ho 9'
    assert tiddler.tags == ['tag9']
    assert tiddler.fields['field9'] == 'field9'
    assert 'server.host' not in tiddler.fields
    store.delete(tiddler)
    py.test.raises(NoTiddlerError, 'store.delete(tiddler)')
    py.test.raises(NoTiddlerError, 'store.get(tiddler)')

def test_binary_tiddler():
    tiddler = Tiddler('binary', 'bag8')
    tiddler.type = 'application/binary'
    tiddler.text = 'not really binary'
    store.put(tiddler)

    new_tiddler = Tiddler('binary', 'bag8')
    new_tiddler = store.get(new_tiddler)
    assert new_tiddler.title == 'binary'
    assert new_tiddler.type == 'application/binary'
    assert tiddler.text == b64encode('not really binary')

def test_handle_empty_policy():
    bag = Bag('empty')
    store.put(bag)
    new_bag = store.get(Bag('empty'))
    assert new_bag.policy.read == []
    assert new_bag.policy.manage == []
    assert new_bag.policy.create == []
    assert new_bag.policy.write == []
    assert new_bag.policy.accept == []
    assert new_bag.policy.owner == None

def test_tiddler_revisions():
    bag_name = u'bag8'
    for i in xrange(20):
        tiddler = Tiddler(u'oh hi', bag_name)
        tiddler.text = u'%s times we go' % i
        tiddler.fields[u'%s' % i] = u'%s' % i
        store.put(tiddler)

    revisions = store.list_tiddler_revisions(Tiddler('oh hi', bag_name))
    assert len(revisions) == 20
    tiddler = Tiddler('oh hi', bag_name)
    tiddler.revision = 14
    tiddler = store.get(tiddler)
    assert tiddler.title == 'oh hi'
    assert tiddler.text == '13 times we go'
    assert tiddler.fields['13'] == '13'
    assert '12' not in tiddler.fields

    tiddler.revision = 90
    py.test.raises(NoTiddlerError, 'store.get(tiddler)')

    py.test.raises(NoTiddlerError,
            'store.list_tiddler_revisions(Tiddler("sleepy", "cow"))')

def test_tiddler_no_bag():
    tiddler = Tiddler('hi')
    py.test.raises(NoBagError, 'store.put(tiddler)')

def test_list_tiddlers_no_bag():
    bag = Bag('carne')
    try:
        py.test.raises(NoBagError, 'store.list_bag_tiddlers(bag).next()')
    except AttributeError:
        assert True
