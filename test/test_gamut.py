
import os

import py.test

from tiddlyweb.config import config
from tiddlyweb.store import Store, NoBagError, NoUserError, NoRecipeError, NoTiddlerError

from tiddlyweb.model.bag import Bag
from tiddlyweb.model.policy import Policy
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
        bag.policy.read = [u'hi%s' % x, 'andextra']
        bag.policy.manage = [u'R:hi%s' % x, 'andmanage']
        store.put(bag)
        recipe = Recipe(recipe_name)
        recipe.policy.owner = u'owner%s' % x
        recipe.policy.read = [u'hi%s' % x, 'andextra']
        recipe.policy.manage = [u'R:hi%s' % x, 'andmanage']
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
    tiddlers = []
    for tiddler in store.list_bag_tiddlers(bag):
        tiddlers.append(store.get(tiddler))
    assert len(tiddlers) == 1
    assert tiddlers[0].title == 'tiddler0'
    assert tiddlers[0].fields['field0'] == 'field0'
    assert tiddlers[0].tags == ['tag0']
    assert sorted(bag.policy.read) == ['andextra', 'hi0']
    assert sorted(bag.policy.manage) == ['R:hi0', 'andmanage']
    assert bag.policy.owner == 'owner0'

    bag = Bag('bag0')
    bag = store.get(bag)
    bag.policy.read.remove('hi0')
    store.put(bag)
    bag = Bag('bag0')
    bag = store.get(bag)
    assert bag.policy.read == ['andextra']

    bag = Bag('bag0')
    bag = store.get(bag)
    bag.policy.read.append('hi0')
    store.put(bag)
    bag = Bag('bag0')
    bag = store.get(bag)
    assert sorted(bag.policy.read) == ['andextra', 'hi0']

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
    assert sorted(recipe.policy.read) == ['andextra', 'hi2']
    assert sorted(recipe.policy.manage) == ['R:hi2', 'andmanage']
    assert recipe.policy.owner == 'owner2'

    recipe.policy.manage = ['andmanage']
    store.put(recipe)

    recipe = Recipe ('recipe2')
    recipe = store.get(recipe)
    assert recipe.policy.manage == ['andmanage']

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

def test_reuse_policy_object():
    """
    Explicitly test a bug fix in policy handling wherein the owner
    field could get transformed into (and stay) a list thus ruining
    second use. Not that second use is encourage, but it could happen.
    """
    policy = Policy()
    policy.owner = 'campy'
    bag = Bag('policytest1')
    bag.policy = policy
    store.put(bag)
    bag = Bag('policytest2')
    bag.policy = policy
    store.put(bag)

    bag1 = store.get(Bag('policytest1'))
    bag2 = store.get(Bag('policytest2'))
    assert bag1.policy.owner == 'campy'
    assert bag2.policy.owner == 'campy'
    assert bag1.policy.owner == bag2.policy.owner

def test_tiddler_revisions():
    bag_name = u'bag8'
    for i in xrange(20):
        tiddler = Tiddler(u'oh hi', bag_name)
        tiddler.text = u'%s times we go' % i
        tiddler.fields[u'%s' % i] = u'%s' % i
        store.put(tiddler)

    revisions = store.list_tiddler_revisions(Tiddler('oh hi', bag_name))
    assert len(revisions) == 20
    first_revision = revisions[-1]
    tiddler = Tiddler('oh hi', bag_name)
    tiddler.revision = first_revision + 13
    tiddler = store.get(tiddler)
    assert tiddler.title == 'oh hi'
    assert tiddler.text == '13 times we go'
    assert tiddler.fields['13'] == '13'
    assert '12' not in tiddler.fields

    tiddler.revision = 90
    py.test.raises(NoTiddlerError, 'store.get(tiddler)')

    py.test.raises(NoTiddlerError,
            'store.list_tiddler_revisions(Tiddler("sleepy", "cow"))')

def test_interleaved_tiddler_revisions():
    bag_name = u'bag8'
    for i in xrange(20):
        tiddler1 = Tiddler(u'oh yes', bag_name)
        tiddler2 = Tiddler(u'oh no', bag_name)
        tiddler1.text = u'%s times we yes' % i
        tiddler2.text = u'%s times we no' % i
        tiddler1.fields[u'%s' % i] = u'%s' % i
        tiddler2.fields[u'%s' % i] = u'%s' % i
        store.put(tiddler1)
        store.put(tiddler2)

    revisions = store.list_tiddler_revisions(Tiddler('oh yes', bag_name))
    assert len(revisions) == 20
    first_revision = revisions[-1]
    tiddler = Tiddler('oh yes', bag_name)
    tiddler.revision = first_revision + 26 
    tiddler = store.get(tiddler)
    assert tiddler.title == 'oh yes'
    assert tiddler.text == '13 times we yes'
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

def test_2bag_policy():
    bag = Bag('pone')
    bag.policy.read = ['cdent']
    bag.policy.write = ['cdent']
    store.put(bag)

    bag = Bag('ptwo')
    bag.policy.read = ['cdent', 'fnd']
    bag.policy.write = ['cdent']
    store.put(bag)

    pone = store.get(Bag('pone'))
    ptwo = store.get(Bag('ptwo'))

    assert pone.policy.read == ['cdent']
    assert pone.policy.write == ['cdent']

    assert sorted(ptwo.policy.read) == ['cdent', 'fnd']
    assert ptwo.policy.write == ['cdent']

    store.delete(pone)

    ptwo = store.get(Bag('ptwo'))

    assert sorted(ptwo.policy.read) == ['cdent', 'fnd']
    assert ptwo.policy.write == ['cdent']

    bag = Bag('pone')
    bag.policy.read = ['cdent']
    bag.policy.write = ['cdent']
    store.put(bag)

    pone = store.get(Bag('pone'))
    assert pone.policy.read == ['cdent']
    assert pone.policy.write == ['cdent']

    pone.policy.read.append('fnd')

    store.put(pone)

    pone = store.get(Bag('pone'))

    assert sorted(pone.policy.read) == ['cdent', 'fnd']

def test_2recipe_policy():
    recipe = Recipe('pone')
    recipe.policy.read = ['cdent']
    recipe.policy.write = ['cdent']
    store.put(recipe)

    recipe = Recipe('ptwo')
    recipe.policy.read = ['cdent', 'fnd']
    recipe.policy.write = ['cdent']
    store.put(recipe)

    pone = store.get(Recipe('pone'))
    ptwo = store.get(Recipe('ptwo'))

    assert pone.policy.read == ['cdent']
    assert pone.policy.write == ['cdent']

    assert sorted(ptwo.policy.read) == ['cdent', 'fnd']
    assert ptwo.policy.write == ['cdent']

    store.delete(pone)

    ptwo = store.get(Recipe('ptwo'))

    assert sorted(ptwo.policy.read) == ['cdent', 'fnd']
    assert ptwo.policy.write == ['cdent']

    recipe = Recipe('pone')
    recipe.policy.read = ['cdent']
    recipe.policy.write = ['cdent']
    store.put(recipe)

    pone = store.get(Recipe('pone'))
    assert pone.policy.read == ['cdent']
    assert pone.policy.write == ['cdent']

    pone.policy.read.append('fnd')

    store.put(pone)

    pone = store.get(Recipe('pone'))

    assert sorted(pone.policy.read) == ['cdent', 'fnd']

def test_revisions_deletions():
    tiddler = Tiddler('tone', 'pone')
    tiddler.text = 'revision1'
    tiddler.tags = ['1','2']
    store.put(tiddler)
    tiddler.text = 'revision2'
    tiddler.tags = ['3','4']
    store.put(tiddler)

    revisions = store.list_tiddler_revisions(tiddler)

    assert len(revisions) == 2

    store.delete(tiddler)

    py.test.raises(NoTiddlerError, 'store.list_tiddler_revisions(tiddler)')


def test_bag_deletes_tiddlers():
    tiddler = Tiddler('tone', 'pone')
    store.put(tiddler)
    tiddler = Tiddler('uone', 'pone')
    store.put(tiddler)

    bag = Bag('pone')

    tiddlers = list(store.list_bag_tiddlers(bag))
    assert len(tiddlers) == 2

    store.delete(bag)

    bag = Bag('pone')
    py.test.raises(NoBagError, 'list(store.list_bag_tiddlers(bag))')
    py.test.raises(NoTiddlerError, 'store.list_tiddler_revisions(tiddler)')

def test_saving_to_non_bag():
    tiddler = Tiddler('oh hi', 'nonexistentbag')
    py.test.raises(NoBagError, 'store.put(tiddler)')
