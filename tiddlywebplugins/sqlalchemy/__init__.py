"""
Yet another attempt to get a good sql store.
"""
import logging

from tiddlyweb import __version__ as VERSION

from base64 import b64encode, b64decode
from sqlalchemy import select, desc
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import relation, mapper, sessionmaker, scoped_session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.schema import (Table, Column, PrimaryKeyConstraint,
        UniqueConstraint, ForeignKey, ForeignKeyConstraint, Index, MetaData)
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import and_, text as text_, alias
from sqlalchemy.types import Unicode, Integer, String, UnicodeText, CHAR
from tiddlyweb.model.bag import Bag
from tiddlyweb.model.policy import Policy
from tiddlyweb.model.recipe import Recipe
from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.user import User
from tiddlyweb.serializer import Serializer
from tiddlyweb.store import (NoBagError, NoRecipeError, NoTiddlerError,
        NoUserError)
from tiddlyweb.stores import StorageInterface
from tiddlyweb.util import binary_tiddler

__version__ = '0.9.17'

#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
#logging.getLogger('sqlalchemy.pool').setLevel(logging.DEBUG)

metadata = MetaData()
Session = scoped_session(sessionmaker())

field_table = Table('field', metadata,
    Column('revision_number', Integer, index=True, nullable=False),
    Column('name', Unicode(64), index=True, nullable=False),
    Column('value', Unicode(1024), index=True),
    PrimaryKeyConstraint('revision_number', 'name'),
    ForeignKeyConstraint(['revision_number'],
                         ['revision.number'],
                         onupdate='CASCADE', ondelete='CASCADE'),
    )

tag_table = Table('tag', metadata,
    Column('revision_number', Integer, index=True, nullable=False),
    Column('tag', Unicode(1024), index=True, nullable=False),
    PrimaryKeyConstraint('revision_number', 'tag'),
    ForeignKeyConstraint(['revision_number'],
                         ['revision.number'],
                         onupdate='CASCADE', ondelete='CASCADE'),
    )

revision_table = Table('revision', metadata,
    Column('bag_name', Unicode(128), index=True, nullable=False),
    Column('tiddler_title', Unicode(128), index=True, nullable=False),
    Column('number', Integer, primary_key=True, nullable=False,
        autoincrement=True),
    Column('modifier', Unicode(128), index=True),
    Column('modified', String(14), index=True),
    Column('type', String(128), index=True),
    Column('text', UnicodeText(16777215), nullable=False, default=u''),
    ForeignKeyConstraint(['bag_name', 'tiddler_title'],
                         ['tiddler.bag_name', 'tiddler.title'],
                         name='revision_name',
                         use_alter=True),
    )

tiddler_table = Table('tiddler', metadata,
    Column('bag_name', Unicode(128), index=True, nullable=False),
    Column('title', Unicode(128), index=True, nullable=False),
    Column('revision_number', Integer, nullable=False),
    Column('first_revision', Integer, nullable=True),
    PrimaryKeyConstraint('bag_name', 'title'),
    ForeignKeyConstraint(['bag_name'],
                         ['bag.name'],
                         onupdate='CASCADE', ondelete='CASCADE'),
    ForeignKeyConstraint(['revision_number'],
                         ['revision.number']),
    ForeignKeyConstraint(['first_revision'],
                         ['revision.number']),
    )

bag_table = Table('bag', metadata,
    Column('id', Integer, primary_key=True, nullable=False,
        autoincrement=True),
    Column('name', Unicode(128), index=True),
    Column('desc', Unicode(1024)),
    UniqueConstraint('id', 'name'),
    )

bag_policy_table = Table('bag_policy', metadata,
    Column('bag_id', Integer, ForeignKey('bag.id'), nullable=False),
    Column('policy_id', Integer, ForeignKey('policy.id'), nullable=False),
    #UniqueConstraint('bag_id', 'policy_id'),
    ForeignKeyConstraint(['bag_id'],
        ['bag.id'],
        onupdate='CASCADE', ondelete='CASCADE'),
    ForeignKeyConstraint(['policy_id'],
        ['policy.id'],
        onupdate='CASCADE', ondelete='CASCADE'),
    )

recipe_table = Table('recipe', metadata,
    Column('id', Integer, primary_key=True, nullable=False,
        autoincrement=True),
    Column('name', Unicode(128), index=True, nullable=False),
    Column('desc', Unicode(1024)),
    Column('recipe_string', UnicodeText, default=u''),
    UniqueConstraint('id', 'name'),
    )

recipe_policy_table = Table('recipe_policy', metadata,
    Column('recipe_id', Integer, ForeignKey('recipe.id'), nullable=False),
    Column('policy_id', Integer, ForeignKey('policy.id'), nullable=False),
    #UniqueConstraint('recipe_id', 'policy_id'),
    ForeignKeyConstraint(['recipe_id'],
        ['recipe.id'],
        onupdate='CASCADE', ondelete='CASCADE'),
    ForeignKeyConstraint(['policy_id'],
        ['policy.id'],
        onupdate='CASCADE', ondelete='CASCADE'),
    )

policy_table = Table('policy', metadata,
    Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
    Column('constraint', String(12), nullable=False),
    Column('principal_name', Unicode(128), index=True, nullable=False),
    Column('principal_type', CHAR(1), nullable=False),
    UniqueConstraint('constraint', 'principal_name', 'principal_type'),
    )

role_table = Table('role', metadata,
    Column('user', Unicode(128), nullable=False),
    Column('name', Unicode(50), nullable=False),
    PrimaryKeyConstraint('user', 'name'),
    ForeignKeyConstraint(['user'], ['user.usersign'],
        onupdate='CASCADE', ondelete='CASCADE'),
    )

user_table = Table('user', metadata,
    Column('usersign', Unicode(128), primary_key=True, nullable=False),
    Column('note', Unicode(1024)),
    Column('password', String(128)),
    PrimaryKeyConstraint('usersign'),
    )


class sField(object):

    def __init__(self, name, value):
        object.__init__(self)
        self.name = name
        self.value = value

    def __repr__(self):
        return '<sField(%s:%s)>' % (self.name, self.value)


class sTag(object):

    def __init__(self, tag):
        object.__init__(self)
        self.tag = tag

    def __repr__(self):
        return '<sTag(%s)>' % (self.tag)


class sRevision(object):

    def __init__(self, tiddler_title, bag_name, rev=0):
        object.__init__(self)
        self.bag_name = bag_name
        self.tiddler_title = tiddler_title
        self.number = rev

    def __repr__(self):
        return '<sRevision(%s:%s:%d)>' % (self.bag_name, self.tiddler_title,
                self.number)


class sTiddler(object):

    def __init__(self, title, bag_name, revision_number=None,
            first_revision=None):
        object.__init__(self)
        self.title = title
        self.bag_name = bag_name
        self.revision_number = revision_number
        self.first_revision = first_revision

    def __repr__(self):
        return '<sTiddler(%s:%s:%s:%s)>' % (self.bag_name,
                self.title, self.revision_number, self.first_revision)


class sPolicy(object):

    def __init__(self, id=None):
        object.__init__(self)
        self.id = id

    def __repr__(self):
        return '<sPolicy(%s:%s:%s:%s)>' % (self.id,
                self.principal_type, self.principal_name, self.constraint)


class sBag(object):

    def __init__(self, name, desc=''):
        object.__init__(self)
        self.name = name
        self.desc = desc

    def __repr__(self):
        return '<sBag(%s:%s)>' % (self.id, self.name)


class sRecipe(object):

    def __init__(self, name, desc=''):
        self.name = name
        self.desc = desc

    def __repr__(self):
        return '<sRecipe(%s:%s)>' % (self.id, self.name)


class sRole(object):

    def __repr__(self):
        return '<sRole(%s:%s)>' % (self.user, self.name)


class sUser(object):

    def __repr__(self):
        return '<sUser(%s)>' % (self.usersign)


mapper(sField, field_table)
mapper(sTag, tag_table)

mapper(sRevision, revision_table, properties=dict(
    fields=relation(sField,
        backref='revision',
        cascade='delete',
        lazy=False),
    tags=relation(sTag,
        backref='revision',
        cascade='delete',
        lazy=False)))

mapper(sTiddler, tiddler_table, properties=dict(
    revisions=relation(sRevision,
        primaryjoin=(and_(tiddler_table.c.bag_name == revision_table.c.bag_name,
            tiddler_table.c.title == revision_table.c.tiddler_title)),
        cascade='delete',
        lazy=True),
    current=relation(sRevision,
        primaryjoin=(tiddler_table.c.revision_number == revision_table.c.number),
        lazy=True),
    first=relation(sRevision,
        primaryjoin=(tiddler_table.c.first_revision == revision_table.c.number),
        lazy=True)))

mapper(sBag, bag_table, properties=dict(
    policy=relation(sPolicy, secondary=bag_policy_table,
        primaryjoin=(bag_policy_table.c.bag_id == bag_table.c.id),
        secondaryjoin=(bag_policy_table.c.policy_id == policy_table.c.id),
        cascade='all',
        passive_updates=False,
        lazy=True)))

mapper(sUser, user_table, properties=dict(
    roles=relation(sRole,
        lazy=False,
        cascade='delete')))

mapper(sPolicy, policy_table)

mapper(sRecipe, recipe_table, properties=dict(
    policy=relation(sPolicy, secondary=recipe_policy_table,
        primaryjoin=(recipe_table.c.id == recipe_policy_table.c.recipe_id),
        secondaryjoin=(recipe_policy_table.c.policy_id == policy_table.c.id),
        cascade='all',
        passive_updates=False,
        lazy=True)))

mapper(sRole, role_table)


class Store(StorageInterface):
    """
    A SqlAlchemy based storage interface for TiddlyWeb.
    """

    mapped = False

    def __init__(self, store_config=None, environ=None):
        super(Store, self).__init__(store_config, environ)
        self.store_type = self._db_config().split(':', 1)[0]
        self._init_store()

    def _init_store(self):
        """
        Establish the database engine and session,
        creating tables if needed.
        """
        engine = create_engine(self._db_config())
        metadata.bind = engine
        Session.configure(bind=engine)
        self.session = Session()
        self.serializer = Serializer('text')

        if not Store.mapped:
            metadata.create_all(engine)
            Store.mapped = True

    def _db_config(self):
        return self.store_config['db_config']

    def list_recipes(self):
        try:
            for srecipe in self.session.query(sRecipe.name).all():
                yield Recipe(srecipe[0])
            self.session.close()
        except:
            self.session.rollback()
            raise

    def list_bags(self):
        try:
            for sbag in self.session.query(sBag.name).all():
                yield Bag(sbag[0])
            self.session.close()
        except:
            self.session.rollback()
            raise

    def list_users(self):
        try:
            for suser in self.session.query(sUser.usersign).all():
                yield User(suser[0])
            self.session.close()
        except:
            self.session.rollback()
            raise

    def list_bag_tiddlers(self, bag):
        try:
            query = (self.session.query(sTiddler.title)
                    .filter(sTiddler.bag_name==bag.name))
            try:
                sbag = self.session.query(sBag.name).filter(sBag.name
                        == bag.name).one()
            except NoResultFound, exc:
                raise NoBagError('no results for bag %s, %s' % (bag.name, exc))

            tiddlers = query.all()

            def _bags_tiddler(stiddler):
                tiddler = Tiddler(stiddler.title, bag.name)
                return tiddler

            for stiddler in tiddlers:
                if stiddler:
                    yield _bags_tiddler(stiddler)
            self.session.close()
        except:
            self.session.rollback()
            raise

    def list_tiddler_revisions(self, tiddler):
        try:
            query = (self.session.query(sRevision.number)
                    .filter(revision_table.c.tiddler_title == tiddler.title)
                    .filter(revision_table.c.bag_name == tiddler.bag)
                    .order_by(sRevision.number.desc()))
            revisions = query.all()
            if not revisions:
                raise NoTiddlerError('tiddler %s not found' % (tiddler.title,))
            else:
                return [revision[0] for revision in revisions]
        except:
            self.session.rollback()
            raise
        finally:
            self.session.close()

    def recipe_delete(self, recipe):
        try:
            try:
                srecipe = self.session.query(sRecipe).filter(sRecipe.name
                        == recipe.name).one()
                self.session.delete(srecipe)
                self.session.commit()
            except NoResultFound, exc:
                raise NoRecipeError('no results for recipe %s, %s' %
                        (recipe.name, exc))
        except:
            self.session.rollback()
            raise

    def recipe_get(self, recipe):
        try:
            try:
                srecipe = self.session.query(sRecipe).filter(sRecipe.name
                        == recipe.name).one()
                recipe = self._load_recipe(recipe, srecipe)
                self.session.close()
                return recipe
            except NoResultFound, exc:
                raise NoRecipeError('no results for recipe %s, %s' %
                        (recipe.name, exc))
        except:
            self.session.rollback()
            raise

    def recipe_put(self, recipe):
        try:
            srecipe = self._store_recipe(recipe)
            self.session.merge(srecipe)
            self.session.commit()
        except:
            self.session.rollback()
            raise

    def bag_delete(self, bag):
        try:
            try:
                sbag = self.session.query(sBag).filter(sBag.name
                        == bag.name).one()
                self.session.delete(sbag)
                self.session.commit()
            except NoResultFound, exc:
                raise NoBagError('Bag %s not found: %s' % (bag.name, exc))
        except:
            self.session.rollback()
            raise

    def bag_get(self, bag):
        try:
            try:
                sbag = self.session.query(sBag).filter(sBag.name
                        == bag.name).one()
                bag = self._load_bag(bag, sbag)
                self.session.close()
                return bag
            except NoResultFound, exc:
                raise NoBagError('Bag %s not found: %s' % (bag.name, exc))
        except:
            self.session.rollback()
            raise

    def bag_put(self, bag):
        try:
            sbag = self._store_bag(bag)
            self.session.merge(sbag)
            self.session.commit()
        except:
            self.session.rollback()
            raise

    def tiddler_delete(self, tiddler):
        try:
            try:
                stiddlers = (self.session.query(sTiddler).
                        filter(sTiddler.title == tiddler.title).
                        filter(sTiddler.bag_name == tiddler.bag))
                rows = stiddlers.delete(synchronize_session='fetch')
                if rows == 0:
                    raise NoResultFound
                self.session.commit()
            except NoResultFound, exc:
                raise NoTiddlerError('no tiddler %s to delete, %s' %
                        (tiddler.title, exc))
        except:
            self.session.rollback()
            raise

    def tiddler_get(self, tiddler):
        try:
            try:
                query = (self.session.query(sTiddler).
                        filter(sTiddler.title == tiddler.title).
                        filter(sTiddler.bag_name == tiddler.bag))
                stiddler = query.one()
                base_tiddler = stiddler.first
                current_tiddler = stiddler.current
                if tiddler.revision:
                    query = (self.session.query(sRevision).
                        filter(sRevision.number == tiddler.revision))
                    current_tiddler = query.one()
                tiddler = self._load_tiddler(tiddler, current_tiddler,
                    base_tiddler)
                self.session.close()
                return tiddler
            except NoResultFound, exc:
                raise NoTiddlerError('Tiddler %s not found: %s' %
                        (tiddler.title, exc))
        except:
            self.session.rollback()
            raise


    def tiddler_put(self, tiddler):
        tiddler.revision = None
        try:
            if not tiddler.bag:
                raise NoBagError('bag required to save')
            stiddler = self._store_tiddler(tiddler)
            tiddler.revision = stiddler.revision_number
            self.session.commit()
        except:
            self.session.rollback()
            raise

    def user_delete(self, user):
        try:
            try:
                suser = self.session.query(sUser).filter(sUser.usersign
                        == user.usersign).one()
                self.session.delete(suser)
                self.session.commit()
            except NoResultFound, exc:
                raise NoUserError('user %s not found, %s' %
                        (user.usersign, exc))
        except:
            self.session.rollback()
            raise

    def user_get(self, user):
        try:
            try:
                suser = self.session.query(sUser).filter(sUser.usersign
                        == user.usersign).one()
                user = self._load_user(user, suser)
                self.session.close()
                return user
            except NoResultFound, exc:
                raise NoUserError('user %s not found, %s' %
                        (user.usersign, exc))
        except:
            self.session.rollback()
            raise

    def user_put(self, user):
        try:
            suser = self._store_user(user)
            self.session.merge(suser)
            self._store_roles(user)
            self.session.commit()
        except:
            self.session.rollback()
            raise

    def _load_bag(self, bag, sbag):
        bag.desc = sbag.desc
        bag.policy = self._load_policy(sbag.policy)
        bag.store = True
        return bag

    def _load_policy(self, spolicy):
        policy = Policy()

        if spolicy is not None:
            for pol in spolicy:
                principal_name = pol.principal_name
                if pol.principal_type == 'R':
                    principal_name = 'R:%s' % pol.principal_name
                if pol.constraint == 'owner':
                    policy.owner = principal_name
                else:
                    principals = getattr(policy, pol.constraint, [])
                    principals.append(principal_name)
                    setattr(policy, pol.constraint, principals)
        return policy

    def _load_tiddler(self, tiddler, stiddler, base_tiddler):
        try:
            revision = stiddler

            tiddler.modifier = revision.modifier
            tiddler.modified = revision.modified
            tiddler.revision = revision.number
            tiddler.type = revision.type

            if binary_tiddler(tiddler):
                tiddler.text = b64decode(revision.text.lstrip().rstrip())
            else:
                tiddler.text = revision.text
            tiddler.tags = [tag.tag for tag in revision.tags]

            for sfield in revision.fields:
                tiddler.fields[sfield.name] = sfield.value

            tiddler.created = base_tiddler.modified
            tiddler.creator = base_tiddler.modifier

            return tiddler
        except IndexError, exc:
            try:
                index_error = exc
                raise NoTiddlerError('No revision %s for tiddler %s, %s' %
                        (stiddler.rev, stiddler.title, exc))
            except AttributeError:
                raise NoTiddlerError('No tiddler for tiddler %s, %s' %
                        (stiddler.title, index_error))

    def _load_recipe(self, recipe, srecipe):
        recipe.desc = srecipe.desc
        recipe.policy = self._load_policy(srecipe.policy)
        recipe.set_recipe(self._load_recipe_string(srecipe.recipe_string))
        recipe.store = True
        return recipe

    def _load_recipe_string(self, recipe_string):
        recipe = []
        if recipe_string:
            for line in recipe_string.split('\n'):
                bag, filter = line.split('?', 1)
                recipe.append((bag, filter))
        return recipe

    def _load_user(self, user, suser):
        user.usersign = suser.usersign
        user._password = suser.password
        user.note = suser.note
        [user.add_role(role.name) for role in suser.roles]
        return user

    def _store_bag(self, bag):
        try:
            sbag = self.session.query(sBag).filter(
                    sBag.name==bag.name).one()
        except NoResultFound:
            sbag = sBag(bag.name, bag.desc)
        sbag.desc = bag.desc
        self._store_policy(sbag, bag.policy)
        return sbag

    def _store_policy(self, container, policy):
        policies = []
        for attribute in policy.attributes:
            if attribute == 'owner':
                policy.owner = policy.owner is None and [] or [policy.owner]

            for principal_name in getattr(policy, attribute, []):
                if principal_name != None:
                    if principal_name.startswith('R:'):
                        pname = principal_name[2:]
                        ptype = u'R'
                    else:
                        pname = principal_name
                        ptype = u'U'

                    try:
                        spolicy = self.session.query(sPolicy).filter(and_(
                                sPolicy.constraint==attribute,
                                sPolicy.principal_name==pname,
                                sPolicy.principal_type==ptype)).one()
                    except NoResultFound:
                        spolicy = sPolicy()

                        spolicy.constraint = attribute
                        spolicy.principal_name = pname
                        spolicy.principal_type = ptype
                        self.session.add(spolicy)
                    policies.append(spolicy)

        container.policy = policies

    def _store_recipe(self, recipe):
        try:
            srecipe = self.session.query(sRecipe).filter(
                    sRecipe.name==recipe.name).one()
        except NoResultFound:
            srecipe = sRecipe(recipe.name, recipe.desc)
        srecipe.desc = recipe.desc
        self._store_policy(srecipe, recipe.policy)
        srecipe.recipe_string = self._store_recipe_string(recipe)
        return srecipe

    def _store_recipe_string(self, recipe_list):
        string = u''
        string += u'\n'.join([u'%s?%s' % (unicode(bag),
            unicode(filter_string)) for bag, filter_string in recipe_list])
        return string

    def _store_roles(self, user):
        usersign = user.usersign
        for role in user.roles:
            srole = sRole()
            srole.user = usersign
            srole.name = role
            self.session.merge(srole)

    def _store_tiddler(self, tiddler):
        srevision = sRevision(tiddler.title, tiddler.bag, None)

        if binary_tiddler(tiddler):
            tiddler.text = unicode(b64encode(tiddler.text))

        srevision.type = tiddler.type
        srevision.modified = tiddler.modified
        srevision.modifier = tiddler.modifier
        srevision.text = tiddler.text
        for tag in tiddler.tags:
            stag = sTag(tag)
            self.session.add(stag)
            srevision.tags.append(stag)
        self.session.add(srevision)
        self.session.flush()

        for field in tiddler.fields:
            if field.startswith('server.'):
                continue
            sfield = sField(field, tiddler.fields[field])
            sfield.revision_number = srevision.number
            srevision.fields.append(sfield)
            self.session.merge(sfield)

        stiddler = (self.session.query(sTiddler)
                .filter(sTiddler.title == tiddler.title)
                .filter(sTiddler.bag_name == tiddler.bag)).first()
        if not stiddler:
            stiddler = sTiddler(tiddler.title, tiddler.bag)
        stiddler.revision_number = srevision.number
        if stiddler.first_revision is None:
            stiddler.first_revision = stiddler.revision_number
        self.session.merge(stiddler)

        return stiddler

    def _store_user(self, user):
        suser = sUser()
        suser.usersign = user.usersign
        suser.password = user._password
        suser.note = user.note
        return suser
