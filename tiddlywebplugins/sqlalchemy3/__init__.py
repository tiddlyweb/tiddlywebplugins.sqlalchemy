"""
The base for using sqlalchemy as a store with TiddlyWeb.
"""

from __future__ import absolute_import

import logging

from tiddlyweb import __version__ as VERSION

from base64 import b64encode, b64decode
from sqlalchemy import select, desc
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import and_

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

from .model import (Base, Session, sBag, sPolicy, sRecipe, sTiddler, sRevision,
        sText, sTag, sField, sCurrentRevision, sFirstRevision, sUser, sRole)

__version__ = '3.0.10'

#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
#logging.getLogger('sqlalchemy.pool').setLevel(logging.DEBUG)


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
        Base.metadata.bind = engine
        Session.configure(bind=engine)
        self.session = Session()
        self.serializer = Serializer('text')

        if not Store.mapped:
            Base.metadata.create_all(engine)
            Store.mapped = True

    def _db_config(self):
        return self.store_config['db_config']

    def list_recipes(self):
        try:
            recipes = self.session.query(sRecipe).all()
            for srecipe in recipes:
                recipe = Recipe(srecipe.name)
                recipe = self._load_recipe(recipe, srecipe)
                yield recipe
            self.session.close()
        except:
            self.session.rollback()
            raise

    def list_bags(self):
        try:
            bags = self.session.query(sBag).all()
            for sbag in bags:
                bag = Bag(sbag.name)
                bag = self._load_bag(bag, sbag)
                yield bag
            self.session.close()
        except:
            self.session.rollback()
            raise

    def list_users(self):
        try:
            users = self.session.query(sUser.usersign).all()
            self.session.close()
        except:
            self.session.rollback()
            raise
        for user in users:
            yield User(user[0])

    def list_bag_tiddlers(self, bag):
        try:
            try:
                self.session.query(sBag.id).filter(
                    sBag.name == bag.name).one()
                tiddlers = self.session.query(sTiddler).filter(
                        sTiddler.bag == bag.name).all()
            except NoResultFound, exc:
                raise NoBagError('no results for bag %s, %s' % (bag.name, exc))
            self.session.close()
        except:
            self.session.rollback()
            raise

        return (Tiddler(stiddler.title, bag.name) for stiddler in tiddlers)

    def list_tiddler_revisions(self, tiddler):
        try:
            try:
                revisions = self.session.query(sTiddler).filter(and_(
                        sTiddler.title == tiddler.title,
                        sTiddler.bag == tiddler.bag)).one().revisions
            except NoResultFound, exc:
                raise NoTiddlerError('tiddler %s not found: %s' % (
                    tiddler.title, exc))

            return [revision.number for revision in revisions]
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
            self.session.commit()
        except:
            self.session.rollback()
            raise

    def tiddler_delete(self, tiddler):
        try:
            try:
                rows = (self.session.query(sTiddler).
                        filter(sTiddler.title == tiddler.title).
                        filter(sTiddler.bag == tiddler.bag).delete())
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
                if tiddler.revision:
                    revision = self.session.query(sRevision).filter(
                            sRevision.number == tiddler.revision).one()
                    stiddler = self.session.query(sTiddler).filter(and_(
                            sTiddler.id == revision.tiddler_id,
                            sTiddler.title == tiddler.title,
                            sTiddler.bag == tiddler.bag)).one()
                    current_revision = revision
                else:
                    stiddler = self.session.query(sTiddler).filter(and_(
                            sTiddler.title == tiddler.title,
                            sTiddler.bag == tiddler.bag)).one()
                    current_revision = stiddler.current
                base_revision = stiddler.first
                tiddler = self._load_tiddler(tiddler, current_revision,
                    base_revision)
                self.session.close()
                return tiddler
            except NoResultFound, exc:
                raise NoTiddlerError('Tiddler %s:%s:%s not found: %s' %
                        (tiddler.bag, tiddler.title, tiddler.revision, exc))
        except:
            self.session.rollback()
            raise

    def tiddler_put(self, tiddler):
        tiddler.revision = None
        try:
            if not tiddler.bag:
                raise NoBagError('bag required to save')
            try:
                sbag = self.session.query(sBag.id).filter(sBag.name
                        == tiddler.bag).one()
            except NoResultFound, exc:
                raise NoBagError('bag %s must exist for tiddler save: %s'
                        % (tiddler.bag, exc))
            current_revision_number = self._store_tiddler(tiddler)
            tiddler.revision = current_revision_number
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

        for pol in spolicy:
            principal_name = pol.principal_name
            if pol.principal_type == 'R':
                principal_name = 'R:%s' % principal_name
            if pol.constraint == 'owner':
                policy.owner = principal_name
            else:
                principals = getattr(policy, pol.constraint, [])
                principals.append(principal_name)
                setattr(policy, pol.constraint, principals)
        return policy

    def _load_tiddler(self, tiddler, current_revision, base_revision):
        tiddler.modifier = current_revision.modifier
        tiddler.modified = current_revision.modified
        tiddler.revision = current_revision.number
        tiddler.type = current_revision.type

        try:
            if binary_tiddler(tiddler):
                tiddler.text = b64decode(
                        current_revision.text.text.lstrip().rstrip())
            else:
                tiddler.text = current_revision.text.text
        except AttributeError:
            tiddler.text = ''

        tiddler.tags = [tag.tag for tag in current_revision.tags]

        for sfield in current_revision.fields:
            tiddler.fields[sfield.name] = sfield.value

        tiddler.created = base_revision.modified
        tiddler.creator = base_revision.modifier

        return tiddler

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
                bag, filter_string = line.rsplit('?', 1)
                recipe.append((bag, filter_string))
        return recipe

    def _load_user(self, user, suser):
        user.usersign = suser.usersign
        user._password = suser.password
        user.note = suser.note
        for role in suser.roles:
            user.add_role(role.name)
        return user

    def _store_bag(self, bag):
        try:
            sbag = self.session.query(sBag).filter(
                    sBag.name == bag.name).one()
        except NoResultFound:
            sbag = sBag(bag.name)
            self.session.add(sbag)
        sbag.desc = bag.desc
        self._store_policy(sbag, bag.policy)
        return sbag

    def _store_policy(self, container, policy):
        policies = []
        for attribute in policy.attributes:
            if attribute == 'owner':
                value = policy.owner is None and [] or [policy.owner]
            else:
                value = getattr(policy, attribute, [])
            spolicies = self._handle_policy_attribute(attribute, value)
            policies.extend(spolicies)

        container.policy = policies

    def _handle_policy_attribute(self, attribute, value):
        spolicies = []
        for principal_name in value:
            if principal_name != None:
                if principal_name.startswith('R:'):
                    pname = principal_name[2:]
                    ptype = u'R'
                else:
                    pname = principal_name
                    ptype = u'U'

                try:
                    spolicy = self.session.query(sPolicy).filter(and_(
                        sPolicy.constraint == attribute,
                        sPolicy.principal_name == pname,
                        sPolicy.principal_type == ptype)).one()
                except NoResultFound:
                    spolicy = sPolicy()
                    spolicy.constraint = attribute
                    spolicy.principal_name = pname
                    spolicy.principal_type = ptype
                    self.session.add(spolicy)
                spolicies.append(spolicy)
        return spolicies

    def _store_recipe(self, recipe):
        try:
            srecipe = self.session.query(sRecipe).filter(
                    sRecipe.name == recipe.name).one()
        except NoResultFound:
            srecipe = sRecipe(recipe.name)
            self.session.add(srecipe)
        srecipe.desc = recipe.desc
        self._store_policy(srecipe, recipe.policy)
        srecipe.recipe_string = self._store_recipe_string(recipe.get_recipe())
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
        if binary_tiddler(tiddler):
            tiddler.text = unicode(b64encode(tiddler.text))

        try:
            stiddler = self.session.query(sTiddler.id).filter(
                    and_(sTiddler.title == tiddler.title,
                        sTiddler.bag == tiddler.bag)).one()
            new_tiddler = False
        except NoResultFound:
            stiddler = sTiddler(tiddler.title, tiddler.bag)
            self.session.add(stiddler)
            new_tiddler = True
            self.session.flush()

        srevision = sRevision()
        srevision.type = tiddler.type
        srevision.modified = tiddler.modified
        srevision.modifier = tiddler.modifier
        srevision.tiddler_id = stiddler.id
        self.session.add(srevision)

        self.session.flush()

        # Do text inserts "raw" so we can use DELAYED
        if 'mysql_engine' in sText.__table__.kwargs:
            prefix = 'DELAYED'
        else:
            prefix = ''
        text_insert = (sText.__table__.insert()
                .prefix_with(prefix).values(text=tiddler.text,
                    revision_number=srevision.number))
        self.session.execute(text_insert)

        for tag in set(tiddler.tags):
            stag = sTag(tag)
            stag.revision_number = srevision.number
            self.session.add(stag)

        for field in tiddler.fields:
            if not field.startswith('server.'):
                sfield = sField(field, tiddler.fields[field])
                sfield.revision_number = srevision.number
                self.session.add(sfield)

        self.session.flush()

        current_revision = sCurrentRevision()
        current_revision.tiddler_id = stiddler.id
        current_revision.current_id = srevision.number
        self.session.merge(current_revision)

        if new_tiddler:
            first_revision = sFirstRevision()
            first_revision.tiddler_id = stiddler.id
            first_revision.first_id = srevision.number
            self.session.merge(first_revision)

        return srevision.number

    def _store_user(self, user):
        suser = sUser()
        suser.usersign = user.usersign
        suser.password = user._password
        suser.note = user.note
        return suser
