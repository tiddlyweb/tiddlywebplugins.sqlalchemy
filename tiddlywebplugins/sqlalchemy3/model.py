
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.schema import (Table, Column, UniqueConstraint, ForeignKey)
from sqlalchemy.types import Unicode, Integer, String, UnicodeText, CHAR
from sqlalchemy.orm import relationship, mapper

Base = declarative_base()
Session = scoped_session(sessionmaker())

bag_policy_table = Table('bag_policy', Base.metadata,
    Column('bag_id', Integer, ForeignKey('bag.id', ondelete='CASCADE'),
        index=True, nullable=False, primary_key=True),
    Column('policy_id', Integer, ForeignKey('policy.id', ondelete='CASCADE'),
        nullable=False, primary_key=True))

recipe_policy_table = Table('recipe_policy', Base.metadata,
    Column('recipe_id', Integer, ForeignKey('recipe.id', ondelete='CASCADE'),
        index=True, nullable=False, primary_key=True),
    Column('policy_id', Integer, ForeignKey('policy.id', ondelete='CASCADE'),
        nullable=False, primary_key=True))

current_revision_table = Table('current_revision', Base.metadata,
        Column('tiddler_id', Integer, ForeignKey('tiddler.id',
            ondelete='CASCADE'), index=True, nullable=False, primary_key=True),
        Column('current_id', Integer, ForeignKey('revision.number',
            ondelete='CASCADE'), index=True, nullable=False),
        UniqueConstraint('tiddler_id', 'current_id'))

first_revision_table = Table('first_revision', Base.metadata,
        Column('tiddler_id', Integer, ForeignKey('tiddler.id',
            ondelete='CASCADE'), index=True, nullable=False, primary_key=True),
        Column('first_id', Integer, ForeignKey('revision.number',
            ondelete='CASCADE'), index=True, nullable=False),
        UniqueConstraint('tiddler_id', 'first_id'))


class sCurrentRevision(object):
    pass


class sFirstRevision(object):
    pass


mapper(sCurrentRevision, current_revision_table)
mapper(sFirstRevision, first_revision_table)


class sField(Base):

    __tablename__ = 'field'

    revision_number = Column(Integer,
            ForeignKey('revision.number', ondelete='CASCADE'),
            nullable=False, index=True, primary_key=True)
    name = Column(Unicode(64), nullable=False, index=True, primary_key=True)
    value = Column(Unicode(1024), nullable=False, index=True, primary_key=True)

    def __init__(self, name, value):
        object.__init__(self)
        self.name = name
        self.value = value

    def __repr__(self):
        return '<sField(%s:%s)>' % (self.name, self.value)


class sTag(Base):

    __tablename__ = 'tag'

    revision_number = Column(Integer,
            ForeignKey('revision.number', ondelete='CASCADE'),
            nullable=False, index=True, primary_key=True)
    tag = Column(Unicode(256), nullable=False, index=True,
            primary_key=True)

    def __init__(self, tag):
        object.__init__(self)
        self.tag = tag

    def __repr__(self):
        return '<sTag(%s:%s)>' % (self.revision_number, self.tag)


class sText(Base):

    __tablename__ = 'text'

    revision_number = Column('revision_number', Integer,
            ForeignKey('revision.number', ondelete='CASCADE'),
            nullable=False, index=True, primary_key=True)
    text = Column(UnicodeText(16777215), nullable=False, default=u'')

    def __init__(self, text):
        object.__init__(self)
        self.text = text

    def __repr__(self):
        return '<sText(%s:<text>)>' % (self.revision_number)


class sRevision(Base):

    __tablename__ = 'revision'

    tiddler_id = Column(Integer, ForeignKey('tiddler.id', ondelete='CASCADE'),
            nullable=False,
            index=True)
    number = Column(Integer, primary_key=True, nullable=False,
        autoincrement=True)
    modifier = Column(Unicode(128), index=True)
    modified = Column(String(14), index=True)
    type = Column(String(128), index=True)

    fields = relationship('sField',
        cascade='all, delete-orphan',
        backref='fields',
        lazy=True)
    tags = relationship('sTag',
        cascade='all, delete-orphan',
        backref='tags',
        lazy=True)
    text = relationship('sText',
        cascade='all, delete-orphan',
        backref='revision_text',
        uselist=False,
        lazy=True)

    def __repr__(self):
        return '<sRevision(%s:%s)>' % (self.tiddler_id,
                self.number)


class sTiddler(Base):
    __tablename__ = 'tiddler'
    __table_args__ = (
            UniqueConstraint('title', 'bag'),)

    id = Column(Integer,
            primary_key=True,
            nullable=False,
            autoincrement=True)
    bag = Column(Unicode(128), ForeignKey('bag.name', ondelete='CASCADE'),
            index=True,
            nullable=False)
    title = Column(Unicode(128),
            index=True,
            nullable=False)

    revisions = relationship('sRevision',
            order_by="desc(sRevision.number)",
            lazy=True,
            cascade='delete, delete-orphan')

    current = relationship('sRevision',
            lazy=True,
            cascade='delete, delete-orphan',
            uselist=False,
            single_parent=True,
            secondary=current_revision_table,
            primaryjoin=(id == current_revision_table.c.tiddler_id),
            secondaryjoin=(
                current_revision_table.c.current_id == sRevision.number))

    first = relationship('sRevision',
            lazy=True,
            uselist=False,
            cascade='delete, delete-orphan',
            single_parent=True,
            secondary=first_revision_table,
            primaryjoin=(id == first_revision_table.c.tiddler_id),
            secondaryjoin=first_revision_table.c.first_id == sRevision.number)

    def __init__(self, title, bag):
        object.__init__(self)
        self.bag = bag
        self.title = title

    def __repr__(self):
        return '<sTiddler(%s:%s:%s)>' % (self.id, self.bag, self.title)


class sPolicy(Base):

    __tablename__ = 'policy'
    __table_args__ = (
        UniqueConstraint('constraint', 'principal_name',
            'principal_type'),)

    id = Column(Integer, nullable=False, primary_key=True,
        autoincrement=True)
    constraint = Column(String(12), nullable=False)
    principal_name = Column(Unicode(128), index=True, nullable=False)
    principal_type = Column(CHAR(1), nullable=False)

    def __init__(self, id=None):
        object.__init__(self)
        self.id = id

    def __repr__(self):
        return '<sPolicy(%s:%s:%s:%s)>' % (self.id,
                self.principal_type, self.principal_name, self.constraint)


class sBag(Base):

    __tablename__ = 'bag'
    __table_args = (
            UniqueConstraint('id', 'name'))

    id = Column(Integer, primary_key=True, nullable=False,
            autoincrement=True)
    name = Column(Unicode(128), index=True, nullable=False, unique=True)
    desc = Column(Unicode(1024))

    policy = relationship('sPolicy',
            secondary=bag_policy_table,
            lazy=False)
    tiddlers = relationship('sTiddler',
            cascade='delete, delete-orphan',
            lazy=True)

    def __init__(self, name, desc=''):
        object.__init__(self)
        self.name = name
        self.desc = desc
        self.policy = []

    def __repr__(self):
        return '<sBag(%s:%s)>' % (self.id, self.name)


class sRecipe(Base):

    __tablename__ = 'recipe'
    __table_args = (
            UniqueConstraint('id', 'name'))

    id = Column(Integer, primary_key=True, nullable=False,
        autoincrement=True)
    name = Column(Unicode(128), index=True, nullable=False, unique=True)
    desc = Column(Unicode(1024))
    recipe_string = Column(UnicodeText, default=u'')

    policy = relationship('sPolicy',
            secondary=recipe_policy_table,
            lazy=False)

    def __init__(self, name, desc=''):
        self.name = name
        self.desc = desc
        self.policy = []

    def __repr__(self):
        return '<sRecipe(%s:%s)>' % (self.id, self.name)


class sRole(Base):

    __tablename__ = 'role'

    user = Column(Unicode(128),
            ForeignKey('user.usersign', ondelete='CASCADE'), nullable=False,
            primary_key=True)
    name = Column(Unicode(50), nullable=False, primary_key=True)

    def __repr__(self):
        return '<sRole(%s:%s)>' % (self.user, self.name)


class sUser(Base):

    __tablename__ = 'user'

    usersign = Column(Unicode(128), primary_key=True, nullable=False)
    note = Column(Unicode(1024))
    password = Column(String(128))

    roles = relationship(sRole,
            lazy=False,
            cascade='delete')

    def __repr__(self):
        return '<sUser(%s)>' % (self.usersign)
