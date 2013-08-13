"""
Produce a sqlalchemy query object from the parser AST.
"""

from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import (and_, or_, not_, text as text_, label)
from sqlalchemy.sql import func

from tiddlyweb.store import StoreError

from tiddlywebplugins.sqlalchemy3 import (sField, sTag, sText, sTiddler,
        sRevision)


class Producer(object):
    """
    Turn a tiddlywebplugins.sqalchemy3.parser AST into a sqlalchemy query.
    """

    def produce(self, ast, query, fulltext=False, geo=False):
        """
        Given an ast and an empty query, build that query into a
        full select, based on the info in the ast.
        """
        self.joined_revision = False
        self.joined_tags = False
        self.joined_fields = False
        self.joined_text = False
        self.in_and = False
        self.in_or = False
        self.in_not = False
        self.limit = None
        self.query = query
        self.fulltext = fulltext
        self.geo = geo
        expressions = self._eval(ast, None)
        if self.limit:
            self.query = self.query.filter(expressions).limit(self.limit)
        else:
            self.query = self.query.filter(expressions)
        return self.query

    def _eval(self, node, fieldname):
        name = node.getName()
        return getattr(self, "_" + name)(node, fieldname)

    def _Toplevel(self, node, fieldname):
        expressions = []
        for subnode in node:
            expression = self._eval(subnode, fieldname)
            # Check to confirm that the expression is a proper
            # expression, otherwise don't add it. None is used
            # to indicate the producer sort of fell through
            if expression is not None:
                expressions.append(expression)
        return and_(*expressions)

    def _Word(self, node, fieldname):
        value = node[0]
        if fieldname:
            like = False
            try:
                if value.endswith('*'):
                    value = value.replace('*', '%')
                    like = True
            except TypeError:
                # Hack around field values containing parens
                # The node[0] is a non-string if that's the case.
                node[0] = '(' + value[0] + ')'
                return self._Word(node, fieldname)

            if fieldname == 'ftitle':
                fieldname = 'title'
            if fieldname == 'fbag':
                fieldname = 'bag'

            if fieldname == 'bag':
                if like:
                    expression = (sTiddler.bag.like(value))
                else:
                    expression = (sTiddler.bag == value)
            elif fieldname == 'title':
                if like:
                    expression = (sTiddler.title.like(value))
                else:
                    expression = (sTiddler.title == value)
            elif fieldname == 'id':
                bag, title = value.split(':', 1)
                expression = and_(sTiddler.bag == bag,
                        sTiddler.title == title)
            elif fieldname == 'tag':
                if self.in_and:
                    tag_alias = aliased(sTag)
                    self.query = self.query.join(tag_alias)
                    if like:
                        expression = (tag_alias.tag.like(value))
                    else:
                        expression = (tag_alias.tag == value)
                else:
                    if not self.joined_tags:
                        self.query = self.query.join(sTag)
                        if like:
                            expression = (sTag.tag.like(value))
                        else:
                            expression = (sTag.tag == value)
                        self.joined_tags = True
                    else:
                        if like:
                            expression = (sTag.tag.like(value))
                        else:
                            expression = (sTag.tag == value)
            elif fieldname == 'near' and self.geo:
                # proximity search on geo.long, geo.lat based on
                # http://cdent.tiddlyspace.com/bags/cdent_public/tiddlers/Proximity%20Search.html
                try:
                    lat, long, radius = [float(item)
                            for item in value.split(',', 2)]
                except ValueError, exc:
                    raise StoreError(
                            'failed to parse search query, malformed near: %s'
                            % exc)
                field_alias1 = aliased(sField)
                field_alias2 = aliased(sField)
                distance = label(u'greatcircle', (6371000
                    * func.acos(
                        func.cos(
                            func.radians(lat))
                        * func.cos(
                            func.radians(field_alias2.value))
                        * func.cos(
                            func.radians(field_alias1.value)
                            - func.radians(long))
                        + func.sin(
                            func.radians(lat))
                        * func.sin(
                            func.radians(field_alias2.value)))))
                self.query = self.query.add_columns(distance)
                self.query = self.query.join(field_alias1)
                self.query = self.query.join(field_alias2)
                self.query = self.query.having(
                        u'greatcircle < %s' % radius).order_by('greatcircle')
                expression = and_(field_alias1.name == u'geo.long',
                        field_alias2.name == u'geo.lat')
                self.limit = 20  # XXX: make this passable
            elif fieldname == '_limit':
                try:
                    self.limit = int(value)
                except ValueError:
                    pass
                self.query = self.query.order_by(
                        sRevision.modified.desc())
                expression = None
            elif fieldname == 'text':
                if not self.joined_text:
                    self.query = self.query.join(sText)
                    self.joined_text = True
                if self.fulltext:
                    expression = (text_(
                        'MATCH(text.text) '
                        + "AGAINST('%s' in boolean mode)" % value))
                else:
                    value = '%' + value + '%'
                    expression = sText.text.like(value)
            elif fieldname in ['modifier', 'modified', 'type']:
                if like:
                    expression = (getattr(sRevision,
                        fieldname).like(value))
                else:
                    expression = (getattr(sRevision,
                        fieldname) == value)
            else:
                if self.in_and:
                    field_alias = aliased(sField)
                    self.query = self.query.join(field_alias)
                    expression = (field_alias.name == fieldname)
                    if like:
                        expression = and_(expression,
                                field_alias.value.like(value))
                    else:
                        expression = and_(expression,
                                field_alias.value == value)
                else:
                    if not self.joined_fields:
                        self.query = self.query.join(sField)
                        expression = (sField.name == fieldname)
                        if like:
                            expression = and_(expression,
                                    sField.value.like(value))
                        else:
                            expression = and_(expression,
                                    sField.value == value)
                        self.joined_fields = True
                    else:
                        expression = (sField.name == fieldname)
                        if like:
                            expression = and_(expression,
                                    sField.value.like(value))
                        else:
                            expression = and_(expression,
                                    sField.value == value)
        else:
            if not self.joined_text:
                self.query = self.query.join(sText)
                self.joined_text = True
            if self.fulltext:
                expression = (text_(
                    'MATCH(text.text) '
                    + "AGAINST('%s' in boolean mode)" % value))
            else:
                value = '%' + value + '%'
                expression = sText.text.like(value)
        return expression

    def _Field(self, node, fieldname):
        return self._Word(node[1], node[0])

    def _Group(self, node, fieldname):
        expressions = []
        for subnode in node:
            expressions.append(self._eval(subnode, fieldname))
        return and_(*expressions)

    def _Or(self, node, fieldname):
        expressions = []
        self.in_or = True
        for subnode in node:
            expressions.append(self._eval(subnode, fieldname))
        self.in_or = False
        return or_(*expressions)

    def _And(self, node, fieldname):
        expressions = []
        self.in_and = True
        for subnode in node:
            expressions.append(self._eval(subnode, fieldname))
        self.in_and = False
        return and_(*expressions)

    def _Not(self, node, fieldname):
        expressions = []
        self.in_not = True
        for subnode in node:
            expressions.append(self._eval(subnode, fieldname))
        self.in_not = False
        return not_(*expressions)

    def _Quotes(self, node, fieldname):
        node[0] = '"%s"' % node[0]
        return self._Word(node, fieldname)
