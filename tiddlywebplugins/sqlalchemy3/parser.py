"""
A search query string parser that generates an ast to be used elsewhere
to create an appropriate SQL query.
"""

from pyparsing import (printables, alphanums, OneOrMore, Group,
        Combine, Suppress, Literal, CharsNotIn,
        Word, Keyword, Empty, White, Forward, QuotedString, StringEnd)


def _make_default_parser():
    """
    Define a search query grammar that basically amounts to:

    term OR quoted terms OR fieldname:value OR fieldname:quoted value

    with optional booleans between chunks.

    Then return a parser for that grammar.

    Borrowed from early Whoosh versions
    """
    escapechar = "\\"

    wordtext = CharsNotIn('\\():"{}[] ')
    escape = Suppress(escapechar) + (Word(printables, exact=1)
            | White(exact=1))
    wordtoken = Combine(OneOrMore(wordtext | escape))
# A plain old word.
    plainWord = Group(wordtoken).setResultsName("Word")

# A range of terms
    startfence = Literal("[") | Literal("{")
    endfence = Literal("]") | Literal("}")
    rangeitem = QuotedString('"') | wordtoken
    openstartrange = Group(Empty()) + Suppress(Keyword("TO")
            + White()) + Group(rangeitem)
    openendrange = Group(rangeitem) + Suppress(White()
            + Keyword("TO")) + Group(Empty())
    normalrange = Group(rangeitem) + Suppress(White()
            + Keyword("TO") + White()) + Group(rangeitem)
    range = Group(startfence + (normalrange | openstartrange
        | openendrange) + endfence).setResultsName("Range")

# A word-like thing
    generalWord = range | plainWord

# A quoted phrase
    quotedPhrase = Group(QuotedString('"')).setResultsName("Quotes")

    expression = Forward()

# Parentheses can enclose (group) any expression
    parenthetical = Group((Suppress("(") + expression
        + Suppress(")"))).setResultsName("Group")

    boostableUnit = generalWord | quotedPhrase
    boostedUnit = Group(boostableUnit + Suppress("^")
            + Word("0123456789", ".0123456789")).setResultsName("Boost")

# The user can flag that a parenthetical group, quoted phrase, or word
# should be searched in a particular field by prepending 'fn:', where fn is
# the name of the field.
    fieldableUnit = parenthetical | boostedUnit | boostableUnit
    fieldedUnit = Group(Word(alphanums + "_" + "-"
        + ".") + Suppress(':') + fieldableUnit).setResultsName("Field")

# Units of content
    generalUnit = fieldedUnit | fieldableUnit

    andToken = Keyword("AND", caseless=False)
    orToken = Keyword("OR", caseless=False)
    notToken = Keyword("NOT", caseless=False)

    operatorAnd = Group(generalUnit + OneOrMore(
        Suppress(White()) + Suppress(andToken) + Suppress(White())
        + generalUnit)).setResultsName("And")
    operatorOr = Group(generalUnit + OneOrMore(
        Suppress(White()) + Suppress(orToken) + Suppress(White())
        + generalUnit)).setResultsName("Or")
    operatorNot = Group(Suppress(notToken) + Suppress(White()) +
        generalUnit).setResultsName("Not")

    expression << (OneOrMore(operatorAnd | operatorOr | operatorNot
        | generalUnit | Suppress(White())) | Empty())

    toplevel = Group(expression).setResultsName("Toplevel") + StringEnd()

    return toplevel.parseString


DEFAULT_PARSER = _make_default_parser()
