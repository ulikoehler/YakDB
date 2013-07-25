# ZCDB Graph Model

This document describes the ZCDB graph storage model, version 1.0.
The purpose of this specification is to allow seamless interaction
of different client graph API frontends because of a well-specified
database format

This specification uses requirement levels based on RFC2119

## Graph database

A graph database is a high-level clientside wrapper on top of the
ZCDB key-value store.

The graph objects are represented in different ZCDB key/value tables.
The numeric identifiers of these tables may be specified at runtime,
in order to allow multiple graphs to be stored in the same ZCDB instance.
    - Node table
    - Edge table
    - Node extended attribute table (optional)
    - Edge extended attribute table (optional)

The detailed purpose and format of these tables are described below.

It is not only allowed but encouraged to use the same node table
with different edge tables (and vice versa).

Edges in the edge table that do not correspond to a node in the respective
node table shall be ignored, unless special cleanup operations (that e.g.
delete every edge that does not belong to a node) are explicitly requested.

## String serialization format

Any strings shall be serialized without a terminating NUL character, unless specified otherwise.

## Integer format

All integers shall be saved in little-endian format.

## Identifiers

The graph API shall support arbitrary identifiers for nodes and attributes,
as long as they fulfil these requirements:
    - They are not zero-length
    - In their binary representation, all bytes are >= 0x20

## EBNF grammar specifications

The format of EBNF grammars shall be follow the specification in ISO/IEC 14977.

The following tokens shall be predefined:
    Byte = <Any 8-bit number> ;
    Uint32 = <Any unsigned 32-bit integer, interpreted as little-endian> ;
    ID character = <Any Byte >= 0x20> ;
    Identifier = IDCharacter, {ID character} ;
    NUL delimited identifier = Identifier, '\x00' ;

## Attributes and Extended Attributes

The graph API shall support arbitrary key-value attributes for both nodes and edges.

There shall be two types of attributes:
    - Basic attributes, which are saved in the same table as the entity (as part of the value part of the entry) and
    - Extended attributes, which are saved in a separate tables

Basic attributes are retrieved together with the entity entry and therefore do not
introduce any additional request overhead. However, if they grow too large,
they might impact the node access time. Adding or changing attributes will
cause a full rebuild of the attribute structure.

The attribute set can only be access all-at-once, so large attribute sets
will consume memory proportional to the size of the entire attribute set plus
some overhead.

Extended attributes are stored in a separate table, occupying one key-value-pair
per attribute. Fetching them introduces additional request overhead, but
as they can be accessed in a sequentially ordered manner (and not neccessarily all-at-once)
the extended attributes for any stored entity might grow arbitrarily large.
Retrieval of attributes will not consume memory proportional to the total
amount of key/value data for that entity.

Zero-length attributes are not allowed, as the API always returns a zero-length
value for non-existing values.

For each entity that supports basic attributes, the API shall provide
at least the following functions (in a language-specific manner)
    - dict get<Entity>BasicAttributes(id)
    - void add<Entity>BasicAttribute(id, key, value)
    - void remove<Entity>BasicAttribute(id, key)
    - dict get<Entity>ExtendedAttributes(id)
    - void add<Entity>ExtendedAttribute(id, key, value)
    - void remove<Entity>ExtendedAttribute(id, key)

For each entity that supports extended attributes, the API shall provide
at least the following functions (in a language-specific manner)
    - string getNodeExtendedAttribute(id, key)
    - string[] getNodeExtendedAttributeRange(id, from, to)
    - string[] getNodeExtendedAttributeRangeWithLimit(id, from, limit)

## Basic attribute serialization format

Basic attributes shall be saved as values in the tables of their respective entities

The value shall be defined by the following EBNF grammar:
    KeyValueComposite = NUL delimited Identifier (* Attribute key *), NUL delimited Identifier (* Attribute value *);
    Serialized attribute set = {KeyValueComposite} ;
where *Serialized attribute set* denotes the value that shall be written to the database.

## Extended attribute serialization format

Extended attributes shall be saved in a specific table. There shall be one such
table for each entity that supports extended attributes

The key shall be defined by the following EBNF grammar (where *Entity ID* is the respective entity iden:
    Key = Entity Identifier, '\x1E' (* ASCII Group separator *), Identifier (* Attribute key *)
    Value = Identifier (* Attribute value *)
where *Key* and *Value* denote the key and value of the database entry respectively.

## Nodes

Nodes or vertices are entity that are identified by an [[Identifier]].

Nodes may (but do not need to) have basic and extended attributes.

The API shall provide at least the following functionality for nodes:
    - void addNode(id)
    - void removeNode(id)
    - bool nodeExists(id)
plus the respective functions 

### Node serialization format

The node table shall contain exactly one entry for each node.
The key shall be equivalent to the node identifier ("node ID").
The value shall be equivalent to the value of the serialized
basic attribute set (therefore it is zero-sized if the
basic attribute set is emtpy)




## Edges

Edges are entities that are identified by an [[Identifier]].
