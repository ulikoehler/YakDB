# ZCDB Graph Model

This document describes the ZCDB graph storage model, version 1.0.
The purpose of this specification is to allow seamless interaction
of different client graph API frontends because of a well-specified
database format

This specification uses requirement levels based on RFC2119

## Purpose of the graph model

The graph model was created to provide a perfomant interface
to adjacency-list-based highly-sparse graphs on top of
key-value databases.

Use case classes:
    - T: At most O(log n) time complexity, where n is the total number of entities in the same table
    - t: At most O(m) time complexity, where m is the number of attributes of the relevant entity
    - r: At most O(m+log(n)) time complexity, given n and m from above
    - S: At most O(n) space complexity, where n is the total number of the requested object in the DB
    - s: At most O(1) space complexity (assuming the size of a single attribute is considered constant)

Usecases the model shall be optimized to include, but are not limited to:
    - (Ts) Verifying the existance of a directed edge between two given nodes
    - (T) Writing a single directed edge between two nodes
    - (t) Writing small attribute sets that can be retrieved without additional requests (--> basic attributes)
    - (ts) Writing a large sets of attributes that can be iterated efficiently and requested partially (--> extended attributes)
    - (rS) Reading a set of basic attributes (--> must fit in memory)
    - (rs) Iterating a set of extended attributes (--> do not need to fit into memory)

## Graph database

A graph database is a high-level clientside wrapper on top of the
ZCDB key-value store.

The graph objects are represented in different ZCDB key/value tables.
The numeric identifiers of these tables may be specified at runtime,
in order to allow multiple graphs to be stored in the same ZCDB instance.
    - Node table
    - Edge table
    - Extended attribute

The detailed purpose and format of these tables are described below.

It is allowed to e.g. use the same node table
with different edge tables.

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
    - Extended attributes, which are saved in a separate table

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
the extended attributes for any stored rentity might grow arbitrarily large.
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

Extended attributes shall be saved in a specific table.

The key shall be defined by the following EBNF grammar (where *Entity ID* is the respective entity iden:
    Key = Entity Identifier, '\x1D' (* ASCII Group separator *), Identifier (* Attribute key *)
    Value = {Byte} (* Attribute value *)
where *Key* and *Value* denote the key and value of the database entry respectively.

Note that in contrast to basic attributes, the extended attribute value does not need to fulfill
the identifier constraints, so it can be any binary string.

## Nodes

Nodes or vertices are entity that are uniquely identified by an [[Identifier]].

Nodes may (but do not need to) have basic and extended attributes.

### Node serialization format

The node table shall contain exactly one entry for each node.
The key shall be equivalent to the node identifier ("node ID").
The value in the node table shall be equivalent to the value of the serialized
basic attribute set (therefore it is zero-sized if the
basic attribute set is empty)

### Node API

The API shall provide at least the following functionality for nodes:
    - void addNode(id)
    - void removeNode(id)
    - bool nodeExists(id)
plus the respective functions for basic and extended attributes.
As long as the same functionality is provided, alternative names
or semantics may be used.

## Edges

Edges are entities that are uniquely identified by:
    - A type (may be zero-length)
    - A source node
    - A destination node
    - (Implicitly) a direction

### Ingoing and outgoing edge forms

In order to support fast retrieal of both ingoing and outgoing edges of nodes,
both forms need to be represented in the database (for the same node)

### Edge types

In order to support rapid application development, the graph model supports
optional types for each edge.

In the database, edge types act as a prefix (which is empty on default),
so searching for edges is specific the edge type and not affected by
edges of other types.

For extremely large datasets, it is recommended to consider the option
of using a separate edge table with the same node table.

### Directed and undirected edges

The graph model described in this document supports
only directed edges.

However, the application may consider an edge undirected,
if the same edge exists in both directions.
The graph API should provide a high-level wrapper to write
undirected edges. If any such wrapper is implemented in a
particular graph API, the implementation shall write and update
both edges. If applicable, both edges shall be updated in a single
request to ensure consistence.

Any API implementation for undirected edges may assume both directions
of an undirected edge have been written correctly, so when a read
request for an undirected edge is issued, only one direction
may be read. If any function is implemented in such a way,
the documentation must clearly state the direction that is read first
and provide an indication to the user what happens if an edge is read
that is only provided in one direction.
Additionally, the API may provide a function that allows to check
if both directions of an edge equal.

API developers must consider that 'updating both edges' for undirected
edges effectively yields 4 individual key/value pairs, because
each individual edge must be written in its active and its passive form

### Edge serialization format

The key (= edge identifier) in the edge table is defined by the following EBNF grammar:
    Direction = '\x0E' (* For outgoing edges *) | '\x0F' (* For ingoing edges *) ;
    Edge type = Identifier || '' (* Type identifier *) ;
    Key = Edge type, '\x1F' (* ASCII Unit separator *), Identifier (* Source node *), Direction, Identifier (* Destination node *) ;
The value in the edge table shall be equivalent to the value of the serialized
basic attribute set (therefore it is zero-sized if the
basic attribute set is empty)

Note that per definition, edges without type are prefixed by '\x1F'.
This has been chosen deliberately over omitting the prefix in this case
to avoid increasing implementation complexity.

In any case, both directions of the same edge (ingoing + outgoing) shall be written.
Both directions should be written in the same write batch/transaction.

The ASCII 0xE (Shift Out) and 0xF (Shift In) have been chosen because of the similarity
of their names to the edge directions, even if their original purpose is different.

For extended attributes, only the 0xE (Shift Out) edge version shall be used as key
in order to avoid overhead

Additionally, because of the requirement that identifiers only have characters >= 0x20,
edge identifiers are generally unique in respect to source & destination nodes.

##### Edge identifier examples

For an example, we assume a graph with nodes A,B and C that has the following edges
    - A -> B, default type
    - A -> C, default type
    - C -> B, type "foo"

The API shall, given only this information, write exactly these keys, all with empty values.
Newlines must not be appended at the end of each line.
    - \x1FA\x0EB
    - \x1FB\x0FA
    - \x1FA\x0EC
    - \x1FC\x0FA
    - foo\x1FC\x0EB
    - foo\x1FB\x0FC

### Edge API

The API shall provide at least the following functionality for edges:
    - void addEdge(source, target, type='')
    - void removeEdge(source, target, type='')
    - void edgeExists(source, target, type='')
plus the respective functions for basic and extended attributes.
As long as the same functionality is provided, alternative names
or semantics may be used.

# Usage notes

### Two-table graph

Because edge identifiers always contain the \x1F (ASCII unit separator) character
and node IDs (because they are Identifiers) may not contain this character,
it is possible to use the same table for nodes and edges.

This can have both positive or negative performance impact

The edge scan keys generated by the API must not generate conflicts with node IDs that
are a prefix of the type.

The node scan keys generated by the API must not generate conflicts with edges that
share a common prefix with the node ID.