# ZCDB Graph Model

This document describes the ZCDB graph storage model, version 1.0

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

## Integer format

All integers shall be saved in little-endian format.

## Identifiers

The graph API shall support arbitrary identifiers for nodes and attributes,
as long as they fulfil these requirements:
    - They are not zero-length
    - In their binary representation, all bytes are >= 0x20

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

## Nodes

Nodes or vertices are objects that are identified by an [[Identifier]].

Nodes optionally may have basic and extended attributes.

The API shall provide at least the following functionality for nodes:
    - void addNode(id)
    - void removeNode(id)
    - bool nodeExists(id)
    - dict getNodeBasicAttributes(id)
    - void addNodeBasicAttribute(id, key, value)
    - void removeNodeBasicAttribute(id, key)
    - dict getNodeExtendedAttributes(id)
    - void addNodeExtendedAttribute(id, key, value)
    - void removeNodeExtendedAttribute(id, key)
    - string getNodeExtendedAttribute(id, key)
    - string[] getNodeExtendedAttributeRange(id, from, to)
    - string[] getNodeExtendedAttributeRangeWithLimit(id, from, limit)

### Node serialization format