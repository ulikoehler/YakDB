# Inverted index format specification

This document specifies how inverted indices are stored in YakDB tables.

## Data model

Inverted indices, in the sense of this document, are, at the most basic level,
represented by a list of related entities for each token. Entities are identified by arbitrary binary strings.

In YakDB, this model is extended by levels, being represented by user-defined strings. Each level may represent a specific verbosity or group of tokens. The level string must contain only printable ASCII characters.

YakDB is intended to store single-token indices only.
Multi-token requests are handled by computing the intersection of the result for each single token.
Therefore it's essential for the user to maintain a list of stoptokens to avoid large intersection computations.
Multi-token indices are generally faster for a large set of tokens and reduce server load, however they require substantially more disk storage and usually perform worse in terms of caching (because infrequent multi-row searches)
However, YakDB does not impose any restrictions on the token content, so the application can also store a multi-token index
if desired.

YakDB inverted index storage is optimized for interactive prefix-based searches and autocompletion.

If the user desires to group tokens, he shall prepend a user-defined prefix to the token.

## Inverted index key/value specification

This section describes in-row storage of inverted indices.
The index for each token is stored in one row per level.

### Entity ID specification

The list assigned to each (token, level) tuple must consist of binary strings that don't contain "\x00" characters (because they are used as separators)

Entity IDs MUST NOT contain the "\x1E" byte. Entities SHOULD only consist of printable characters.

### Level specification

The level may be any string that does not contains the "\x1E" character in its binary form.

### Key

For both, in-row and multi-row storage, the key format is as follows:

    <level>\x1E<token>

The key format is optimized to improve cache hits and reducte overhead for prefix searches when the level is known.

### Value

The value is the list of related entities, joined by "\x00". The joined string shall not have a trailing "\x00" at the end.

Additionally, any entity ID MAY have a part identifier (separated from the actual entity ID by "\x1E") denoting the part of the document . In case that identifier is empty, no "\x1E" separator SHALL be appended.
