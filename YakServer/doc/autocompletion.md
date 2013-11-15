# Autocompletion specification

Revision: 1.0

This document defines how autocompletion datasets are stored in YakDB.

## Definitions

A *tokenizer* is a function that takes a single string as argument and returns
a non-empty list of tokens of the input string.
The tokenizer function shall be idempotent.

An *entity normalizer* is a function that takes a token string and returns a single string.
The purpose of the normalizer is to implement different search modes, e.g. case-insensitive search.

## Entity processing

The autocompletion implementation shall not do any entity processing by default.
It should however provide interfaces to the user to use arbitrary functions for
entity processing and tokenization, if supported by the languange binding.

## Table format

Any data for one set of autocompletion data shall be saved in a single table.

Key: The normalized token
Value: A "\x00"-separated list of unprocessed, untokenized entities this token belongs to.
       No trailing "\x00" shall be inserted.

## Search execution

The autocompletion server

## Autocompletion display recommendation

The following section provides a recommendation for how to display autocompletion suggestions.
Implementing this suggestion is not neccessary for implementation of the protocol.

The user interface shall call the autocompletion server on every keystroke in the input field.
The server shall, upon receipt of the request, execute the aforementioned search algorithm and
return a JSON list of matched entities over HTTP.
The number of returned entities shall be limited by a developer-defined value.

The user interface shall, upon receipt of the server HTTP response, display the list of suggestions underneath the input field. Each suggestion shall handle a click event in a way that either redirects the user to an entity page or replaces the text in the input field by the clicked suggestion.

The UI implementation may provide keystrokes for navigating the suggestion list.

## YakDB embedded HTTP Server integration

The YakDB integrated HTTP server shall offer a reverse-proxy-capable safe, asynchronous, stateless autocompletion server. The implementation details shall be documented in a HTTP API.
The embedded HTTP server implementation shall implement the server characteristics outlined in [Autocompletion display recommendation].

