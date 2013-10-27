# YakDB Dump format specification

## About this document.

This document describes YakDB Dump format (from hereon called YDF) Version 1.0.
Requirement levels shall be understood as defined in RFC2119.

This document is currently a draft and does not neccessarily describe the
finished dump format version 1.0.

## General format description

A YakDB table can be dumped into a YDF file. Any YDF file must contain data for exactly
one table. YDF files must not be entirely empty, but the must contain at least a file header.
The data section of any YDF file may be empty.

Where applicable, the YDF file should use the .ydf file extension.

Any value in the YDF file format shall be stored in little-endian mode.
Binary fields in the specification shall be written to the file in the order they occur in
the specification.

The file format must follow this scheme:
- YDF file header
- Arbitrary number of:
    - YDF key-value header
    - Binary key
    - Binary value

## YDF file header

The file header shall consist of the following fields, in this exact order:

- Magic word, 0x6DDF, 16 bits
- Version word, 0x0001, 16 bits

The file header shall occur exactly once, at file offset 0, if file offsets
are applicable for the application.

## YDF key-value header

The following header shall be prepended to each key-value pair.
Its purpose is to provide the length of the following key and value.

- Magic word, 0x6DE0, 16 bits
- Key length in bytes, 64 bits
- Value length in bytes, 64 bits
