# owl-db

**owl-db** is a lightweight, high-performance NoSQL database built on top of [Badger](https://github.com/dgraph-io/badger), a fast key-value store in Go, with BSON (Binary JSON) serialization inspired by MongoDB. This database is designed to offer a flexible and scalable solution for managing document-like data structures.

---

## Features

- **Badger-backed Storage**: Uses Badger as the underlying key-value store for fast disk-based operations.
- **BSON Serialization**: Stores documents in BSON format, making it ideal for applications familiar with MongoDB's data model.
- **MongoDB-inspired**: owl-db offers MongoDB-like capabilities, including querying, indexing, and managing documents with flexibility.
- **In-memory Indexes**: Efficient indexing system for fast querying and data retrieval.
- **Concurrency Support**: Manages concurrent read and write operations using Badger transactions.
- **Lightweight & Embedded**: Designed to be embedded into your Go applications with minimal overhead.

---

## Status
It's still very work in progress
