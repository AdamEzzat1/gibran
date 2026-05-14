"""Sync layer.

`gibran sync` parses metric/policy/quality YAML, validates ASTs against
the catalog and operator whitelist, and applies via transaction.
Migration runner lives at gibran.sync.migrations."""
