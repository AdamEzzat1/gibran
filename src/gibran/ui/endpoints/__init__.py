"""Per-endpoint modules for the FastAPI UI.

Each module exposes a `register(app)` function that attaches its
routes to the passed FastAPI instance. The server's `_wire_endpoints`
calls each registrar exactly once at app construction.
"""
