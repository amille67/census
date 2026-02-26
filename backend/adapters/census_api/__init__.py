"""Census API adapter.

Wraps the npappin-wsu/census library (installed as 'census' package).
Uses Census() client for ACS/SF1/PL data access with built-in field
chunking and retry support.
"""
