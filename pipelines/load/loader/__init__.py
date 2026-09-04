"""Destination-agnostic load layer: raw sink files -> warehouse RAW schema.

The pieces:

``config``        yml -> typed settings, with per-source overrides
``schema``        JSON records -> canonical column types (no user schema needed)
``discovery``     scan the sink for candidate files
``queue``         filesystem job queue between Airflow and the writer service
``destinations``  the portability seam: one class per warehouse
``service``       the single writer — owns the connection, applies jobs serially
``cli``           operator commands; reach them via ``pipelines/load/bin/loader``
"""

__version__ = "0.1.0"
