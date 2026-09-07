"""The taxi suite over a Lance dataset — the third engine, reading a third copy.

The Lance dataset is not the Iceberg table. It is a transcode of it, in a
different file format, read by a different reader, and a Lance timing and an
Iceberg timing are therefore not two measurements of the same thing end to end.
The answers, on the other hand, must be identical, and `scripts/compare.py`
holds this leg to exactly the same gate as the other two.
"""
