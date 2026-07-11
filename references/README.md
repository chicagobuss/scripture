# Local prior-art checkouts

The complete SlateDB/OpenData repository is cloned locally at
`references/opendata/` for prior-art study. It contains the Buffer and Log
projects as well as their shared infrastructure, RFCs, and operational
context.

- Upstream: https://github.com/opendata-oss/opendata
- Checkout used for this study: `b14d86b6e62f0c44bddd82029b3e758f9fec2db9`
- Relevant paths: `buffer/`, `log/`, `common/`, and the repository-level `rfcs/`

The checkout is intentionally ignored by Scripture's Git repository. Refresh
it independently when studying upstream changes; do not copy its code into
the product without an explicit license/provenance review.
