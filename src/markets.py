"""Active market scope shared by collection and the stdlib dashboard exporter.

Historical country metadata and stored rows remain in config/database tables.
"""

ACTIVE_MARKETS = {"US": "미국", "KR": "한국", "JP": "일본", "VN": "베트남", "IN": "인도", "DE": "독일"}
EXCLUDED_MARKETS = ("CN",)
