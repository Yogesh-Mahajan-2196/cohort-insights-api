import re


TAG_PATTERNS = {
    "python": r"\bpython\b",
    "fastapi": r"\bfast[\s-]?api\b",
    "django": r"\bdjango\b",
    "flask": r"\bflask\b",
    "mongodb": r"\bmongo(?:db)?\b",
    "redis": r"\bredis\b",
    "postgresql": r"\bpostgres(?:ql)?\b",
    "mysql": r"\bmysql\b",
    "backend": r"\bbackend\b",
    "api": r"\bapi\b",
    "database": r"\bdatabase\b",
    "web": r"\bweb\b",
    "react": r"\breact(?:js)?\b",
    "javascript": r"\bjavascript\b",
    "typescript": r"\btypescript\b",
    "docker": r"\bdocker\b",
    "kafka": r"\bkafka\b",
}


def extract_tags(content: str, summary: str = "") -> list[str]:
    """
    Extract tags dynamically from document content and summary.

    Matching is case-insensitive and uses word boundaries
    to avoid substring false positives.
    """

    text = f"{content} {summary}".lower()

    tags = []

    for tag, pattern in TAG_PATTERNS.items():
        if re.search(pattern, text, flags=re.IGNORECASE):
            tags.append(tag)

    return tags

