"""License validation utilities — only open-licensed data should be collected."""

import re

# Accepted license prefixes / identifiers
OPEN_LICENSES = {
    "cc-by",
    "cc-by-sa",
    "cc-by-nc",
    "cc-by-nc-sa",
    "cc-by-nd",
    "cc-by-nc-nd",
    "cc0",
    "cc0-1.0",
    "public domain",
    "odc-by",
    "odc-odbl",
    "odc-pddl",
    "mit",
    "apache-2.0",
    "standard-access",  # QDR: docs are CC BY-SA 4.0, data free for registered users
}


# Regex patterns for common CC license variants → SPDX form.
# Order matters: more specific patterns must come first.
_V = r"(\d+\.\d+)"  # version capture group
_A = r"creative\s+commons\s+attribution[\s-]+"  # CC Attribution prefix
_CC_PATTERNS: list[tuple[re.Pattern, str]] = [
    # Long-form: "Creative Commons Attribution … 4.0"
    (re.compile(_A + r"share\s*alike[\s-]+" + _V, re.I),
     "CC-BY-SA-{v}"),
    (re.compile(_A + r"non\s*commercial[\s-]+no\s*deriv\w*[\s-]+" + _V, re.I),
     "CC-BY-NC-ND-{v}"),
    (re.compile(_A + r"non\s*commercial[\s-]+share\s*alike[\s-]+" + _V, re.I),
     "CC-BY-NC-SA-{v}"),
    (re.compile(_A + r"non\s*commercial[\s-]+" + _V, re.I),
     "CC-BY-NC-{v}"),
    (re.compile(_A + r"no\s*deriv\w*[\s-]+" + _V, re.I),
     "CC-BY-ND-{v}"),
    (re.compile(_A + _V, re.I),
     "CC-BY-{v}"),
    # Short: "CC BY 4.0", "CC-BY-4.0", "CC BY-SA 4.0", etc.
    (re.compile(r"cc[\s-]+by[\s-]+nc[\s-]+nd[\s-]+" + _V, re.I),
     "CC-BY-NC-ND-{v}"),
    (re.compile(r"cc[\s-]+by[\s-]+nc[\s-]+sa[\s-]+" + _V, re.I),
     "CC-BY-NC-SA-{v}"),
    (re.compile(r"cc[\s-]+by[\s-]+nc[\s-]+" + _V, re.I),
     "CC-BY-NC-{v}"),
    (re.compile(r"cc[\s-]+by[\s-]+nd[\s-]+" + _V, re.I),
     "CC-BY-ND-{v}"),
    (re.compile(r"cc[\s-]+by[\s-]+sa[\s-]+" + _V, re.I),
     "CC-BY-SA-{v}"),
    (re.compile(r"cc[\s-]+by[\s-]+" + _V, re.I),
     "CC-BY-{v}"),
    # CC0: "CC0 1.0", "CC0-1.0", "Creative Commons Zero"
    (re.compile(r"cc[\s-]*0[\s-]+" + _V, re.I),
     "CC0-{v}"),
    (re.compile(r"creative\s+commons\s+zero", re.I),
     "CC0-1.0"),
]


def normalize_license(raw: str) -> str:
    """Canonicalize common CC license variants to SPDX format.

    Examples:
        "CC BY 4.0"                                  → "CC-BY-4.0"
        "cc-by-4.0"                                  → "CC-BY-4.0"
        "Creative Commons Attribution 4.0 International" → "CC-BY-4.0"
        "CC0 1.0"                                    → "CC0-1.0"
        "Standard Access"                            → "Standard Access" (unchanged)
    """
    if not raw:
        return raw
    for pattern, template in _CC_PATTERNS:
        m = pattern.search(raw)
        if m:
            version = m.group(1) if m.lastindex else ""
            return template.replace("{v}", version)
    return raw


def is_open_license(license_id: str | None) -> bool:
    """Return True if the license identifier looks like an open license."""
    if not license_id:
        return False
    normalized = license_id.strip().lower().replace(" ", "-").replace("_", "-")
    return any(normalized.startswith(prefix) for prefix in OPEN_LICENSES)
