"""License validation utilities — only open-licensed data should be collected."""

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


def is_open_license(license_id: str | None) -> bool:
    """Return True if the license identifier looks like an open license."""
    if not license_id:
        return False
    normalized = license_id.strip().lower().replace(" ", "-").replace("_", "-")
    return any(normalized.startswith(prefix) for prefix in OPEN_LICENSES)
