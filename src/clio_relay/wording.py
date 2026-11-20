"""Small wording helpers for text a person reads."""

from __future__ import annotations

# Names read letter by letter, so their article follows the first letter's sound.
_SPELLED = frozenset(
    {"ssh", "frp", "iri", "scp", "sftp", "http", "https", "mfa", "sso", "oidc", "mpi", "hpc"}
)
_VOWEL_SOUNDING_LETTERS = frozenset("aefhilmnorsx")


def with_article(name: str) -> str:
    """Return ``name`` after "a" or "an", as it is said: "an ssh", "a Globus", "an IRI"."""
    word = name.strip()
    head = word.split(":", 1)[0].lower()
    letters = _VOWEL_SOUNDING_LETTERS if head in _SPELLED else frozenset("aeiou")
    vowel = head[:1] in letters
    return f"{'an' if vowel else 'a'} {word}"
