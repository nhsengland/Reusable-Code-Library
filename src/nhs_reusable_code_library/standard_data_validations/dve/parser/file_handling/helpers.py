"""Helpers for file handling."""

from io import TextIOWrapper
from urllib.parse import urlparse

from dve.parser.type_hints import URI, Hostname, Scheme, URIPath


class NonClosingTextIOWrapper(TextIOWrapper):
    """A TextIOWrapper implementation which detaches instead of closing
    the stream upon exit.

    """

    def __exit__(self, *_):  # pragma: no cover
        """Exits the context and detaches"""
        try:
            self.detach()
        except ValueError:
            # Assume all ValuesErrors are safe to absorb.
            return


def parse_uri(uri: URI) -> tuple[Scheme, Hostname, URIPath]:
    """Parse a URI, yielding the scheme, hostname and URI path."""
    parse_result = urlparse(uri)
    scheme = parse_result.scheme.lower() or "file"  # Assume missing scheme is file URI.

    return scheme, parse_result.hostname, parse_result.path
