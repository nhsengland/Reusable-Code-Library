"""Tools for handling resource URIs."""

# ruff: noqa: F401
from dve.parser.file_handling.helpers import NonClosingTextIOWrapper, parse_uri
from dve.parser.file_handling.log_handler import ResourceHandler
from dve.parser.file_handling.service import (
    add_implementation,
    build_relative_uri,
    copy_prefix,
    copy_resource,
    create_directory,
    file_uri_to_local_path,
    get_content_length,
    get_file_name,
    get_file_stem,
    get_file_suffix,
    get_parent,
    get_resource_digest,
    get_resource_exists,
    is_supported,
    iter_prefix,
    joinuri,
    move_prefix,
    move_resource,
    open_stream,
    remove_prefix,
    remove_resource,
    resolve_location,
    scheme_is_supported,
)
from dve.parser.file_handling.utilities import TemporaryPrefix
