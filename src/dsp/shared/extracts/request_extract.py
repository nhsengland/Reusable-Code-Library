from typing import Dict, Any

from dsp.shared.extracts.registry import ExtractsRegistry
from dsp.shared.logger import log_action, add_fields
from dsp.shared.store import Stores
from dsp.shared.store.extract_requests import ExtractRequests, create_extract_request


@log_action(log_args=["consumer", "sender", "request", "priority", "extract_type", "test_scope"])
def request_extract(
        sender: str, consumer: str, request: Dict[str, Any],
        priority: int, extract_type: str, test_scope: str = None, stores: Stores = None
) -> int:
    """
    Send an extract request

    Args:
        sender: the identity of the sender
        consumer: the extract consumer e.g. sdscs
        request: the original request
        priority: priority of the extract job
        extract_type: the type of the extract
        test_scope (str): test scope isolation
        stores (Stores): stores for injection

    Returns:
        Extract id of the newly created extract
    """
    extract_request = create_extract_request(
        sender, consumer, extract_type, request, priority=priority, test_scope=test_scope
    )

    extract_processor = ExtractsRegistry.get(extract_type)

    validation_issues = extract_processor.can_execute(extract_request)

    if validation_issues:
        add_fields(validation_issues=validation_issues)
        raise ExtractRequestValidationException(validation_issues)

    stores = stores or Stores({Stores.Names.extract_requests: ExtractRequests})
    extract_request = stores.extract_requests.store_extract_request(extract_request)

    add_fields(extract_id=extract_request.extract_id)

    return extract_request.extract_id


class ExtractRequestValidationException(Exception):
    def __init__(self, issues):
        self.issues = issues
