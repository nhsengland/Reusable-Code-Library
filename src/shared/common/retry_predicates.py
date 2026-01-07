from botocore.exceptions import ClientError


def s3_throttle_retry_predicate(err: Exception) -> bool:
    if not isinstance(err, ClientError):
        return False

    return err.response.get('Error', {}).get('Code', 'Unknown') == 'SlowDown'
