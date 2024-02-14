_ARN_DELIMETER = ':'
_HTTP_SCHEMA = "http://"
_HTTPS_SCHEMA = "https://"

class SqsUrlParser:
    @staticmethod
    def get_sqs_remote_target(sqs_url: str):
        sqs_url = strip_schema_from_url(sqs_url)

        if not is_sqs_url(sqs_url) and not is_legacy_sqs_url(sqs_url) and not is_custom_url(sqs_url):
            return None

        region = get_region(sqs_url)
        account_id = get_account_id(sqs_url)
        partition = get_partition(sqs_url)
        queue_name = get_queue_name(sqs_url)

        remote_target = []

        if all((region, account_id, partition, queue_name)):
            remote_target.append("arn")

        remote_target.extend([
            _ARN_DELIMETER,
            null_to_empty(partition),
            _ARN_DELIMETER,
            "sqs",
            _ARN_DELIMETER,
            null_to_empty(region),
            _ARN_DELIMETER,
            null_to_empty(account_id),
            _ARN_DELIMETER,
            queue_name
        ])

        return ''.join(remote_target)

def strip_schema_from_url(url):
    return url.replace(_HTTP_SCHEMA, "").replace(_HTTPS_SCHEMA, "")

def get_region(sqs_url):
    if sqs_url is None:
        return None

    if sqs_url.startswith("queue.amazonaws.com/"):
        return "us-east-1"
    elif SqsUrlParser.is_sqs_url(sqs_url):
        return SqsUrlParser.get_region_from_sqs_url(sqs_url)
    elif SqsUrlParser.is_legacy_sqs_url(sqs_url):
        return SqsUrlParser.get_region_from_legacy_sqs_url(sqs_url)
    else:
        return None

def is_sqs_url(sqs_url):
    split = sqs_url.split("/")
    return len(split) == 3 and \
           split[0].startswith("sqs.") and \
           split[0].endswith(".amazonaws.com") and \
           SqsUrlParser.is_account_id(split[1]) and \
           SqsUrlParser.is_valid_queue_name(split[2])

def is_legacy_sqs_url(sqs_url):
    split = sqs_url.split("/")
    return len(split) == 3 and \
           split[0].endswith(".queue.amazonaws.com") and \
           SqsUrlParser.is_account_id(split[1]) and \
           SqsUrlParser.is_valid_queue_name(split[2])

def is_custom_url(sqs_url):
    split = sqs_url.split("/")
    return len(split) == 3 and \
           SqsUrlParser.is_account_id(split[1]) and \
           SqsUrlParser.is_valid_queue_name(split[2])

def is_valid_queue_name(input):
    if len(input) == 0 or len(input) > 80:
        return False

    for c in input:
        if c != '_' and c != '-' and not c.isalpha() and not c.isdigit():
            return False

    return True

def is_account_id(input):
    return len(input) == 12 and input.isdigit()

def get_region_from_sqs_url(sqs_url):
    split = sqs_url.split(".")
    return split[1] if len(split) >= 2 else None

def get_region_from_legacy_sqs_url(sqs_url):
    split = sqs_url.split(".")
    return split[0]

def get_account_id(sqs_url):
    if sqs_url is None:
        return None

    split = sqs_url.split("/")
    return split[1] if len(split) >= 2 else None

def get_partition(sqs_url):
    region = SqsUrlParser.get_region(sqs_url)

    if region is None:
        return None

    if region.startswith("us-gov-"):
        return "aws-us-gov"
    elif region.startswith("cn-"):
        return "aws-cn"
    else:
        return "aws"

def get_queue_name(sqs_url):
    split = sqs_url.split("/")
    return split[2] if len(split) >= 3 else None

def null_to_empty(input):
    return input if input is not None else ""
