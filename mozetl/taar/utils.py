import boto3
import json
import logging
import os
import requests
from botocore.exceptions import ClientError
from tempfile import gettempdir


logger = logging.getLogger(__name__)


def fetch_json(uri):
    """ Perform an HTTP GET on the given uri, return the results as json.

    Args:
        uri: the string URI to fetch.

    Returns:
        A JSON object with the response or None if the status code of the
        response is an error code.
    """
    r = requests.get(uri)
    if r.status_code != requests.codes.ok:
        return None

    return r.json()


def get_s3_cache_dir():
    return os.path.join(gettempdir(), 'taar')


def get_s3_cache_filename(s3_bucket, s3_key):
    cache_dir = get_s3_cache_dir()
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    return os.path.join(cache_dir,
                        '_'.join([s3_bucket, s3_key]).replace('/', '_'))


def get_s3_json_content(s3_bucket, s3_key):
    """Download and parse a json file stored on AWS S3.

    The file is downloaded and then cached for future use.
    """
    local_path = get_s3_cache_filename(s3_bucket, s3_key)

    if not os.path.exists(local_path):
        try:
            s3 = boto3.client('s3')
            s3.download_file(s3_bucket, s3_key, local_path)
        except ClientError:
            logger.exception("Failed to download from S3", extra={
                "bucket": s3_bucket,
                "key": s3_key})
            return None

    # It can happen to have corrupted files. Account for the
    # sad reality of life.
    try:
        with open(local_path, 'r') as data:
            return json.loads(data.read())
    except ValueError:
        # Remove the corrupted cache file.
        logging.error("Removing corrupted S3 cache", extra={"cache_path": local_path})
        os.remove(local_path)

    return None
