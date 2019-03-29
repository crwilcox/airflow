import logging

import httplib2
import google.auth
import google_auth_httplib2
import google.oauth2.service_account
import os
import tempfile

import tenacity

from google.api_core.exceptions import Forbidden, ResourceExhausted
from google.api_core import retry
from google.cloud import translate_v2
from google.cloud import vision


# Setup env
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Setup test cases
_DEFAULT_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)

credentials, project = google.auth.default(scopes=_DEFAULT_SCOPES)
http = httplib2.Http()
authed_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http)

client_annotator = vision.ImageAnnotatorClient(credentials=credentials)
client_translate = translate_v2.Client(credentials=credentials)

def test_annotate_image():
    response = client_annotator.annotate_image(
        request={
            'image': {'source': {'image_uri': 'gs://gcp_vision_refernece_image/logo.png'}},
            'features': [{'type': vision.enums.Feature.Type.LOGO_DETECTION}],
        }
    )
    return response.logo_annotations[0].description


def test_translate():
    TEXT = """
    Airflow is a platform to programmatically author, schedule and monitor workflows.
    """

    response = client_translate.translate(TEXT, target_language="PL")['translatedText']
    return response


TEST_FNS= [
    test_annotate_image,
    test_translate
]

# Setup retry mechanism
INVALID_KEYS = [
    'DefaultRequestsPerMinutePerProject',
    'DefaultRequestsPerMinutePerProject'
]
INVALID_REASONS = [
    'userRateLimitExceeded',
]

class retry_if_temporary_quota(tenacity.retry_if_exception):
    """Retries if there was an exception for exceeding the temporary quota."""

    def __init__(self):
        def is_soft_quota_exception(exception):
            logger.debug("is_soft_quota_exception: type: %s", type(exception))
            logger.debug("is_soft_quota_exception: str: %s", str(exception))
            if isinstance(exception, Forbidden):
                logger.debug("Variant A")
                return any(
                    reason in error["reason"]
                    for reason in INVALID_REASONS
                    for error in exception.errors
                )

            if isinstance(exception, ResourceExhausted):
                logger.debug("Variant B")
                return any(
                    key in error.details()
                    for key in INVALID_KEYS
                    for error in exception.errors
                )

            return False
        super(retry_if_temporary_quota, self).__init__(is_soft_quota_exception)


# Test code
for fn in TEST_FNS:
    for i in range(15):
        print("I =", i)
        try:

            @tenacity.retry(
                wait=tenacity.wait_exponential(multiplier=1, max=100),
                retry=retry_if_temporary_quota(),
                before=tenacity.before_log(logger, logging.DEBUG),
                after=tenacity.after_log(logger, logging.DEBUG),
            )
            def wrapped_fn():
                r = fn()
                logger.info("Result: %s", r)

            wrapped_fn()
            print("Success")
        except Exception as e:
            import ipdb; ipdb.set_trace()
            print(e)
