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
from google.cloud.translate_v2 import Client
from google.cloud import vision


logging.basicConfig(level=logging.DEBUG)

_DEFAULT_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)

credentials, project = google.auth.default(scopes=_DEFAULT_SCOPES)
http = httplib2.Http()
authed_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http)

# client = Client(credentials=credentials)
#
# TEXT = """
# Airflow is a platform to programmatically author, schedule and monitor workflows.
# """
# logger = logging.getLogger(__name__)
#
#
# class retry_if_temporary_quota(tenacity.retry_if_exception):
#     """Retries if there was an exception for exceeding the temporary quota."""
#
#     def __init__(self):
#         def is_soft_quota_exception(exception):
#             if not isinstance(exception, Forbidden):
#                 return False
#             import ipdb
#
#             ipdb.set_trace()
#             reasons = [exception["reason"] for exception in exception.errors]
#             return "userRateLimitExceeded" in reasons
#
#         super(retry_if_temporary_quota, self).__init__(is_soft_quota_exception)
#
#
# for i in range(10):
#     print("I =", i)
#     try:
#
#         @tenacity.retry(
#             wait=tenacity.wait_exponential(multiplier=1, max=100),
#             retry=retry_if_temporary_quota(),
#             before=tenacity.before_log(logger, logging.DEBUG),
#             after=tenacity.after_log(logger, logging.DEBUG),
#         )
#         def fetch():
#             print("start fetch")
#             response = client.translate(TEXT, target_language="PL")['translatedText']
#             print("end fetch")
#
#             return response
#
#         response = fetch()
#         print(response)
#         print("Success")
#     except Forbidden as e:
#         import ipdb
#
#         ipdb.set_trace()
#
#         import ipdb
#
#         ipdb.set_trace()


def retry_any(*predicates):
    """Retries if any of the retries condition is valid."""
    def retry_any_predicate(exception):
        return any(r(exception) for r in predicates)

    return retry_any_predicate


KEYS = [
    'DefaultRequestsPerMinutePerProject',
]


def if_temporary_quota_exception(exception):
    if not isinstance(exception, ResourceExhausted):
        return False

    reasons = [error.details() for error in exception.errors]
    return any(
        key in reason
        for key in KEYS
        for reason in reasons
    )


if_transient_error_or_temporary_quota = retry_any(
    retry.if_transient_error
    # if_temporary_quota_exception,
)

for i in range(30):
    print("I = ", i)
    client = vision.ImageAnnotatorClient(credentials=credentials)
    while True:
        try:
            response = client.annotate_image(
                request={
                    'image': {'source': {'image_uri': 'gs://gcp_vision_refernece_image/logo.png'}},
                    'features': [{'type': vision.enums.Feature.Type.LOGO_DETECTION}],
                },
                # retry=retry.Retry(
                #     # predicate=if_transient_error_or_temporary_quota
                # )
            )
            print("Description: ", response.logo_annotations[0].description)
        except ResourceExhausted as ex:
            print("Exception!!!!!!", ex)
            continue

# def if_temporary_quota_error(exception):
#     """
#     A predicate that checks if an exception is a temporary quota error.
#
#     The following server errors are considered transient:
#
#     -
#     """
#     if isinstance(exception, Forbidden):
