import unittest

import mock

from airflow.contrib.hooks.gcp_api_base_hook import CloudQuotaSystemTestMixin, GoogleCloudBaseHook
from google.cloud import translate

from airflow.contrib.hooks.gcp_translate_hook import CloudTranslateHook
from tests.contrib.utils.base_gcp_mock import mock_base_gcp_hook_no_default_project_id
from tests.contrib.utils.gcp_authenticator import GcpAuthenticator, GCP_AI_KEY

TEXT = """
Airflow is a platform to programmatically author, schedule and monitor workflows.
Airflow is a platform to programmatically author, schedule and monitor workflows.
Airflow is a platform to programmatically author, schedule and monitor workflows.
"""


class CloudTranslateQuotaSystemTestCase(unittest.TestCase, CloudQuotaSystemTestMixin):
    @classmethod
    @GoogleCloudBaseHook._Decorators.provide_gcp_credential_file(key_path=GcpAuthenticator(GCP_AI_KEY).full_key_path)
    @mock.patch(
        'airflow.contrib.hooks.gcp_translate_hook.CloudTranslateHook.__init__',
        new=mock_base_gcp_hook_no_default_project_id,
    )
    def setUpClass(cls):
        cls.client = translate.Client()
        cls.hook = CloudTranslateHook(gcp_conn_id='test')

    def do_request(self):
        response = self.hook.translate(TEXT, target_language="PL")['translatedText']
        return response

    @classmethod
    def tearDownClass(cls):
        cls.client = None
