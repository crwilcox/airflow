# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest

import six
from google.api_core.exceptions import Forbidden
import tenacity

from airflow.contrib.hooks.gcp_translate_hook import CloudTranslateHook
from airflow.contrib.hooks.gcp_translate_hook import retry_if_temporary_quota
from airflow.exceptions import AirflowException
from tests.contrib.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class NoForbiddenAfterCount(object):
    """Holds counter state for invoking a method several times in a row."""

    def __init__(self, count, **kwargs):
        self.counter = 0
        self.count = count
        self.kwargs = kwargs

    def __call__(self):
        """Raise an Forbidden until after count threshold has been crossed.
        Then return True.
        """
        if self.counter < self.count:
            self.counter += 1
            raise Forbidden(**self.kwargs)
        return True


@tenacity.retry(retry=retry_if_temporary_quota())
def _retryable_test_with_temporare_quota_retry(thing):
    return thing()


class retry_if_temporary_quotaTestCase(unittest.TestCase):
    def test_do_nothing_on_non_error(self):
        r = _retryable_test_with_temporare_quota_retry(lambda: 42)
        self.assertTrue(r, 42)

    def test_retry_on_exception(self):
        message = "POST https://translation.googleapis.com/language/translate/v2: User Rate Limit Exceeded"
        errors = [
            {
                'message': 'User Rate Limit Exceeded',
                'domain': 'usageLimits',
                'reason': 'userRateLimitExceeded',
            }
        ]
        _retryable_test_with_temporare_quota_retry(NoForbiddenAfterCount(5, message=message, errors=errors))

    def test_raise_exception_on_non_quota_exception(self):
        with six.assertRaisesRegex(self, Forbidden, "Daily Limit Exceeded"):
            message = "POST https://translation.googleapis.com/language/translate/v2: Daily Limit Exceeded"
            errors = [
                {'message': 'Daily Limit Exceeded', 'domain': 'usageLimits', 'reason': 'dailyLimitExceeded'}
            ]

            _retryable_test_with_temporare_quota_retry(
                NoForbiddenAfterCount(5, message=message, errors=errors)
            )


class TestCloudTranslateHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'airflow.contrib.hooks.gcp_translate_hook.CloudTranslateHook.__init__',
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudTranslateHook(gcp_conn_id='test')

    @mock.patch('airflow.contrib.hooks.gcp_translate_hook.CloudTranslateHook.get_conn')
    def test_translate_called(self, get_conn):
        # Given
        translate_method = get_conn.return_value.translate
        translate_method.return_value = {
            'translatedText': 'Yellowing self Gęśle',
            'detectedSourceLanguage': 'pl',
            'model': 'base',
            'input': 'zażółć gęślą jaźń',
        }
        # When
        result = self.hook.translate(
            values=['zażółć gęślą jaźń'],
            target_language='en',
            format_='text',
            source_language=None,
            model='base',
        )
        # Then
        self.assertEqual(
            result,
            {
                'translatedText': 'Yellowing self Gęśle',
                'detectedSourceLanguage': 'pl',
                'model': 'base',
                'input': 'zażółć gęślą jaźń',
            },
        )
        translate_method.assert_called_once_with(
            values=['zażółć gęślą jaźń'],
            target_language='en',
            format_='text',
            source_language=None,
            model='base',
        )
