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

from tests.contrib.utils.base_gcp_mock import mock_base_gcp_hook_no_default_project_id
from airflow.contrib.hooks.gcp_bigquery_dts_hook import BiqQueryDataTransferServiceHook
from tests.compat import mock


PROJECT_ID = 'id'
TRANSFER_CONFIG = {"test_transfer_config": "mocked"}
NAME = 'projects/123abc/locations/321cba/transferConfig/1a2b3c'


class BigQueryDataTransferHookTestCase(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__',
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.bigquery_dts_hook = BiqQueryDataTransferServiceHook(gcp_conn_id='test')

    @mock.patch('airflow.contrib.hooks.gcp_bigquery_dts_hook.BiqQueryDataTransferServiceHook.get_conn')
    def test_create_transfer_config(self, get_conn_mock):
        mocked_method = get_conn_mock.return_value.create_transfer_config

        self.bigquery_dts_hook.create_transfer_config(transfer_config=TRANSFER_CONFIG, project_id=PROJECT_ID)

        get_conn_mock.assert_called_once_with()
        mocked_method.assert_called_once_with(
            parent='projects/{}'.format(PROJECT_ID),
            transfer_config=TRANSFER_CONFIG,
            authorization_code=None,
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch('airflow.contrib.hooks.gcp_bigquery_dts_hook.BiqQueryDataTransferServiceHook.get_conn')
    def test_delete_transfer_config(self, get_conn_mock):
        mocked_method = get_conn_mock.return_value.delete_transfer_config

        self.bigquery_dts_hook.delete_transfer_config(name=NAME)

        get_conn_mock.assert_called_once_with()
        mocked_method.assert_called_once_with(name=NAME, metadata=None, retry=None, timeout=None)
