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

from google.cloud.bigquery_datatransfer_v1.proto.transfer_pb2 import TransferConfig
from google.protobuf.struct_pb2 import Struct, Value
from tests.compat import mock

from airflow.contrib.operators.gcp_bigquery_dts_operator import (
    BigQueryCreateDataTransferOperator,
    BigQueryDeleteDataTransferConfigOperator,
)


PROJECT_ID = 'id'

PARAMS = Struct(
    fields={
        'destination_table_name_template': Value(string_value="example_table"),
        'data_path_template': Value(string_value="gs://example-bigquery-bucket/example.csv"),
        'field_delimiter': Value(string_value=","),
        'file_format': Value(string_value="CSV"),
        'max_bad_records': Value(string_value="0"),
        'skip_leading_rows': Value(string_value="0"),
    }
)

TRANSFER_CONFIG = {
    "data_source_id": "google_cloud_storage",
    "destination_dataset_id": "example_dataset",
    "params": PARAMS,
    "display_name": "example-transfer",
    "disabled": False,
    "data_refresh_window_days": 0,
    "schedule": "first sunday of quarter 00:00",
}

NAME = 'projects/123abc/locations/321cba/transferConfig/1a2b3c'


class BigQueryDataTransferOperatorsTestCase(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_bigquery_dts_operator.BiqQueryDataTransferServiceHook')
    def test_create_data_transfer_operator_greenpath(self, mock_hook):
        mock_hook.return_value.create_transfer_config.return_value = TransferConfig()
        op = BigQueryCreateDataTransferOperator(
            transfer_config=TRANSFER_CONFIG, project_id=PROJECT_ID, task_id='id'
        )
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id='google_cloud_default')
        mock_hook.return_value.create_transfer_config.assert_called_once_with(
            authorization_code=None,
            metadata=None,
            transfer_config=TRANSFER_CONFIG,
            project_id=PROJECT_ID,
            retry=None,
            timeout=None,
        )

    @mock.patch('airflow.contrib.operators.gcp_bigquery_dts_operator.BiqQueryDataTransferServiceHook')
    def test_delete_data_transfer_operator_greenpath(self, mock_hook):
        mock_hook.return_value.delete_transfer_config.return_value = True
        op = BigQueryDeleteDataTransferConfigOperator(name=NAME, task_id='id')
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id='google_cloud_default')
        mock_hook.return_value.delete_transfer_config.assert_called_once_with(
            metadata=None, name=NAME, retry=None, timeout=None
        )
