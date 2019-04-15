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

"""
Example Airflow DAG that creates and deletes Bigquery data transfer configurations.
"""
import os

from google.protobuf.struct_pb2 import Struct, Value

import airflow
from airflow import models
from airflow.contrib.operators.gcp_bigquery_dts_operator import (
    BigQueryCreateDataTransferOperator,
    BigQueryDeleteDataTransferConfigOperator,
)
from airflow.operators.bash_operator import BashOperator

# [START howto_bigquery_dts_os_args]
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
# [END howto_bigquery_dts_os_args]

# [START howto_bigquery_dts_create_args]
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
# [END howto_bigquery_dts_create_args]

default_args = {'start_date': airflow.utils.dates.days_ago(1)}

with models.DAG(
    'example_gcp_bigquery_dts',
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
) as dag:
    # [START howto_bigquery_create_data_transfer]
    gcp_bigquery_create_transfer = BigQueryCreateDataTransferOperator(
        transfer_config=TRANSFER_CONFIG, project_id=GCP_PROJECT_ID, task_id='gcp_bigquery_create_transfer'
    )
    # [END howto_bigquery_create_data_transfer]

    # [START howto_bigquery_create_data_transfer_result]
    gcp_bigquery_create_transfer_result = BashOperator(
        bash_command="echo {{task_instance.xcom_pull('gcp_bigquery_create_transfer')}}",
        task_id="gcp_bigquery_create_transfer_result",
    )
    # [END howto_bigquery_create_data_transfer_result]

    # [START howto_bigquery_delete_data_transfer]
    gcp_bigquery_delete_transfer = BigQueryDeleteDataTransferConfigOperator(
        name="{{ task_instance.xcom_pull('gcp_bigquery_create_transfer')['name'] }}",
        task_id='gcp_bigquery_delete_transfer',
    )
    # [END howto_bigquery_delete_data_transfer]

    gcp_bigquery_create_transfer >> gcp_bigquery_create_transfer_result >> gcp_bigquery_delete_transfer
