..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Google Cloud Bigquery Data Transfer Operators
=============================================

.. contents::
  :depth: 1
  :local:

.. _howto/operator:BigQueryCreateDataTransferOperator:

BigQueryCreateDataTransferOperator
----------------------------------

Creates a new data transfer configuration.

For parameter definition, take a look at
:class:`airflow.contrib.operators.gcp_bigquery_dts_operator.BigQueryCreateDataTransferOperator`

Arguments
"""""""""
You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used.
Project id argument in the example DAG is taken from the OS environment variables:

.. literalinclude:: ../../../../airflow/contrib/example_dags/example_gcp_bigquery_dts.py
      :language: python
      :start-after: [START howto_bigquery_dts_os_args]
      :end-before: [END howto_bigquery_dts_os_args]

For transfer_config argument you can use google.cloud.bigquery_datatransfer_v1.types.TransferConfig object
or corresponding dict.

.. literalinclude:: ../../../../airflow/contrib/example_dags/example_gcp_bigquery_dts.py
      :language: python
      :start-after: [START howto_bigquery_dts_create_args]
      :end-before: [END howto_bigquery_dts_create_args]

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. literalinclude:: ../../../../airflow/contrib/example_dags/example_gcp_bigquery_dts.py
      :language: python
      :dedent: 4
      :start-after: [START howto_bigquery_create_data_transfer]
      :end-before: [END howto_bigquery_create_data_transfer]

The result of translation is available as dictionary or array of dictionaries accessible via the usual
XCom mechanisms of Airflow:

.. literalinclude:: ../../../../airflow/contrib/example_dags/example_gcp_bigquery_dts.py
      :language: python
      :dedent: 4
      :start-after: [START howto_bigquery_create_data_transfer_result]
      :end-before: [END howto_bigquery_create_data_transfer_result]


Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_bigquery_dts_operator.py
    :language: python
    :dedent: 4
    :start-after: [START bigquery_dts_create_transfer_config_template_fields]
    :end-before: [END bigquery_dts_create_transfer_config_template_fields]

More information
""""""""""""""""

See `Google Cloud Bigquery create transfer config documentation
<https://googleapis.github.io/google-cloud-python/latest/bigquery_datatransfer/gapic/v1/api.html#google.cloud.bigquery_datatransfer_v1.DataTransferServiceClient.create_transfer_config>`_.


.. _howto/operator:BigQueryDeleteDataTransferConfigOperator:

BigQueryDeleteDataTransferConfigOperator
----------------------------------------

Deletes data transfer configuration.

For parameter definition, take a look at
:class:`airflow.contrib.operators.gcp_bigquery_dts_operator.BigQueryDeleteDataTransferConfigOperator`

Arguments
"""""""""

Name argument is a string with transfer config name to delete. It has to be in the format:
'projects/{project_id}/locations/{location_id}/transferConfigs/{config_id}'

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. literalinclude:: ../../../../airflow/contrib/example_dags/example_gcp_bigquery_dts.py
      :language: python
      :dedent: 4
      :start-after: [START howto_bigquery_delete_data_transfer]
      :end-before: [END howto_bigquery_delete_data_transfer]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_bigquery_dts_operator.py
    :language: python
    :dedent: 4
    :start-after: [START bigquery_dts_delete_transfer_config_template_fields]
    :end-before: [END bigquery_dts_delete_transfer_config_template_fields]

More information
""""""""""""""""

See `Google Cloud Bigquery delete transfer config documentation
<https://googleapis.github.io/google-cloud-python/latest/bigquery_datatransfer/gapic/v1/api.html#google.cloud.bigquery_datatransfer_v1.DataTransferServiceClient.delete_transfer_config>`_.
