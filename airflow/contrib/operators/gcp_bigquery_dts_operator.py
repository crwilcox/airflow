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
from google.protobuf.json_format import MessageToDict

from airflow.contrib.hooks.gcp_bigquery_dts_hook import BiqQueryDataTransferServiceHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BigQueryCreateDataTransferOperator(BaseOperator):
    """
    Creates a new data transfer configuration.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCreateDataTransferOperator`

    :param project_id: (Optional) The project in which the Product is located. If set to None or
        missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param transfer_config: Data transfer configuration to create.
    :type transfer_config: dict or google.cloud.bigquery_datatransfer_v1.types.TransferConfig
    :param authorization_code: (Optional) authorization code to use with this transfer configuration.
        This is required if new credentials are needed.
    :type authorization_code: str
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    # [START bigquery_dts_create_transfer_config_template_fields]
    template_fields = ('project_id', 'gcp_conn_id')
    # [END bigquery_dts_create_transfer_config_template_fields]

    @apply_defaults
    def __init__(
        self,
        transfer_config,
        authorization_code=None,
        project_id=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs
    ):
        super(BigQueryCreateDataTransferOperator, self).__init__(*args, **kwargs)
        self.transfer_config = transfer_config
        self.authorization_code = authorization_code
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = BiqQueryDataTransferServiceHook(gcp_conn_id=self.gcp_conn_id)
        response = hook.create_transfer_config(
            project_id=self.project_id,
            transfer_config=self.transfer_config,
            authorization_code=self.authorization_code,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(response)


class BigQueryDeleteDataTransferConfigOperator(BaseOperator):
    """
    Deletes transfer configuration.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryDeleteDataTransferConfigOperator`

    :param name: The name of data transfer configuration to be deleted.
    :type name: str
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    # [START bigquery_dts_delete_transfer_config_template_fields]
    template_fields = ('name', 'gcp_conn_id')
    # [END bigquery_dts_delete_transfer_config_template_fields]

    @apply_defaults
    def __init__(
        self,
        name,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs
    ):
        super(BigQueryDeleteDataTransferConfigOperator, self).__init__(*args, **kwargs)
        self.name = name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = BiqQueryDataTransferServiceHook(gcp_conn_id=self.gcp_conn_id)
        hook.delete_transfer_config(
            name=self.name, retry=self.retry, timeout=self.timeout, metadata=self.metadata
        )
