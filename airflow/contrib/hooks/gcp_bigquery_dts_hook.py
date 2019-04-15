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
#
"""
This module contains a BigQuery Hook, as well as a very basic PEP 249
implementation for BigQuery.
"""

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from google.cloud.bigquery_datatransfer_v1 import DataTransferServiceClient


class BiqQueryDataTransferServiceHook(GoogleCloudBaseHook):
    """
     Hook for Google Bigquery Transfer API.

     All the methods in the hook where project_id is used must be called with
     keyword arguments rather than positional.

     """

    _conn = None

    def __init__(self, gcp_conn_id='google_cloud_default', delegate_to=None):
        super(BiqQueryDataTransferServiceHook, self).__init__(
            gcp_conn_id=gcp_conn_id, delegate_to=delegate_to
        )

    def get_conn(self):
        """
        Retrieves connection to Google Bigquery.

        :return: Google Bigquery API client
        :rtype: google.cloud.bigquery_datatransfer_v1.DataTransferServiceClient
        """
        if not self._conn:
            self._conn = DataTransferServiceClient(credentials=self._get_credentials())
        return self._conn

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_transfer_config(
        self,
        transfer_config,
        project_id=None,
        authorization_code=None,
        retry=None,
        timeout=None,
        metadata=None,
    ):
        """
        Creates a new data transfer configuration.

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
        """

        client = self.get_conn()
        return client.create_transfer_config(
            parent='projects/{}'.format(project_id),
            transfer_config=transfer_config,
            authorization_code=authorization_code,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def delete_transfer_config(self, name, retry=None, timeout=None, metadata=None):
        """
        Creates a new data transfer configuration.

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
        """

        client = self.get_conn()
        return client.delete_transfer_config(name=name, retry=retry, timeout=timeout, metadata=metadata)
