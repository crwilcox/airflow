#!/usr/bin/env python
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
import csv
import os

import argparse
from tempfile import NamedTemporaryFile

from tests.contrib.utils.gcp_authenticator import GcpAuthenticator, GCP_BIGQUERY_KEY
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_BQ_DATASET = os.environ.get('GCP_BQ_DATASET', 'example_dataset')
GCP_BQ_TABLE = os.environ.get('GCP_BQ_TABLE', 'example_table')

CSV_BUCKET_NAME = os.environ.get('GPC_BQ_BUCKET', 'example-bigquery-bucket')
CSV_FILE_NAME = os.environ.get('GCP_BQ_CSV_FILENAME', 'example.csv')

CSV_CONTENT = [{'col_a': "example1", 'col_b': "example2"}, {'col_a': "example1", 'col_b': "example2"}]


class GcpBigqueryDtsTestHelper(LoggingCommandExecutor):
    def create_csv_file(self):
        self.execute_cmd(
            [
                'gsutil',
                'mb',
                "-p",
                GCP_PROJECT_ID,
                "-c",
                "regional",
                "-l",
                "europe-north1",
                "gs://{}".format(CSV_BUCKET_NAME),
            ]
        )

        with NamedTemporaryFile(suffix=".csv", mode='w', newline='') as file:
            fieldnames = list(CSV_CONTENT[0].keys())
            csv_writer = csv.DictWriter(file, fieldnames=fieldnames)
            csv_writer.writeheader()
            csv_writer.writerows(CSV_CONTENT)
            file.flush()
            self.execute_cmd(['gsutil', 'cp', file.name, "gs://{}/{}".format(CSV_BUCKET_NAME, CSV_FILE_NAME)])

    def create_dataset(self):
        columns = list(CSV_CONTENT[0].keys())
        dataset = '{}:{}'.format(GCP_PROJECT_ID, GCP_BQ_DATASET)
        table = '{}.{}'.format(GCP_BQ_DATASET, GCP_BQ_TABLE)
        schema = ":STRING,".join(columns) + ":STRING"

        self.execute_cmd(['bq', 'mk', dataset])
        self.execute_cmd(["bq", "mk", "--table", table, schema])

    def delete_csv_file(self):
        self.execute_cmd(['gsutil', 'rm', '-r', "gs://{}".format(CSV_BUCKET_NAME)])

    def delete_dataset(self):
        self.execute_cmd(['bq', 'rm', '-r', '-f', '{}:{}'.format(GCP_PROJECT_ID, GCP_BQ_DATASET)])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create or delete spanner instances for system tests.')
    parser.add_argument(
        '--action',
        dest='action',
        required=True,
        choices=(
            'create_csv_file',
            'create_dataset',
            'delete_csv_file',
            'delete_dataset',
            'before-tests',
            'after-tests',
        ),
    )
    action = parser.parse_args().action

    helper = GcpBigqueryDtsTestHelper()
    gcp_authenticator = GcpAuthenticator(GCP_BIGQUERY_KEY)
    helper.log.info('Starting action: {}'.format(action))

    gcp_authenticator.gcp_store_authentication()
    try:
        gcp_authenticator.gcp_authenticate()
        if action == 'before-tests':
            pass
        elif action == 'after-tests':
            pass
        elif action == 'create_csv_file':
            helper.create_csv_file()
        elif action == 'create_dataset':
            helper.create_dataset()
        elif action == 'delete_csv_file':
            helper.delete_csv_file()
        elif action == 'delete_dataset':
            helper.delete_dataset()
        else:
            raise Exception("Unknown action: {}".format(action))
    finally:
        gcp_authenticator.gcp_restore_authentication()

    helper.log.info('Finishing action: {}'.format(action))
