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
import os
import unittest
try:
    # noinspection PyProtectedMember
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

from parameterized import parameterized

from airflow import models, AirflowException, example_dags, DAG


class ExampleDagsTestCase(unittest.TestCase):

    @parameterized.expand([
        ("example_bash_operator",),
        ('example_branch_operator', ),
        ('example_branch_dop_operator_v3', ),
        ('example_default_args', ),
        ('foo_1', ),
        ('foo_10', ),
        ('example_http_operator', ),
        ('example_latest_only', ),
        ('example_latest_only_with_trigger', ),
        ('example_lineage', ),
        ('example_passing_params_via_test_command', ),
        ('example_python_operator', ),
        ('example_short_circuit_operator', ),
        ('example_skip_dag', ),
        ('example_subdag_operator', ),
        ('example_trigger_controller', ),
        ('example_trigger_target_dag', ),
        ('example_xcom', ),
    ])
    def test_run_dag(self, dag_id):
        dag_id = 'example_default_args'
        dag_folder = example_dags.__path__[0]
        dag_bag = models.DagBag(dag_folder=dag_folder, include_examples=False)
        dag = dag_bag.get_dag(dag_id)
        if dag is None:
            raise AirflowException(
                "The Dag {dag_id} could not be found. It's either an import problem or the dag was not "
                "symlinked to the DAGs folder. The content of the {dag_folder} folder is {dags_list}".format(
                     dag_id=dag_id, dag_folder=dag_folder, dags_list=os.listdir(dag_folder)
                )
            )
        dag.clear(reset_dag_runs=True)
        dag.run(ignore_first_depends_on_past=True, verbose=True)


class ExampleDynamicDagTestCase(unittest.TestCase):

    def test_example(self):
        with mock.patch.dict('sys.modules'):
            import airflow.example_dags.example_dynamic_dag
            for i in range(10):
                dag = getattr(airflow.example_dags.example_dynamic_dag, 'foo_{}'.format(i))
                self.assertIsInstance(dag, DAG)
