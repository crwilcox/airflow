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

# [START faq_dynamic_dag]
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

for i in range(10):
    dag_id = 'foo_{}'.format(i)
    dag = DAG(dag_id=dag_id, start_date=airflow.utils.dates.days_ago(2))

    task = DummyOperator(
        task_id='tqwk_ie', dag=dag
    )

    globals()[dag_id] = dag
    # or better, call a function that returns a DAG object!
# [END faq_dynamic_dag]
