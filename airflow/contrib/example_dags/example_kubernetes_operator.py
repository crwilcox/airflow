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

from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

args = {"owner": "airflow", "start_date": days_ago(2)}

with DAG(dag_id="example_kubernetes_operator", default_args=args, schedule_interval=None):
    tolerations = [{"key": "key", "operator": "Equal", "value": "value"}]

    k = KubernetesPodOperator(
        namespace="default",
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=False,
        task_id="task",
        get_logs=True,
        is_delete_operator_pod=False,
        tolerations=tolerations,
    )


with DAG(dag_id="example_advaned_kubernetes_operator", default_args=args, schedule_interval=None):
    # [START kubernetes_operator]
    secret_file = Secret("volume", "/etc/sql_conn", "airflow-secrets", "sql_alchemy_conn")
    secret_env = Secret("env", "SQL_CONN", "airflow-secrets", "sql_alchemy_conn")
    volume_mount = VolumeMount("test-volume", mount_path="/root/mount_file", sub_path=None, read_only=True)

    volume_config = {"persistentVolumeClaim": {"claimName": "test-volume"}}
    volume = Volume(name="test-volume", configs=volume_config)

    affinity = {
        "nodeAffinity": {
            "preferredDuringSchedulingIgnoredDuringExecution": [
                {
                    "weight": 1,
                    "preference": {
                        "matchExpressions": {"key": "disktype", "operator": "In", "values": ["ssd"]}
                    },
                }
            ]
        },
        "podAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [
                {
                    "labelSelector": {
                        "matchExpressions": [{"key": "security", "operator": "In", "values": ["S1"]}]
                    },
                    "topologyKey": "failure-domain.beta.kubernetes.io/zone",
                }
            ]
        },
        "podAntiAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [
                {
                    "labelSelector": {
                        "matchExpressions": [{"key": "security", "operator": "In", "values": ["S2"]}]
                    },
                    "topologyKey": "kubernetes.io/hostname",
                }
            ]
        },
    }

    tolerations = [{"key": "key", "operator": "Equal", "value": "value"}]

    k = KubernetesPodOperator(
        namespace="default",
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        secrets=[secret_file, secret_env],
        volumes=[volume],
        volume_mounts=[volume_mount],
        name="test",
        task_id="task",
        affinity=affinity,
        is_delete_operator_pod=True,
        hostnetwork=False,
        tolerations=tolerations,
    )
    # [END kubernetes_operator]
