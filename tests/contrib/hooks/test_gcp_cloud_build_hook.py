import unittest
from unittest import mock

import six

from airflow import AirflowException
from airflow.contrib.hooks.gcp_cloud_build_hook import CloudBuildHook
from tests.contrib.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_CREATE_BODY = {
    "source": {"storageSource": {"bucket": "cloud-build-examples", "object": "node-docker-example.tar.gz"}},
    "steps": [
        {"name": "gcr.io/cloud-builders/docker", "args": ["build", "-t", "gcr.io/$PROJECT_ID/my-image", "."]}
    ],
    "images": ["gcr.io/$PROJECT_ID/my-image"],
}

TEST_BUILD = {"name": "build-name"}
TEST_WAITING_OPERATION = {"done": False, "response": "response"}
TEST_DONE_OPERATION = {"done": True, "response": "response"}
TEST_ERROR_OPERATION = {"done": True, "response": "response", "error": "error"}


class TestCloudBuildHookDefaultProjectId(unittest.TestCase):
    hook = None

    def setUp(self):
        with mock.patch(
            "airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudBuildHook(gcp_conn_id="test")

    @mock.patch('airflow.contrib.hooks.gcp_cloud_build_hook.CloudBuildHook.get_conn')
    def test_minimal_green_path(self, get_conn_mock):
        service_mock = get_conn_mock.return_value

        service_mock.projects.\
            return_value.builds.\
            return_value.create.\
            return_value.execute.\
            return_value = TEST_BUILD

        service_mock.projects. \
            return_value.builds. \
            return_value.get. \
            return_value.execute. \
            return_value = TEST_BUILD

        service_mock.operations.return_value.get.return_value.execute.return_value = TEST_DONE_OPERATION

        result = self.hook.create_build(body={})

        self.assertEquals(result, TEST_BUILD)

    @mock.patch('airflow.contrib.hooks.gcp_cloud_build_hook.CloudBuildHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_cloud_build_hook.time.sleep')
    def test_waiting_operation(self, sleep_mock, get_conn_mock):
        service_mock = get_conn_mock.return_value

        service_mock.projects. \
            return_value.builds. \
            return_value.create. \
            return_value.execute. \
            return_value = TEST_BUILD

        service_mock.projects. \
            return_value.builds. \
            return_value.get. \
            return_value.execute. \
            return_value = TEST_BUILD

        execute_mock = mock.Mock(**{"side_effect": [
            TEST_WAITING_OPERATION,
            TEST_DONE_OPERATION,
            TEST_DONE_OPERATION,
        ]})
        service_mock.operations.return_value.get.return_value.execute = execute_mock

        result = self.hook.create_build(body={})

        self.assertEquals(result, TEST_BUILD)

    @mock.patch('airflow.contrib.hooks.gcp_cloud_build_hook.CloudBuildHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_cloud_build_hook.time.sleep')
    def test_error_operation(self, sleep_mock, get_conn_mock):
        service_mock = get_conn_mock.return_value

        service_mock.projects. \
            return_value.builds. \
            return_value.create. \
            return_value.execute. \
            return_value = TEST_BUILD

        execute_mock = mock.Mock(**{"side_effect": [
            TEST_WAITING_OPERATION,
            TEST_ERROR_OPERATION,
        ]})
        service_mock.operations.return_value.get.return_value.execute = execute_mock
        with six.assertRaisesRegex(self, AirflowException, "error"):
            self.hook.create_build(body={})


class TestGcpComputeHookNoDefaultProjectId(unittest.TestCase):
    hook = None

    def setUp(self):
        with mock.patch(
            "airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudBuildHook(gcp_conn_id="test")


class TestCloudBuildHookNoProjectId:
    hook = None

    def setUp(self):
        with mock.patch(
            "airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudBuildHook(gcp_conn_id="test")
