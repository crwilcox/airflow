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

Plugins
=======

Airflow has a simple plugin manager built-in that can integrate external
features to its core by simply dropping files in your
``$AIRFLOW_HOME/plugins`` folder.

The python modules in the ``plugins`` folder get imported,
and **hooks**, **operators**, **sensors**, **macros**, **executors** and web **views**
get integrated to Airflow's main collections and become available for use.

What for?
---------

Airflow offers a generic toolbox for working with data. Different
organizations have different stacks and different needs. Using Airflow
plugins can be a way for companies to customize their Airflow installation
to reflect their ecosystem.

Plugins can be used as an easy way to write, share and activate new sets of
features.

There's also a need for a set of more complex applications to interact with
different flavors of data and metadata.

Examples:

* A set of tools to parse Hive logs and expose Hive metadata (CPU /IO / phases/ skew /...)
* An anomaly detection framework, allowing people to collect metrics, set thresholds and alerts
* An auditing tool, helping understand who accesses what
* A config-driven SLA monitoring tool, allowing you to set monitored tables and at what time
  they should land, alert people, and expose visualizations of outages
* ...

Why build on top of Airflow?
----------------------------

Airflow has many components that can be reused when building an application:

* A web server you can use to render your views
* A metadata database to store your models
* Access to your databases, and knowledge of how to connect to them
* An array of workers that your application can push workload to
* Airflow is deployed, you can just piggy back on its deployment logistics
* Basic charting capabilities, underlying libraries and abstractions


Interface
---------

To create a plugin you will need to derive the
``airflow.plugins_manager.AirflowPlugin`` class and reference the objects
you want to plug into Airflow. Here's what the class you need to derive
looks like:

.. literalinclude:: ../airflow/plugins_manager.py
    :language: python
    :start-after: [START plugins_interface]
    :end-before: [END plugins_interface]


You can derive it by inheritance (please refer to the example below).
Please note ``name`` inside this class must be specified.

After the plugin is imported into Airflow,
you can invoke it using statement like


.. code:: python

    from airflow.{type, like "operators", "sensors"}.{name specified inside the plugin class} import *


When you write your own plugins, make sure you understand them well.
There are some essential properties for each type of plugin.
For example,

* For ``Operator`` plugin, an ``execute`` method is compulsory.
* For ``Sensor`` plugin, a ``poke`` method returning a Boolean value is compulsory.

Make sure you restart the webserver and scheduler after making changes to plugins so that they take effect.


Example
-------

The code below defines a plugin that injects a set of dummy object
definitions in Airflow.

.. literalinclude:: ../tests/plugins/test_plugin.py
    :language: python
    :start-after: [START plugins_example]
    :end-before: [END plugins_example]

Note on role based views
------------------------

Airflow 1.10 introduced role based views using FlaskAppBuilder. You can configure which UI is used by setting
rbac = True. To support plugin views and links for both versions of the UI and maintain backwards compatibility,
the fields appbuilder_views and appbuilder_menu_items were added to the AirflowTestPlugin class.


Plugins as Python packages
--------------------------

It is possible to load plugins via `setuptools entrypoint <https://packaging.python.org/guides/creating-and-discovering-plugins/#using-package-metadata>`_ mechanism. To do this link
your plugin using an entrypoint in your package. If the package is installed, airflow
will automatically load the registered plugins from the entrypoint list.

.. note::
    Neither the entrypoint name (eg, `my_plugin`) nor the name of the
    plugin class will contribute towards the module and class name of the plugin
    itself. The structure is determined by
    `airflow.plugins_manager.AirflowPlugin.name` and the class name of the plugin
    component with the pattern `airflow.{component}.{name}.{component_class_name}`.

.. code-block:: python

    # my_package/my_plugin.py
    from airflow.plugins_manager import AirflowPlugin
    from airflow.models import BaseOperator
    from airflow.hooks.base_hook import BaseHook

    class MyOperator(BaseOperator):
      pass

    class MyHook(BaseHook):
      pass

    class MyAirflowPlugin(AirflowPlugin):
      name = 'my_namespace'
      operators = [MyOperator]
      hooks = [MyHook]


.. code-block:: python

    from setuptools import setup

    setup(
        name="my-package",
        ...
        entry_points = {
            'airflow.plugins': [
                'my_plugin = my_package.my_plugin:MyAirflowPlugin'
            ]
        }
    )


This will create a hook, and an operator accessible at:
 - `airflow.hooks.my_namespace.MyHook`
 - `airflow.operators.my_namespace.MyOperator`
