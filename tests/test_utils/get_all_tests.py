#!/usr/bin/env python
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
import sys
from xml.etree import ElementTree


def last_replace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)


def print_all_cases(xmlunit_test_file):
    with open(xmlunit_test_file, "r") as f:
        text = f.read()

    root = ElementTree.fromstring(text)

    test_cases = root.findall('.//testcase')
    classes = set()
    modules = set()

    for test_case in test_cases:
        the_module = '.'.join(test_case.get('classname').split('.')[:-1])
        the_class = last_replace(test_case.get('classname'), ".", ":", 1)
        test_method = test_case.get('name')
        modules.add(the_module)
        classes.add(the_class)
        print(the_class + "." + test_method)

    for the_class in classes:
        print(the_class)

    for the_module in modules:
        print(the_module)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please provide name of xml unit file as first parameter")
        exit(0)
    file_name = sys.argv[1]
    print_all_cases(file_name)
