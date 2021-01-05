#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import importlib

from build_environment import get_build_environment


tests = importlib.import_module("run-tests")

if __name__ == '__main__':
    env = get_build_environment()
    extra_profiles = tests.get_hadoop_profiles(env.hadoop_version)

    tests.build_apache_spark(env.build_tool, extra_profiles)

    if env.build_tool == "sbt":
        # TODO(dsanduleac): since this is required for tests, might as well run it right
        # away in our build step.

        # Since we did not build assembly/package before running dev/mima, we need to
        # do it here because the tests still rely on it; see SPARK-13294 for details.
        tests.build_spark_assembly_sbt(extra_profiles)
