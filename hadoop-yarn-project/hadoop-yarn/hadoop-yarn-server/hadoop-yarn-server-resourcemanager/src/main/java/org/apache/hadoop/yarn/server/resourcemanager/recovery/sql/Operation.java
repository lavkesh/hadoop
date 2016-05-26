/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.recovery.sql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class Operation {
  Map<String, List<Object>> operationArgumentsMap;

  public Operation() {
    operationArgumentsMap = new HashMap<String, List<Object>>();
  }

  public void put(String key, List<Object> argList) {
    operationArgumentsMap.put(key, argList);
  }

  public List<Object> get(String key) {
    return operationArgumentsMap.get(key);
  }

  public Set<String> keySet() {
    return operationArgumentsMap.keySet();
  }

}
