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

public class SQLConstants {

  public static final String DELETE_RANGE_KEY = "deleteRange";
  public static final String UPDATE_KEY = "update";
  public static final String INSERT_IF_NOT_EXIST_KEY = "insertIfNotExists";
  public static final String KEY_COLUMN = "key";
  public static final String VALUE_COLUMN = "value";

  private SQLConstants() {

  }
}
