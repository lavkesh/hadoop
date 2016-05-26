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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFencedException;

import java.sql.SQLException;
import java.util.List;

public interface SQLOperations {

  void init(Configuration conf);

  void doCreateTable(String applicationTable) throws SQLException;

  void fence(Credential credential) throws SQLException;

  void disconnect() throws SQLException;

  LocalResultSet getFromTable(String table , String key) throws SQLException;

  void doUpdateOrInsert(String table, String key, byte[] value, Credential credential) throws SQLException, StoreFencedException;

  LocalResultSet getFromTableRange(String table , String from , String key) throws SQLException;

  void doMultipleOp(List<Operation> opList, Credential credential) throws SQLException, StoreFencedException;

  void doDelete(String table, String key, Credential credential) throws SQLException, StoreFencedException;

  void doDropTable(String table) throws SQLException;

  void doFencingCheck(Credential credential) throws SQLException, StoreFencedException ;


}
