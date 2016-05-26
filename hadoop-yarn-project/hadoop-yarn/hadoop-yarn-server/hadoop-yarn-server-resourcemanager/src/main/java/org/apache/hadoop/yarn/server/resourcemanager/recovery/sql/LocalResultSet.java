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

import org.apache.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class LocalResultSet {

  public ResultSet set = null;
  public PreparedStatement stmt = null;

  public LocalResultSet(ResultSet set, PreparedStatement stmt) {
    this.set = set;
    this.stmt = stmt;
  }

  public void close() {
    if (set != null) {
      try {
        set.close();
      } catch (Exception e) {
        Logger.getLogger(getClass()).warn("Error closing LocalResultSet " + e);
      }
    }
    if (stmt != null) {
      try {
        stmt.close();
      } catch (Exception e) {
        Logger.getLogger(getClass()).warn("Error closing LocalResultSet " + e);
      }
    }
  }
}