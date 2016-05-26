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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.sql.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.h2.tools.DeleteDbFiles;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestSqlStateStore extends RMStateStoreTestBase {

  public static final String H2_LOCATION = "h2db";

  @Test
  public void testSqlStateStore() throws Exception {
    try {
      TestSqlStateStoreTests tester = new TestSqlStateStoreTests();
      testRMAppStateStore(tester);
      testRMDTSecretManagerStateStore(tester);
      testCheckVersion(tester);
      testEpoch(tester);
      testAppDeletion(tester);
      testAMRMTokenSecretManagerStateStore(tester);
      testReservationStateStore(tester);
      testDeleteStore(tester);
      tester.store.sqlop.disconnect();
    } finally {
      DeleteDbFiles.execute("./target", H2_LOCATION, true);
    }
  }

  private Configuration createHARMConf(String rmIds, String rmId, int adminPort) {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, rmIds);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, TestSqlStateInternal.class.getName());
    conf.set(YarnConfiguration.RM_HA_ID, rmId);
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:0");

    for (String rpcAddress : YarnConfiguration.getServiceAddressConfKeys(conf)) {
      for (String id : HAUtil.getRMHAIds(conf)) {
        conf.set(HAUtil.addSuffix(rpcAddress, id), "localhost:0");
      }
    }
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_ADMIN_ADDRESS, rmId), "localhost:" + adminPort);
    return conf;
  }

  @Test
  public void testSqlStateStoreFencing() throws Exception {
    try {
      StateChangeRequestInfo req = new StateChangeRequestInfo(HAServiceProtocol.RequestSource.REQUEST_BY_USER);
      Configuration conf1 = createHARMConf("rm1,rm2", "rm1", 1234);
      conf1.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
      ResourceManager rm1 = new ResourceManager();
      rm1.init(conf1);
      rm1.start();
      rm1.getRMContext().getRMAdminService().transitionToActive(req);
      assertEquals("RM with SQL Store didn't start", Service.STATE.STARTED, rm1.getServiceState());
      assertEquals("RM should be Active", HAServiceProtocol.HAServiceState.ACTIVE,
          rm1.getRMContext().getRMAdminService().getServiceStatus().getState());
      Configuration conf2 = createHARMConf("rm1,rm2", "rm2", 5678);
      conf2.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
      ResourceManager rm2 = new ResourceManager();
      rm2.init(conf2);
      rm2.start();
      rm2.getRMContext().getRMAdminService().transitionToActive(req);
      assertEquals("RM with SQL Store didn't start", Service.STATE.STARTED, rm2.getServiceState());
      assertEquals("RM should be Active", HAServiceProtocol.HAServiceState.ACTIVE,
          rm2.getRMContext().getRMAdminService().getServiceStatus().getState());
      Thread.sleep(YarnConfiguration.DEFAULT_RM_DB_VERIFICATION_TIMEOUT);
      assertEquals("RM should have been fenced", HAServiceProtocol.HAServiceState.STANDBY,
          rm1.getRMContext().getRMAdminService().getServiceStatus().getState());
      assertEquals("RM should be Active", HAServiceProtocol.HAServiceState.ACTIVE,
          rm2.getRMContext().getRMAdminService().getServiceStatus().getState());
    } finally {
      DeleteDbFiles.execute("./target", H2_LOCATION, true);
    }
  }

  static class TestSqlStateInternal extends SqlRMStateStore {

    public TestSqlStateInternal(Configuration conf, String uuid) {
      this.uuid = uuid;
      init(conf);
      start();
    }

    public TestSqlStateInternal() {
    }

    @Override
    protected SQLOperations createSQLOperationsWithRetry() {
      return new TestSQlOperations();
    }

  }

  static class TestSQlOperations extends PSQLOperations {
    @Override
    protected void ensureConnection() throws SQLException {
      if (connection != null && !connection.isClosed() && connection.isValid(0)) {
        return;
      }
      connection = DriverManager.getConnection("jdbc:h2:./target/" + H2_LOCATION, "test", "");
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    }

    @Override
    public void doCreateTable(String table) throws SQLException {
      Connection conn = DriverManager.getConnection("jdbc:h2:./target/" + H2_LOCATION, "test", "");
      PreparedStatement stmt = null;
      try {
        stmt = conn.prepareStatement(createTable(table));
        stmt.execute();
      } finally {
        try {
          stmt.close();
        } catch (Exception e) {

        }
      }
    }

    @Override
    public void doDropTable(String table) throws SQLException {
      Connection conn = DriverManager.getConnection("jdbc:h2:./target/" + H2_LOCATION, "test", "");
      PreparedStatement stmt = null;
      try {
        stmt = conn.prepareStatement(dropTable(table));
        stmt.execute();
      } finally {
        try {
          stmt.close();
        } catch (Exception e) {

        }
      }
    }

    @Override
    protected String createTable(String table) {
      return "CREATE TABLE IF NOT EXISTS " + table + " (key varchar(1024) PRIMARY KEY, value blob)";
    }

    @Override
    public void doUpdateOrInsert(String table, String key, byte[] value, Credential credential)
        throws SQLException, StoreFencedException {
      List<PreparedStatement> opList = new ArrayList<PreparedStatement>();
      String sql = "MERGE INTO " + table + " (key, value) KEY (key) VALUES (?,?)";
      ensureConnection();
      PreparedStatement stmt = connection.prepareStatement(sql);
      stmt.setString(1, key);
      stmt.setObject(2, value);
      opList.add(stmt);
      doTransaction(opList, credential);
    }

    @Override
    public PreparedStatement prepareInsertIfNotExists(String table, String key, byte[] value) throws SQLException {
      ensureConnection();
      PreparedStatement preparedStatement = connection.prepareStatement(insertIfNotExists(table));
      preparedStatement.setString(1, key);
      preparedStatement.setBytes(2, value);
      return preparedStatement;
    }

    @Override
    protected String selectForShare(String table) {
      // Overriding it and changing it to select for update because select for share is not available in H2.
      return selectForUpdate(table);
    }

    protected String selectForUpdate(String table) {
      return "SELECT key, value from " + table + " WHERE key = ? FOR UPDATE";
    }

    @Override
    protected String insertIfNotExists(String table) {
      return "MERGE INTO " + table + " (key, value) KEY (key) VALUES (?,?)";
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void doMultipleOp(List<Operation> opList, Credential credential) throws StoreFencedException, SQLException {
      try {
        List<PreparedStatement> listOfOperations = new ArrayList<>();
        for (Operation m : opList) {
          for (String key : m.keySet()) {
            try {
              switch (key.toString()) {
              case "update":
                Method method = this.getClass().getMethod("prepareUpdate", String.class, String.class, byte[].class);
                List params = m.get(key);
                listOfOperations
                    .add((PreparedStatement) method.invoke(this, params.get(0), params.get(1), params.get(2)));
                break;
              case "insertIfNotExists":
                method = this.getClass()
                    .getMethod("prepareInsertIfNotExists", String.class, String.class, byte[].class);
                params = m.get(key);
                listOfOperations
                    .add((PreparedStatement) method.invoke(this, params.get(0), params.get(1), params.get(2)));
                break;
              case "deleteRange":
                method = this.getClass().getMethod("prepareDeleteRange", String.class, String.class, String.class);
                params = m.get(key);
                listOfOperations
                    .add((PreparedStatement) method.invoke(this, params.get(0), params.get(1), params.get(2)));
                break;
              default:
                break;
              }
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
              throw new YarnRuntimeException("Method invokation failed " + e);
            }
          }
        }
        doTransaction(listOfOperations, credential);
      } catch (SQLException e) {
        throw new SQLException(e);
      }
    }
  }

  static class TestSqlStateStoreTests implements RMStateStoreHelper {
    TestSqlStateInternal store;
    String uuid = UUID.randomUUID().toString();

    @Override
    public RMStateStore getRMStateStore() throws Exception {
      YarnConfiguration conf = new YarnConfiguration();
      this.store = new TestSqlStateInternal(conf, uuid);
      return this.store;
    }

    @Override
    public boolean isFinalStateValid() throws Exception {
      return true;
    }

    @Override
    public void writeVersion(Version version) throws Exception {
      byte[] data = ((VersionPBImpl) version).getProto().toByteArray();
      store.sqlop.doUpdateOrInsert(TestSqlStateInternal.CREDENTIAL_TABLE, TestSqlStateInternal.VERSION, data,
          store.credential);
    }

    @Override
    public Version getCurrentVersion() throws Exception {
      return store.getCurrentVersion();
    }

    @Override
    public boolean appExists(RMApp app) throws Exception {
      LocalResultSet lrs = null;
      try {
        lrs = store.sqlop.getFromTable(TestSqlStateInternal.APPLICATION_TABLE, app.getApplicationId().toString());
        return lrs.set.next();
      } catch (Exception e) {
        return false;
      } finally {
        try {
          lrs.close();
        } catch (Exception e) {

        }
      }
    }

    @Override
    public boolean attemptExists(RMAppAttempt attempt) throws Exception {
      LocalResultSet lrs = null;
      try {
        lrs = store.sqlop.getFromTable(TestSqlStateInternal.ATTEMPT_TABLE,
            attempt.getSubmissionContext().getApplicationId() + store.SEPARATOR + attempt.getAppAttemptId());
        return lrs.set.next();
      } catch (Exception e) {
        return false;
      } finally {
        try {
          lrs.close();
        } catch (Exception e) {

        }
      }
    }
  }
}
