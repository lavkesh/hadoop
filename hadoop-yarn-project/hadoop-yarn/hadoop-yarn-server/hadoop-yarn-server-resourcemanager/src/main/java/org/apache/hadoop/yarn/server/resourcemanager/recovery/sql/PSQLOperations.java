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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFencedException;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains Postgresql based SQL operations for SqlRMStateStore.
 *
 * DB operations normally throw PSQLException, We are catching it and
 * rethrowing SQLException so that Retry policy can work.
 *
 */
public class PSQLOperations implements SQLOperations {
  protected Connection connection = null;
  private String dbName = null;
  private String dbHostName = null;
  private String userName = null;
  private String password = null;
  private static Logger LOG = Logger.getLogger(PSQLOperations.class);

  private Map<String, Method> methodsMap = new HashMap<String, Method>();

  static {
    LOG.info("LOADED CLASS PSQLOperations modified on 27 oct 2015");
  }

  @Override
  public void init(Configuration conf) {
    userName = conf.get(YarnConfiguration.RM_DB_USERNAME);
    dbName = conf.get(YarnConfiguration.RM_DB_DBNAME);
    dbHostName =
        "jdbc:" + conf.get(YarnConfiguration.RM_DB_TYPE) + "://" + conf.get(YarnConfiguration.RM_DB_HOST) + "/"
            + dbName;
    String passFile = "";
    if ((passFile = conf.get(YarnConfiguration.RM_DB_PASSWORD_FILE, passFile)).isEmpty()) {
      password = conf.get(YarnConfiguration.RM_DB_PASSWORD);
    } else {
      BufferedReader br = null;
      try {
        br = new BufferedReader(new FileReader(passFile));
        password = br.readLine();
      } catch (Exception e) {
        LOG.warn("Exception occured while reading password file " + e);
        // If Exception then read conf from the yarn-site
        password = conf.get(YarnConfiguration.RM_DB_PASSWORD);
      } finally {
        if (br != null) {
          try {
            br.close();
          } catch (Exception e) {

          }
        }

      }
    }
    // Cache methods for doMultipleOp()
    try {
      methodsMap.put(SQLConstants.UPDATE_KEY,
          this.getClass().getMethod("prepareUpdate", String.class, String.class, byte[].class));
      methodsMap.put(SQLConstants.INSERT_IF_NOT_EXIST_KEY,
          this.getClass().getMethod("prepareInsertIfNotExists", String.class, String.class, byte[].class));
      methodsMap.put(SQLConstants.DELETE_RANGE_KEY,
          this.getClass().getMethod("prepareDeleteRange", String.class, String.class, String.class));
    } catch (NoSuchMethodException | SecurityException e) {
      throw new YarnRuntimeException("One or more methods initilisation failed " + e);
    }
  }

  @VisibleForTesting
  protected synchronized void ensureConnection() throws SQLException {
    if (connection != null && !connection.isClosed() && connection.isValid(0)) {
      return;
    }
    connection = DriverManager.getConnection(dbHostName, userName, password);
    connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
  }

  @VisibleForTesting
  public synchronized void disconnect() throws SQLException {
    if (connection != null)
      connection.close();
  }

  @Override
  public synchronized void doCreateTable(String table) throws SQLException {
    try {
      Connection conn = DriverManager.getConnection(dbHostName, userName, password);
      PreparedStatement stmt = null;
      try {
        stmt = conn.prepareStatement(createTable(table));
        stmt.execute();
      } finally {
        try {
          stmt.close();
          conn.close();
        } catch (Exception e) {
          LOG.warn("Exception occured while closing connetion in doCreateTable " + e);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
  }

  @Override
  public synchronized void doDropTable(String table) throws SQLException {
    try {
      Connection conn = DriverManager.getConnection(dbHostName, userName, password);
      PreparedStatement stmt = null;
      try {
        stmt = conn.prepareStatement(dropTable(table));
        stmt.execute();
      } finally {
        try {
          stmt.close();
          conn.close();
        } catch (Exception e) {
          LOG.warn("Exception occured while closing connetion in doDropTable " + e);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
  }

  @Override
  public synchronized LocalResultSet getFromTable(String table, String key) throws SQLException {
    try {
      ensureConnection();
      PreparedStatement getKey = connection.prepareStatement(select(table));
      getKey.setString(1, key);
      return new LocalResultSet(getKey.executeQuery(), getKey);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
  }

  public synchronized LocalResultSet getFromTableForShare(String table, String key) throws SQLException {
    try {
      ensureConnection();
      PreparedStatement getKey = connection.prepareStatement(selectForShare(table));
      getKey.setString(1, key);
      return new LocalResultSet(getKey.executeQuery(), getKey);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
  }

  @Override
  public synchronized LocalResultSet getFromTableRange(String table, String from, String to) throws SQLException {
    try {
      ensureConnection();
      PreparedStatement getKey = connection.prepareStatement(selectRange(table));
      getKey.setString(1, from);
      getKey.setString(2, to);
      return new LocalResultSet(getKey.executeQuery(), getKey);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
  }

  public synchronized PreparedStatement prepareCreateTable(String table) throws SQLException {
    ensureConnection();
    PreparedStatement preparedStatement = connection.prepareStatement(createTable(table));
    return preparedStatement;
  }

  public synchronized PreparedStatement prepareDeleteRange(String table, String from, String to) throws SQLException {
    ensureConnection();
    PreparedStatement preparedStatement = connection.prepareStatement(deleteRange(table));
    preparedStatement.setString(1, from);
    preparedStatement.setString(2, to);
    return preparedStatement;
  }

  public synchronized PreparedStatement prepareDelete(String table, String key) throws SQLException {
    ensureConnection();
    PreparedStatement preparedStatement = connection.prepareStatement(delete(table));
    preparedStatement.setString(1, key);
    return preparedStatement;

  }

  public synchronized PreparedStatement prepareUpdate(String table, String key, byte[] value) throws SQLException {
    ensureConnection();
    PreparedStatement preparedStatement = connection.prepareStatement(update(table));
    preparedStatement.setString(2, key);
    preparedStatement.setBytes(1, value);
    return preparedStatement;
  }

  public synchronized PreparedStatement prepareInsertIfNotExists(String table, String key, byte[] value)
      throws SQLException {
    ensureConnection();
    PreparedStatement preparedStatement = connection.prepareStatement(insertIfNotExists(table));
    preparedStatement.setString(1, key);
    preparedStatement.setBytes(2, value);
    preparedStatement.setString(3, key);
    return preparedStatement;
  }

  protected String selectRange(String table) {
    return "SELECT key, value from " + table + " WHERE key >= ? and key <= ?";
  }

  protected String select(String table) {
    return "SELECT key, value from " + table + " WHERE key = ?";
  }

  protected String selectForShare(String table) {
    return "SELECT key, value from " + table + " WHERE key = ? FOR SHARE";
  }

  protected String insertIfNotExists(String table) {
    return "INSERT INTO " + table + " (key, value) SELECT ?,? WHERE NOT EXISTS ( SELECT value FROM " + table
        + " WHERE key = ?)";
  }

  protected String deleteRange(String table) {
    return "DELETE FROM " + table + " WHERE key >= ? and key <= ?";
  }

  protected String delete(String table) {
    return "DELETE FROM " + table + " WHERE key = ?";
  }

  protected String createTable(String table) {
    return "CREATE TABLE IF NOT EXISTS " + table + " (key text PRIMARY KEY, value bytea)";
  }

  protected String update(String table) {
    return "UPDATE " + table + " SET value = ? WHERE key = ?";
  }

  protected String dropTable(String table) {
    return "DROP TABLE IF EXISTS " + table;
  }

  /**
   * 1. Begin transaction.
   * 2. Update stuff
   * 3. Check the master key.
   * 4. If the master key is not same as the digest
   * then abort
   * else
   * 5. Commit.
   * @throws StoreFencedException
   * @throws SQLException
   */
  protected synchronized void doTransaction(List<PreparedStatement> operations, Credential credential)
      throws StoreFencedException, SQLException {
    LocalResultSet lrs = null;
    try {
      ensureConnection();
      connection.setAutoCommit(false);
      for (PreparedStatement st : operations) {
        st.executeUpdate();
      }
      lrs = getFromTableForShare(credential.getCredentialTable(), credential.getKey());
      if (!lrs.set.next()) {
        LOG.warn("Credential set is null");
        throw new StoreFencedException();
      }
      String cred = new String(lrs.set.getBytes("value"));
      if (!cred.equals(credential.getSecret())) {
        LOG.warn("Stored credentials are not equal to RM's credentials");
        throw new StoreFencedException();
      }
      connection.commit();
    } catch (StoreFencedException | SQLException e) {
      try {
        connection.rollback();
      } catch (SQLException e1) {
        LOG.error("Error occurred while Connnection rollback " + e1);
      }
      e.printStackTrace();
      throw e;
    } finally {
      // This should not throw error. If it does current transaction will be retried.
      ensureConnection();
      connection.setAutoCommit(true);
      if (lrs != null)
        lrs.close();
      for (PreparedStatement st : operations) {
        try {
          st.close();
        } catch (Exception e) {
          LOG.warn("Exception occured while closing prepared statements.");
        }
      }
    }
  }

  @Override
  public synchronized void fence(Credential credential) throws SQLException {
    // Put the credential in the master table.
    try {
      byte[] value = credential.getSecret().getBytes();
      PreparedStatement update = prepareUpdate(credential.getCredentialTable(), credential.getKey(), value);
      PreparedStatement insertIfNotThere =
          prepareInsertIfNotExists(credential.getCredentialTable(), credential.getKey(), value);
      try {
        ensureConnection();
        connection.setAutoCommit(false);
        update.executeUpdate();
        insertIfNotThere.executeUpdate();
        connection.commit();
      } catch (SQLException e) {
        try {
          connection.rollback();
        } catch (SQLException e1) {
          LOG.warn("Exception occured while rollback in fence() " + e);
        }
        throw e;
      } finally {
        try {
          connection.setAutoCommit(true);
        } catch (SQLException e1) {
          LOG.warn("Exception occured while setting auto commit fence() " + e1);
        }
        try {
          update.close();
          insertIfNotThere.close();
        } catch (Exception e) {
          LOG.warn("Exception occured while closing prepared statements in fence() " + e);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
  }

  @Override
  public void doUpdateOrInsert(String table, String key, byte[] value, Credential credential) throws SQLException,
      StoreFencedException {
    try {
      List<PreparedStatement> opList = new ArrayList<PreparedStatement>();
      opList.add(prepareUpdate(table, key, value));
      opList.add(prepareInsertIfNotExists(table, key, value));
      doTransaction(opList, credential);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
  }

  @Override
  public void doDelete(String table, String key, Credential credential) throws SQLException, StoreFencedException {
    try {
      List<PreparedStatement> opList = new ArrayList<PreparedStatement>();
      opList.add(prepareDelete(table, key));
      doTransaction(opList, credential);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
  }

  @Override
  public void doFencingCheck(Credential credential) throws StoreFencedException, SQLException {
    try {
      doTransaction(new ArrayList<PreparedStatement>(), credential);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void doMultipleOp(List<Operation> opList, Credential credential) throws StoreFencedException, SQLException {
    try {
      List<PreparedStatement> listOfOperations = new ArrayList<>();
      for (Operation m : opList) {
        for (String key : m.keySet()) {
          try {
            Method method = methodsMap.get(key);
            List params = m.get(key);
            // For now all the methods have 3 parameters.
            listOfOperations.add((PreparedStatement) method.invoke(this, params.get(0), params.get(1),
                params.get(2)));
          } catch (SecurityException | IllegalAccessException e) {
            throw new YarnRuntimeException("Method invokation failed " + e);
          } catch (InvocationTargetException e) {
            // Handle InvocationTargetExeception separately.
            if (e.getCause().getClass().isAssignableFrom(SQLException.class)) {
              throw new SQLException(e.getCause());
            }
          }
        }
      }
      doTransaction(listOfOperations, credential);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
  }
}
