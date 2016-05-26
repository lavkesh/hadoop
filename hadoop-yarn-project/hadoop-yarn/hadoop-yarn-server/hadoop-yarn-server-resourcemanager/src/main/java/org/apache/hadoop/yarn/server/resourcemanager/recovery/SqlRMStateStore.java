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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.AMRMTokenSecretManagerStatePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.EpochPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.sql.*;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SqlRMStateStore extends RMStateStore {
  protected static final String CREDENTIAL_TABLE = "credentialtable";
  protected static final String APPLICATION_TABLE = "applicationtable";
  protected static final String ATTEMPT_TABLE = "attempttable";
  protected static final String RMDELEGATAION_TABLE = "rmdelegationtable";
  protected static final String RMMASTERKEYS_TABLE = "rmmasterkeystable";
  protected static final String RESERVATION_PLANS_TABLE = "reservationtable";
  protected static final String DelegtaionTokenRoot = "DelegtaionTokenRoot";
  protected static final String RMDTSequentialNumber = "RMDTSequentialNumber";
  protected static final String SEPARATOR = "/";
  protected static final String VERSION = "version";
  protected static final String EPOCH = "epoch";
  protected static final String MASTERCRED = "mastercred";
  protected static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 3);
  protected Credential credential = null;
  protected String uuid = UUID.randomUUID().toString();
  protected int verificationTimeOut = 10 * 1000;
  protected SQLOperations sqlop = null;

  private static final Logger LOG = Logger.getLogger(SqlRMStateStore.class);
  private Thread verifyActiveStatusThread;
  // Retry policy
  private int maxRetries = 0;
  private int retryIntervalSeconds = 0;

  protected SQLOperations createSQLOperationsWithRetry() {
    SQLOperations operations = new PSQLOperations();
    RetryPolicy basePolicy = RetryPolicies
        .retryUpToMaximumCountWithFixedSleep(maxRetries, retryIntervalSeconds, TimeUnit.SECONDS);
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<>();
    exceptionToPolicyMap.put(SQLException.class, basePolicy);
    RetryPolicy methodPolicy = RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();
    methodNameToPolicyMap.put("doUpdateOrInsert", methodPolicy);
    methodNameToPolicyMap.put("doDelete", methodPolicy);
    methodNameToPolicyMap.put("fence", methodPolicy);
    methodNameToPolicyMap.put("getFromTable", methodPolicy);
    methodNameToPolicyMap.put("getFromTableRange", methodPolicy);
    methodNameToPolicyMap.put("doDropTable", methodPolicy);
    methodNameToPolicyMap.put("doFencingCheck", methodPolicy);
    methodNameToPolicyMap.put("doMultipleOp", methodPolicy);
    return (SQLOperations) RetryProxy.create(SQLOperations.class, operations, methodNameToPolicyMap);
  }

  @Override
  protected void initInternal(Configuration conf) throws Exception {
    maxRetries = conf.getInt(YarnConfiguration.RM_DB_RETRIES, YarnConfiguration.DEFAULT_RM_DB_RETRIES);
    retryIntervalSeconds = conf
        .getInt(YarnConfiguration.RM_DB_RETRIES_INTERVAL, YarnConfiguration.DEFAULT_RM_DB_RETRIES_INTERVAL);
    verificationTimeOut = conf
        .getInt(YarnConfiguration.RM_DB_VERIFICATION_TIMEOUT, YarnConfiguration.DEFAULT_RM_DB_VERIFICATION_TIMEOUT);

    sqlop = createSQLOperationsWithRetry();
    sqlop.init(conf);
    credential = new Credential(CREDENTIAL_TABLE, MASTERCRED, uuid);
    LOG.debug(
        "Max Retries = " + maxRetries + " Retry Inverval Seconds = " + retryIntervalSeconds + " Verification Timeout "
            + verificationTimeOut);
    LOG.debug("Credential " + credential);
  }

  @Override
  protected void startInternal() throws Exception {
    // Create Various tables if non existing.
    sqlop.doCreateTable(CREDENTIAL_TABLE);
    sqlop.doCreateTable(APPLICATION_TABLE);
    sqlop.doCreateTable(ATTEMPT_TABLE);
    sqlop.doCreateTable(RMDELEGATAION_TABLE);
    sqlop.doCreateTable(RMMASTERKEYS_TABLE);
    sqlop.doCreateTable(RESERVATION_PLANS_TABLE);
    sqlop.fence(credential);
    if (HAUtil.isHAEnabled(getConfig())) {
      // Start the checker thread if HA is enabled.
      verifyActiveStatusThread = new VerifyActiveStatusThread();
      verifyActiveStatusThread.start();
    }
  }

  @Override
  protected void closeInternal() throws Exception {
    if (verifyActiveStatusThread != null) {
      verifyActiveStatusThread.interrupt();
      verifyActiveStatusThread.join(1000);
    }
    sqlop.disconnect();
  }

  @Override
  protected Version loadVersion() throws Exception {
    LocalResultSet lrs = sqlop.getFromTable(CREDENTIAL_TABLE, VERSION);
    try {
      if (!lrs.set.next()) {
        // No result
        return null;
      }
      return new VersionPBImpl(
          YarnServerCommonProtos.VersionProto.parseFrom(lrs.set.getBytes(SQLConstants.VALUE_COLUMN)));
    } finally {
      lrs.close();
    }
  }

  @Override
  protected void storeVersion() throws Exception {
    byte[] data = ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();
    sqlop.doUpdateOrInsert(CREDENTIAL_TABLE, VERSION, data, credential);
  }

  @Override
  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  public long getAndIncrementEpoch() throws Exception {
    long currentEpoch = 0;
    LocalResultSet lrs = sqlop.getFromTable(CREDENTIAL_TABLE, EPOCH);
    try {
      if (!lrs.set.next()) {
        byte[] storeData = Epoch.newInstance(currentEpoch + 1).getProto().toByteArray();
        sqlop.doUpdateOrInsert(CREDENTIAL_TABLE, EPOCH, storeData, credential);
      } else {
        byte[] data = lrs.set.getBytes(SQLConstants.VALUE_COLUMN);
        Epoch epoch = new EpochPBImpl(YarnServerResourceManagerRecoveryProtos.EpochProto.parseFrom(data));
        currentEpoch = epoch.getEpoch();
        // increment epoch and store it
        byte[] storeData = Epoch.newInstance(currentEpoch + 1).getProto().toByteArray();
        sqlop.doUpdateOrInsert(CREDENTIAL_TABLE, EPOCH, storeData, credential);
      }
      return currentEpoch;
    } finally {
      lrs.close();
    }
  }

  @Override
  public RMState loadState() throws Exception {
    RMState rmState = new RMState();
    // recover DelegationTokenSecretManager
    loadRMDTSecretManagerState(rmState);
    // recover RM applications
    loadRMAppState(rmState);
    // recover AMRMTokenSecretManager
    loadAMRMTokenSecretManagerState(rmState);
    // recover reservation state
    loadReservationSystemState(rmState);
    return rmState;
  }

  private void loadReservationSystemState(RMState rmState) throws Exception {
    LocalResultSet lrs = sqlop
        .getFromTableRange(RESERVATION_PLANS_TABLE, RESERVATION_SYSTEM_ROOT + SEPARATOR, RESERVATION_SYSTEM_ROOT + "z");
    int reservationCount = 0;
    try {
      while (lrs.set.next()) {
        // reservation is stored as RESERVATION_SYSTEM_ROOT + SEPARATOR + plan_name + SEPARATOR + reservation_id;
        String key = lrs.set.getString(SQLConstants.KEY_COLUMN);
        String planReservationString = key.substring((RESERVATION_SYSTEM_ROOT + SEPARATOR).length());
        String[] parts = planReservationString.split(SEPARATOR);
        if (parts.length != 2) {
          LOG.warn("Incorrect reservation state key " + key);
          continue;
        }
        String planName = parts[0];
        String reservationName = parts[1];
        ReservationAllocationStateProto allocationState = ReservationAllocationStateProto
            .parseFrom(lrs.set.getBytes(SQLConstants.VALUE_COLUMN));
        if (!rmState.getReservationState().containsKey(planName)) {
          rmState.getReservationState().put(planName, new HashMap<>());
        }
        ReservationId reservationId = ReservationId.parseReservationId(reservationName);
        rmState.getReservationState().get(planName).put(reservationId, allocationState);
        reservationCount++;
      }
    } finally {
      LOG.debug("Total reservation count " + reservationCount);
      lrs.close();
    }
  }

  private void loadRMDTSecretManagerState(RMState rmState) throws Exception {
    loadRMDelegationKeyState(rmState);
    loadRMSequentialNumberState(rmState);
    loadRMDelegationTokenState(rmState);

  }

  private void loadRMDelegationKeyState(RMState rmState) throws Exception {
    String keyIn = RM_DT_SECRET_MANAGER_ROOT + SEPARATOR + DELEGATION_KEY_PREFIX;
    LocalResultSet lrs = sqlop.getFromTableRange(RMMASTERKEYS_TABLE, keyIn, keyIn + "z");
    int delegationKeyCounter = 0;
    try {
      while (lrs.set.next()) {
        byte[] childData = lrs.set.getBytes(SQLConstants.VALUE_COLUMN);
        ByteArrayInputStream is = new ByteArrayInputStream(childData);
        DataInputStream fsIn = new DataInputStream(is);
        try {
          DelegationKey key = new DelegationKey();
          key.readFields(fsIn);
          rmState.rmSecretManagerState.masterKeyState.add(key);
          delegationKeyCounter++;
        } finally {
          is.close();
        }
      }
    } finally {
      LOG.debug("Total RMDelegationKeys loaded " + delegationKeyCounter);
      lrs.close();
    }
  }

  private void loadRMSequentialNumberState(RMState rmState) throws Exception {
    LocalResultSet lrs = sqlop.getFromTable(RMDELEGATAION_TABLE, RMDTSequentialNumber);
    try {
      if (lrs.set.next()) {
        byte[] seqData = lrs.set.getBytes(SQLConstants.VALUE_COLUMN);
        if (seqData != null) {
          ByteArrayInputStream seqIs = new ByteArrayInputStream(seqData);
          DataInputStream seqIn = new DataInputStream(seqIs);
          try {
            rmState.rmSecretManagerState.dtSequenceNumber = seqIn.readInt();
          } finally {
            seqIn.close();
          }
        }
      }
    } finally {
      lrs.close();
    }
  }

  private void loadRMDelegationTokenState(RMState rmState) throws Exception {
    String keyIn = DelegtaionTokenRoot + SEPARATOR + DELEGATION_TOKEN_PREFIX;
    LocalResultSet lrs = sqlop.getFromTableRange(RMDELEGATAION_TABLE, keyIn, keyIn + "z");
    int delegationTokenCounter = 0;
    try {
      while (lrs.set.next()) {
        byte[] childData = lrs.set.getBytes(SQLConstants.VALUE_COLUMN);
        if (childData == null) {
          LOG.warn("Content of " + lrs.set.getString(SQLConstants.KEY_COLUMN) + " is broken.");
          continue;
        }
        ByteArrayInputStream is = new ByteArrayInputStream(childData);
        DataInputStream fsIn = new DataInputStream(is);
        try {
          RMDelegationTokenIdentifierData identifierData = new RMDelegationTokenIdentifierData();
          identifierData.readFields(fsIn);
          RMDelegationTokenIdentifier identifier = identifierData.getTokenIdentifier();
          long renewDate = identifierData.getRenewDate();
          rmState.rmSecretManagerState.delegationTokenState.put(identifier, renewDate);
          delegationTokenCounter++;
          LOG.debug("Loaded RMDelegationTokenIdentifier: " + identifier + " renewDate=" + renewDate);
        } finally {
          is.close();
        }
      }
    } finally {
      LOG.debug("Total Delegation Tokens loaded " + delegationTokenCounter);
      lrs.close();

    }
  }

  private void loadRMAppState(RMState rmState) throws Exception {
    // Get all applications.
    LocalResultSet lrs = sqlop
        .getFromTableRange(APPLICATION_TABLE, ApplicationId.appIdStrPrefix, ApplicationId.appIdStrPrefix + "z");
    int appCounter = 0;
    try {
      while (lrs.set.next()) {
        ApplicationId appId = ConverterUtils.toApplicationId(lrs.set.getString(SQLConstants.KEY_COLUMN));
        ApplicationStateDataPBImpl appStateData = new ApplicationStateDataPBImpl(
            YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto
                .parseFrom(lrs.set.getBytes(SQLConstants.VALUE_COLUMN)));
        if (!appId.equals(appStateData.getApplicationSubmissionContext().getApplicationId())) {
          throw new YarnRuntimeException("The key is different from the application id");
        }
        rmState.appState.put(appId, appStateData);
        appCounter++;
        // For each application fetch attempts
        int attemptCounter = loadApplicationAttemptState(appStateData, appId);
        LOG.debug("App loaded " + appId + " with attempts " + attemptCounter);
      }
    } finally {
      LOG.debug("Total apps loaded " + appCounter);
      lrs.close();
    }
  }

  private int loadApplicationAttemptState(ApplicationStateData appState, ApplicationId appId) throws Exception {
    LocalResultSet lrs = sqlop.getFromTableRange(ATTEMPT_TABLE, appId.toString(), appId.toString() + "z");
    int attemptCounter = 0;
    try {
      while (lrs.set.next()) {
        byte[] attemptData = lrs.set.getBytes(SQLConstants.VALUE_COLUMN);
        ApplicationAttemptStateDataPBImpl attemptState = new ApplicationAttemptStateDataPBImpl(
            YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto.parseFrom(attemptData));
        appState.attempts.put(attemptState.getAttemptId(), attemptState);
        attemptCounter++;
      }
    } finally {
      lrs.close();
    }
    return attemptCounter;
  }

  private void loadAMRMTokenSecretManagerState(RMState rmState) throws Exception {
    LocalResultSet lrs = sqlop.getFromTable(RMMASTERKEYS_TABLE, AMRMTOKEN_SECRET_MANAGER_ROOT);
    try {
      byte[] data;
      if (lrs.set.next()) {
        data = lrs.set.getBytes(SQLConstants.VALUE_COLUMN);
      } else {
        LOG.warn("There is no data saved");
        return;
      }
      AMRMTokenSecretManagerStatePBImpl stateData = new AMRMTokenSecretManagerStatePBImpl(
          YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto.parseFrom(data));
      rmState.amrmTokenSecretManagerState = AMRMTokenSecretManagerState
          .newInstance(stateData.getCurrentMasterKey(), stateData.getNextMasterKey());
    } finally {
      lrs.close();
    }
  }

  @Override
  protected void storeApplicationStateInternal(ApplicationId appId, ApplicationStateData appStateData)
      throws Exception {
    updateApplicationStateInternal(appId, appStateData);
  }

  @Override
  protected void updateApplicationStateInternal(ApplicationId appId, ApplicationStateData appStateDataPB)
      throws Exception {
    byte[] appStateData = appStateDataPB.getProto().toByteArray();
    sqlop.doUpdateOrInsert(APPLICATION_TABLE, appId.toString(), appStateData, credential);
  }

  @Override
  protected void storeApplicationAttemptStateInternal(ApplicationAttemptId attemptId,
      ApplicationAttemptStateData attemptStateDataPB) throws Exception {
    updateApplicationAttemptStateInternal(attemptId, attemptStateDataPB);
  }

  @Override
  protected void updateApplicationAttemptStateInternal(ApplicationAttemptId attemptId,
      ApplicationAttemptStateData attemptStateDataPB) throws Exception {
    String key = attemptId.getApplicationId().toString() + SEPARATOR + attemptId;
    byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();
    sqlop.doUpdateOrInsert(ATTEMPT_TABLE, key, attemptStateData, credential);
  }

  @Override
  protected void storeRMDelegationTokenState(RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    storeOrUpdateRMDelegationTokenState(rmDTIdentifier, renewDate, false);
  }

  @Override
  protected void removeRMDelegationTokenState(RMDelegationTokenIdentifier rmDTIdentifier) throws Exception {
    String key = DelegtaionTokenRoot + SEPARATOR + DELEGATION_TOKEN_PREFIX + rmDTIdentifier.getSequenceNumber();
    sqlop.doDelete(RMDELEGATAION_TABLE, key, credential);
  }

  @Override
  protected void updateRMDelegationTokenState(RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    storeOrUpdateRMDelegationTokenState(rmDTIdentifier, renewDate, true);
  }

  private void storeOrUpdateRMDelegationTokenState(RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      boolean isUpdate) throws Exception {
    String key = DelegtaionTokenRoot + SEPARATOR + DELEGATION_TOKEN_PREFIX + rmDTIdentifier.getSequenceNumber();
    RMDelegationTokenIdentifierData identifierData = new RMDelegationTokenIdentifierData(rmDTIdentifier, renewDate);
    Operation delegationArgMap = new Operation();
    List<Object> delegationArguments = new ArrayList<>();
    delegationArguments.add(RMDELEGATAION_TABLE);
    delegationArguments.add(key);
    delegationArguments.add(identifierData.toByteArray());
    delegationArgMap.put(SQLConstants.UPDATE_KEY, delegationArguments);
    delegationArgMap.put(SQLConstants.INSERT_IF_NOT_EXIST_KEY, delegationArguments);
    List<Operation> opList = new ArrayList<>();
    opList.add(delegationArgMap);
    if (LOG.isDebugEnabled()) {
      LOG.debug((isUpdate ? "Storing " : "Updating ") + "RMDelegationToken_" +
          rmDTIdentifier.getSequenceNumber());
    }
    if (!isUpdate) {
      // Add sequence number only when adding.
      ByteArrayOutputStream seqOs = new ByteArrayOutputStream();
      try (DataOutputStream seqOut = new DataOutputStream(seqOs)) {
        seqOut.writeInt(rmDTIdentifier.getSequenceNumber());
      }
      Operation seqArgMap = new Operation();
      List<Object> seqArguments = new ArrayList<>();
      seqArguments.add(RMDELEGATAION_TABLE);
      seqArguments.add(RMDTSequentialNumber);
      seqArguments.add(seqOs.toByteArray());
      seqArgMap.put(SQLConstants.UPDATE_KEY, seqArguments);
      seqArgMap.put(SQLConstants.INSERT_IF_NOT_EXIST_KEY, seqArguments);
      opList.add(seqArgMap);
    }
    sqlop.doMultipleOp(opList, credential);
  }

  @Override
  protected void storeRMDTMasterKeyState(DelegationKey delegationKey) throws Exception {
    String key = RM_DT_SECRET_MANAGER_ROOT + SEPARATOR + DELEGATION_KEY_PREFIX + delegationKey.getKeyId();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream fsOut = new DataOutputStream(os);
    delegationKey.write(fsOut);
    try {
      sqlop.doUpdateOrInsert(RMMASTERKEYS_TABLE, key, os.toByteArray(), credential);
    } finally {
      os.close();
    }

  }

  @Override
  protected void storeReservationState(ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
    String key = RESERVATION_SYSTEM_ROOT + SEPARATOR + planName + SEPARATOR + reservationIdName;
    byte[] value = reservationAllocation.toByteArray();
    sqlop.doUpdateOrInsert(RESERVATION_PLANS_TABLE, key, value, credential);
  }

  @Override
  protected void removeReservationState(String planName, String reservationIdName) throws Exception {
    String key = RESERVATION_SYSTEM_ROOT + SEPARATOR + planName + SEPARATOR + reservationIdName;
    sqlop.doDelete(RESERVATION_PLANS_TABLE, key, credential);
  }

  @Override
  protected void removeRMDTMasterKeyState(DelegationKey delegationKey) throws Exception {
    String key = RM_DT_SECRET_MANAGER_ROOT + SEPARATOR + DELEGATION_KEY_PREFIX + delegationKey.getKeyId();
    sqlop.doDelete(RMMASTERKEYS_TABLE, key, credential);
  }

  @Override
  protected void storeOrUpdateAMRMTokenSecretManagerState(AMRMTokenSecretManagerState amrmTokenSecretManagerState,
      boolean isUpdate) throws Exception {
    AMRMTokenSecretManagerState data = AMRMTokenSecretManagerState.newInstance(amrmTokenSecretManagerState);
    byte[] stateData = data.getProto().toByteArray();
    try {
      sqlop.doUpdateOrInsert(RMMASTERKEYS_TABLE, AMRMTOKEN_SECRET_MANAGER_ROOT, stateData, credential);
    } catch (Exception ex) {
      LOG.info("Error storing info for AMRMTokenSecretManager", ex);
      notifyStoreOperationFailed(ex);
    }
  }

  @Override
  protected void removeApplicationStateInternal(ApplicationStateData appState) throws Exception {

    String appId = appState.getApplicationSubmissionContext().getApplicationId().toString();

    Operation applicationDelete = new Operation();
    List<Object> deleteApplicationArgs = new ArrayList<>();
    deleteApplicationArgs.add(APPLICATION_TABLE);
    deleteApplicationArgs.add(appId);
    deleteApplicationArgs.add(appId + "z");
    applicationDelete.put(SQLConstants.DELETE_RANGE_KEY, deleteApplicationArgs);

    Operation attemptsDelete = new Operation();
    List<Object> deleteAttemptsArgs = new ArrayList<>();
    deleteAttemptsArgs.add(ATTEMPT_TABLE);
    deleteAttemptsArgs.add(appId);
    deleteAttemptsArgs.add(appId + "z");
    attemptsDelete.put(SQLConstants.DELETE_RANGE_KEY, deleteAttemptsArgs);

    List<Operation> toDelete = new ArrayList<>();
    toDelete.add(applicationDelete);
    toDelete.add(attemptsDelete);

    sqlop.doMultipleOp(toDelete, credential);
  }

  @Override
  protected void removeApplicationAttemptInternal(ApplicationAttemptId attemptId) throws Exception {
    String key = attemptId.getApplicationId().toString() + SEPARATOR + attemptId;
    sqlop.doDelete(ATTEMPT_TABLE, key, credential);
  }

  @Override
  public void deleteStore() throws Exception {
    // Drop All the tables;
    sqlop.doDropTable(CREDENTIAL_TABLE);
    sqlop.doDropTable(APPLICATION_TABLE);
    sqlop.doDropTable(ATTEMPT_TABLE);
    sqlop.doDropTable(RMDELEGATAION_TABLE);
    sqlop.doDropTable(RMMASTERKEYS_TABLE);
    sqlop.doDropTable(RESERVATION_PLANS_TABLE);
  }

  @Override
  public void removeApplication(ApplicationId removeAppId) throws Exception {
    sqlop.doDelete(APPLICATION_TABLE, removeAppId.toString(), credential);
  }

  private class VerifyActiveStatusThread extends Thread {
    VerifyActiveStatusThread() {
      super(VerifyActiveStatusThread.class.getName());
    }

    @Override
    public void run() {
      try {
        while (true) {
          LOG.debug("Verifying master check");
          sqlop.doFencingCheck(credential);
          Thread.sleep(verificationTimeOut);
        }
      } catch (InterruptedException ie) {
        LOG.info(VerifyActiveStatusThread.class.getName() + " thread " + "interrupted! Exiting!");
      } catch (Exception e) {
        notifyStoreOperationFailed(new StoreFencedException());
      }
    }
  }
}
