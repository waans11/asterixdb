/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.transaction.management.opcallbacks;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.AbstractOperationCallback;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback.Operation;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleWriter;

public abstract class AbstractIndexModificationOperationCallback extends AbstractOperationCallback {

    private static final byte INSERT_OP = (byte) IndexOperation.INSERT.ordinal();
    private static final byte DELETE_OP = (byte) IndexOperation.DELETE.ordinal();
    protected final long resourceId;
    protected final byte resourceType;
    protected final IndexOperation indexOp;
    protected final ITransactionSubsystem txnSubsystem;
    protected final ILogRecord logRecord;

    protected AbstractIndexModificationOperationCallback(int datasetId, int[] primaryKeyFields,
            ITransactionContext txnCtx, ILockManager lockManager, ITransactionSubsystem txnSubsystem, long resourceId,
            int resourcePartition, byte resourceType, IndexOperation indexOp) {
        super(datasetId, primaryKeyFields, txnCtx, lockManager);
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.indexOp = indexOp;
        this.txnSubsystem = txnSubsystem;
        logRecord = new LogRecord();
        logRecord.setTxnCtx(txnCtx);
        logRecord.setLogType(LogType.UPDATE);
        logRecord.setJobId(txnCtx.getJobId().getId());
        logRecord.setDatasetId(datasetId);
        logRecord.setResourceId(resourceId);
        logRecord.setResourcePartition(resourcePartition);
        logRecord.setNewOp((byte) (indexOp.ordinal()));
    }

    protected void log(int PKHash, ITupleReference newValue, ITupleReference oldValue) throws ACIDException {
        logRecord.setPKHashValue(PKHash);
        logRecord.setPKFields(primaryKeyFields);
        logRecord.setPKValue(newValue);
        logRecord.computeAndSetPKValueSize();
        if (newValue != null) {
            logRecord.setNewValueSize(SimpleTupleWriter.INSTANCE.bytesRequired(newValue));
            logRecord.setNewValue(newValue);
        } else {
            logRecord.setNewValueSize(0);
        }
        if (oldValue != null) {
            logRecord.setOldValueSize(SimpleTupleWriter.INSTANCE.bytesRequired(oldValue));
            logRecord.setOldValue(oldValue);
        } else {
            logRecord.setOldValueSize(0);
        }
        logRecord.computeAndSetLogSize();
        txnSubsystem.getLogManager().log(logRecord);
    }

    public void setOp(Operation op) throws HyracksDataException {
        switch (op) {
            case DELETE:
                logRecord.setNewOp(DELETE_OP);
                break;
            case INSERT:
                logRecord.setNewOp(INSERT_OP);
                break;
        }
    }
}
