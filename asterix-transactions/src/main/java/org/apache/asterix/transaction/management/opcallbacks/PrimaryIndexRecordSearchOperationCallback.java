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
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;

/**
 * Assumes LSM-BTrees as primary indexes. Implements try/locking and unlocking on primary keys.
 * This Callback method acquires/releases a record level lock.
 */
public class PrimaryIndexRecordSearchOperationCallback extends AbstractOperationCallback implements
        ISearchOperationCallback {

    public PrimaryIndexRecordSearchOperationCallback(int datasetId, int[] entityIdFields, ILockManager lockManager,
            ITransactionContext txnCtx) {
        super(datasetId, entityIdFields, txnCtx, lockManager);
    }

    @Override
    // Attempt to get a record level S lock
    public boolean proceed(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            return lockManager.tryLock(datasetId, pkHash, LockMode.S, txnCtx);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    // When proceed() is failed, get a record level S lock
    public void reconcile(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            lockManager.lock(datasetId, pkHash, LockMode.S, txnCtx);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    // We need to decide whether this method should reverse the effect of proceed() or reconcile()
    // or just keep this as no-op
    public void cancelReconcile(ITupleReference tuple) throws HyracksDataException {
        //no op - lock will be released in CommitOp
    }

    @Override
    public void cancelProceed(ITupleReference tuple) throws HyracksDataException {
        //no op - lock will be released in CommitOp
    }

    @Override
    public void complete(ITupleReference tuple) throws HyracksDataException {
        //no op - lock will be released in CommitOp
    }

}
