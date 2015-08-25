/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * Assumes LSM-BTrees as primary indexes. Implements try/locking and unlocking
 * on primary keys. This Callback method acquires/releases a record level
 * instant lock.
 */
public class PrimaryIndexInstantRecordSearchOperationCallback extends AbstractOperationCallback implements
        ISearchOperationCallback {

    public PrimaryIndexInstantRecordSearchOperationCallback(int datasetId, int[] entityIdFields,
            ILockManager lockManager, ITransactionContext txnCtx) {
        super(datasetId, entityIdFields, txnCtx, lockManager);
    }

    @Override
    public boolean proceed(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            return lockManager.instantTryLock(datasetId, pkHash, LockMode.S, txnCtx);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    // Get a lock after proceed() is failed
    public void reconcile(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            lockManager.lock(datasetId, pkHash, LockMode.S, txnCtx);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    // Reverse the effect of reconcile()
    public void cancelReconcile(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            lockManager.unlock(datasetId, pkHash, LockMode.S, txnCtx);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    // Reverse the effect of proceed()
    public void cancelProceed(ITupleReference tuple) throws HyracksDataException {
        // Do nothing since proceed() is the instantTryLock(). Nothing was kept in the lockManager.
    }

    @Override
    // Release the lock acquired from reconcile()
    public void complete(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            lockManager.unlock(datasetId, pkHash, LockMode.S, txnCtx);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

}
