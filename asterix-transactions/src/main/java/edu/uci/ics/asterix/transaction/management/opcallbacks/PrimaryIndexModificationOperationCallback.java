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

package edu.uci.ics.asterix.transaction.management.opcallbacks;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.ILockManager;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;

/**
 * Assumes LSM-BTrees as primary indexes.
 * Performs locking on primary keys, and also logs before/after images.
 */
public class PrimaryIndexModificationOperationCallback extends AbstractIndexModificationOperationCallback implements
        IModificationOperationCallback {

    // For the temporary experiment
    //    Random randomValueGenerator = null;

    public PrimaryIndexModificationOperationCallback(int datasetId, int[] primaryKeyFields, ITransactionContext txnCtx,
            ILockManager lockManager, ITransactionSubsystem txnSubsystem, long resourceId, byte resourceType,
            IndexOperation indexOp) {
        super(datasetId, primaryKeyFields, txnCtx, lockManager, txnSubsystem, resourceId, resourceType, indexOp);
        //        this.randomValueGenerator = new Random(System.currentTimeMillis());
    }

    @Override
    public void before(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            lockManager.lock(datasetId, pkHash, LockMode.X, txnCtx);
            // Temporary for the experiment
            //            try {
            //                Thread.sleep(randomValueGenerator.nextInt(501));// QUERY
            //            } catch (InterruptedException e1) {
            //                // TODO Auto-generated catch block
            //                e1.printStackTrace();
            //            }
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void found(ITupleReference before, ITupleReference after) throws HyracksDataException {
        try {
            int pkHash = computePrimaryKeyHashValue(after, primaryKeyFields);
            log(pkHash, after);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }
}
