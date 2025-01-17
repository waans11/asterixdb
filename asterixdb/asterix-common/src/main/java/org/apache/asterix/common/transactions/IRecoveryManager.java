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
package org.apache.asterix.common.transactions;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Provides API for failure recovery. Failure could be at application level and
 * require a roll back or could be at system level (crash) and require a more
 * sophisticated mechanism of replaying logs and bringing the system to a
 * consistent state ensuring durability.
 */
public interface IRecoveryManager {

    public enum SystemState {
        INITIAL_RUN,
        NEW_UNIVERSE,
        RECOVERING,
        HEALTHY,
        CORRUPTED
    }

    public class ResourceType {
        public static final byte LSM_BTREE = 0;
        public static final byte LSM_RTREE = 1;
        public static final byte LSM_INVERTED_INDEX = 2;
    }

    /**
     * Returns the state of the system.
     * Health state of the system could be any one of the following. RECOVERING:
     * The system is recovering HEALTHY: The system is in healthy state
     * CORRUPTEED: The system is in corrupted state. This happens when a
     * rollback or recovery task fails. In this state the system is unusable.
     *
     * @see SystemState
     * @return SystemState The state of the system
     * @throws ACIDException
     */
    SystemState getSystemState() throws ACIDException;

    /**
     * Initiates a crash recovery.
     *
     * @param synchronous
     *            indicates if the recovery is to be done in a synchronous
     *            manner. In asynchronous mode, the recovery will happen as part
     *            of a separate thread.
     * @return SystemState the state of the system (@see SystemState) post
     *         recovery.
     * @throws ACIDException
     */
    public void startRecovery(boolean synchronous) throws IOException, ACIDException;

    /**
     * Rolls back a transaction.
     *
     * @param txnContext
     *            the transaction context associated with the transaction
     * @throws ACIDException
     */
    public void rollbackTransaction(ITransactionContext txnContext) throws ACIDException;

    /**
     * @return min first LSN of the open indexes (including remote indexes if replication is enabled)
     * @throws HyracksDataException
     */
    public long getMinFirstLSN() throws HyracksDataException;

    /**
     * @return min first LSN of the open indexes
     * @throws HyracksDataException
     */
    public long getLocalMinFirstLSN() throws HyracksDataException;

    /**
     * Replay the logs that belong to the passed {@code partitions} starting from the {@code lowWaterMarkLSN}
     *
     * @param partitions
     * @param lowWaterMarkLSN
     * @param failedNode
     * @throws IOException
     * @throws ACIDException
     */
    public void replayPartitionsLogs(Set<Integer> partitions, ILogReader logReader, long lowWaterMarkLSN)
            throws IOException, ACIDException;

    /**
     * Creates a temporary file to be used during recovery
     *
     * @param jobId
     * @param fileName
     * @return A file to the created temporary file
     * @throws IOException
     *             if the file for the specified {@code jobId} with the {@code fileName} already exists
     */
    public File createJobRecoveryFile(int jobId, String fileName) throws IOException;

    /**
     * Deletes all temporary recovery files
     */
    public void deleteRecoveryTemporaryFiles();

    void startLocalRecovery(Set<Integer> partitions) throws IOException, ACIDException;
}
