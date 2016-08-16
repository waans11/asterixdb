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
package org.apache.hyracks.dataflow.std.group;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ISpillableTable {

    /**
     * Result Type for an insertion.
     */
    public enum InsertResultType {
        SUCCESS,
        FAIL,
        // If a memory budget is given and if an insertion is successful,
        // but exceeds the given budget, we return this code.
        SUCCESS_BUT_EXCEEDS_BUDGET
    }

    /**
     * Release all the storage resources.
     * @throws HyracksDataException
     */
    void close() throws HyracksDataException;

    /**
     * Reset the specific partition to the initial state. The occupied resources will be released.
     * @param partition
     * @throws HyracksDataException
     */
    void clear(int partition) throws HyracksDataException;

    /**
     * Insert the specific tuple into the table.
     * @param accessor
     * @param tIndex
     * @return
     * @throws HyracksDataException
     */
    InsertResultType insert(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException;

    /**
     * Flush the certain partition to writer, and return the numOfTuples that have been flushed
     * @param partition
     * @param writer
     * @param type
     * @return
     * @throws HyracksDataException
     */
    int flushFrames(int partition, IFrameWriter writer, AggregateType type) throws HyracksDataException;

    /**
     * Get number of partitions
     */
    int getNumPartitions();

    /**
     * Get number of used frames
     */
    int getNumFrames();

    /**
     * Returns true if the number of used frames exceed the budget.
     */
    boolean isUsedNumFramesExceedBudget();

    /**
     * When the table is full, it will return a proper partition which will be the flush() candidate.
     * The {@code accessor} and {@code tIndex} given the reference to the tuple to be inserted.
     * @return the partition id of the victim, -1 if it failed to find a partition
     * @param accessor
     * @param tIndex
     */
    int findVictimPartition(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException;
}
