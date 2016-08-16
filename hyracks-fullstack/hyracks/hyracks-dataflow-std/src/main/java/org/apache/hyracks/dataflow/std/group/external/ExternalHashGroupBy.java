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

package org.apache.hyracks.dataflow.std.group.external;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.group.AggregateType;
import org.apache.hyracks.dataflow.std.group.ISpillableTable;
import org.apache.hyracks.dataflow.std.group.ISpillableTable.InsertResultType;

public class ExternalHashGroupBy {

    private final IRunFileWriterGenerator runFileWriterGenerator;

    private FrameTupleAccessor accessor;
    private ISpillableTable table;
    private RunFileWriter[] runWriters;
    private int[] spilledNumTuples;

    public ExternalHashGroupBy(IRunFileWriterGenerator runFileWriterGenerator, ISpillableTable table,
            RunFileWriter[] runWriters, RecordDescriptor inRecordDescriptor) {
        this.runFileWriterGenerator = runFileWriterGenerator;
        this.table = table;
        this.runWriters = runWriters;
        this.accessor = new FrameTupleAccessor(inRecordDescriptor);
        this.spilledNumTuples = new int[runWriters.length];
    }

    /**
     * Inserts tuples to the spillable table.
     * If an insertion was not successful, that means there are not enough frames.
     * Thus, this method tries to spill a data partition to disk and tries that insertion again.
     * If the insertion was successful, but that insertion consumed all budget,
     * the only issue was that it has exceeded the budget slightly.
     * Like the failing case, we try to spill a partition to the disk until the number of frames
     * that we are used is within the budget.
     */
    public void insert(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            InsertResultType result = table.insert(accessor, i);
            if (result == InsertResultType.FAIL || result == InsertResultType.SUCCESS_BUT_EXCEEDS_BUDGET) {
                do {
                    int partition = table.findVictimPartition(accessor, i);
                    if (partition < 0) {
                        throw new HyracksDataException("Failed to insert a new buffer into the aggregate operator!");
                    }
                    RunFileWriter writer = getPartitionWriterOrCreateOneIfNotExist(partition);
                    flushPartitionToRun(partition, writer);
                    if (result == InsertResultType.SUCCESS_BUT_EXCEEDS_BUDGET) {
                        if (!table.isUsedNumFramesExceedBudget()) {
                            // If the table conforms to the budget, we can stop here.
                            // If not, we continue to spill another partitions.
                            result = InsertResultType.SUCCESS;
                        }
                    } else {
                        result = table.insert(accessor, i);
                    }
                } while (result != InsertResultType.SUCCESS);
            }
        }
    }

    private void flushPartitionToRun(int partition, RunFileWriter writer)
            throws HyracksDataException {
        try {
            spilledNumTuples[partition] += table.flushFrames(partition, writer, AggregateType.PARTIAL);
            table.clear(partition);
        } catch (Exception ex) {
            writer.fail();
            throw new HyracksDataException(ex);
        }
    }

    public void flushSpilledPartitions() throws HyracksDataException {
        for (int i = 0; i < runWriters.length; ++i) {
            if (runWriters[i] != null) {
                flushPartitionToRun(i, runWriters[i]);
                runWriters[i].close();
            }
        }
    }

    private RunFileWriter getPartitionWriterOrCreateOneIfNotExist(int partition) throws HyracksDataException {
        if (runWriters[partition] == null) {
            runWriters[partition] = runFileWriterGenerator.getRunFileWriter();
            runWriters[partition].open();
        }
        return runWriters[partition];
    }

    public int[] getSpilledNumTuples() {
        return spilledNumTuples;
    }
}
