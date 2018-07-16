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

import java.util.ArrayList;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.group.AggregateType;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.ISpillableTable;
import org.apache.hyracks.dataflow.std.group.ISpillableTableFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExternalGroupWriteOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable
        implements IRunFileWriterGenerator {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IHyracksTaskContext ctx;
    private final Object stateId;
    private final ISpillableTableFactory spillableTableFactory;
    private final RecordDescriptor partialAggRecordDesc;
    private final RecordDescriptor outRecordDesc;
    private final IAggregatorDescriptorFactory mergeAggregatorFactory;
    private final int[] mergeGroupFields;
    private final IBinaryComparator[] groupByComparators;
    private final int frameLimit;
    private final INormalizedKeyComputer nmkComputer;
    private final ArrayList<RunFileWriter> generatedRuns = new ArrayList<>();

    // Temp :
    private final boolean limitMemory;
    private final boolean hashTableGarbageCollection;

    // Original const - confirmed that it is only called from a test
    public ExternalGroupWriteOperatorNodePushable(IHyracksTaskContext ctx, Object stateId,
            ISpillableTableFactory spillableTableFactory, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, int framesLimit, int[] groupFields,
            INormalizedKeyComputerFactory nmkFactory, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptorFactory aggregatorFactory) {
        this.ctx = ctx;
        this.stateId = stateId;
        this.spillableTableFactory = spillableTableFactory;
        frameLimit = framesLimit;
        nmkComputer = nmkFactory == null ? null : nmkFactory.createNormalizedKeyComputer();

        this.partialAggRecordDesc = partialAggRecordDesc;
        this.outRecordDesc = outRecordDesc;

        mergeAggregatorFactory = aggregatorFactory;

        //create merge group fields
        int numGroupFields = groupFields.length;
        mergeGroupFields = new int[numGroupFields];
        for (int i = 0; i < numGroupFields; i++) {
            mergeGroupFields[i] = i;
        }

        //setup comparators for grouping
        groupByComparators = new IBinaryComparator[Math.min(mergeGroupFields.length, comparatorFactories.length)];
        for (int i = 0; i < groupByComparators.length; i++) {
            groupByComparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        // Temp :
        limitMemory = true;
        hashTableGarbageCollection = true;
    }

    // Temp :
    public ExternalGroupWriteOperatorNodePushable(IHyracksTaskContext ctx, Object stateId,
            ISpillableTableFactory spillableTableFactory, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, int framesLimit, int[] groupFields,
            INormalizedKeyComputerFactory nmkFactory, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptorFactory aggregatorFactory, boolean limitMemory, boolean hashTableGarbageCollection) {
        this.ctx = ctx;
        this.stateId = stateId;
        this.spillableTableFactory = spillableTableFactory;
        frameLimit = framesLimit;
        nmkComputer = nmkFactory == null ? null : nmkFactory.createNormalizedKeyComputer();

        this.partialAggRecordDesc = partialAggRecordDesc;
        this.outRecordDesc = outRecordDesc;

        mergeAggregatorFactory = aggregatorFactory;

        //create merge group fields
        int numGroupFields = groupFields.length;
        mergeGroupFields = new int[numGroupFields];
        for (int i = 0; i < numGroupFields; i++) {
            mergeGroupFields[i] = i;
        }

        //setup comparators for grouping
        groupByComparators = new IBinaryComparator[Math.min(mergeGroupFields.length, comparatorFactories.length)];
        for (int i = 0; i < groupByComparators.length; i++) {
            groupByComparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        // Temp :
        this.limitMemory = limitMemory;
        this.hashTableGarbageCollection = hashTableGarbageCollection;
    }

    @Override
    public void initialize() throws HyracksDataException {
        ExternalGroupState aggState = (ExternalGroupState) ctx.getStateObject(stateId);
        ISpillableTable table = aggState.getSpillableTable();
        RunFileWriter[] partitionRuns = aggState.getRuns();
        int[] numberOfTuples = aggState.getSpilledNumTuples();
        try {
            // Temp :
            LOGGER.log(Level.INFO, this.hashCode() + "\t" + "initialize" + "\tSTART");
            //
            writer.open();
            doPass(table, partitionRuns, numberOfTuples, writer, 1); // level 0 use used at build stage.
            // Temp :
            LOGGER.log(Level.INFO, this.hashCode() + "\t" + "initialize" + "\tFINISH");
            //
        } catch (Exception e) {
            try {
                for (RunFileWriter run : generatedRuns) {
                    run.erase();
                }
            } finally {
                writer.fail();
            }
            throw e;
        } finally {
            writer.close();
        }
    }

    private void doPass(ISpillableTable table, RunFileWriter[] runs, int[] numOfTuples, IFrameWriter writer, int level)
            throws HyracksDataException {
        assert table.getNumPartitions() == runs.length;
        // Temp :
        int inMemoryPartitionCount = 0;
        //
        for (int i = 0; i < runs.length; i++) {
            if (runs[i] == null) {
                table.flushFrames(i, writer, AggregateType.FINAL);
                // Temp :
                inMemoryPartitionCount++;
                //
            }
        }
        // Temp :
        String result = table.printInfo();
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "doPass" + "\tlevel:\t" + level + "\truns_size:\t" + runs.length
                + "\tprocessed_in-memory_part#:\t" + inMemoryPartitionCount + "\tremaining:\t"
                + (runs.length - inMemoryPartitionCount) + "\tall_in_memory:\t"
                + ((runs.length - inMemoryPartitionCount) == 0) + "\twriter:\t" + writer.toString() + "\n" + result);
        //
        table.close();

        for (int i = 0; i < runs.length; i++) {
            if (runs[i] != null) {
                // Calculates the hash table size (# of unique hash values) based on the budget and a tuple size.
                int memoryBudgetInBytes = ctx.getInitialFrameSize() * frameLimit;
                int groupByColumnsCount = mergeGroupFields.length;
                int hashTableCardinality = ExternalGroupOperatorDescriptor.calculateGroupByTableCardinality(
                        memoryBudgetInBytes, groupByColumnsCount, ctx.getInitialFrameSize());
                hashTableCardinality = Math.min(hashTableCardinality, numOfTuples[i]);
                ISpillableTable partitionTable = spillableTableFactory.buildSpillableTable(ctx, hashTableCardinality,
                        runs[i].getFileSize(), mergeGroupFields, groupByComparators, nmkComputer,
                        mergeAggregatorFactory, partialAggRecordDesc, outRecordDesc, frameLimit, level, limitMemory,
                        hashTableGarbageCollection);
                RunFileWriter[] runFileWriters = new RunFileWriter[partitionTable.getNumPartitions()];
                int[] sizeInTuplesNextLevel =
                        buildGroup(runs[i].createDeleteOnCloseReader(), partitionTable, runFileWriters, i, level);
                for (int idFile = 0; idFile < runFileWriters.length; idFile++) {
                    if (runFileWriters[idFile] != null) {
                        generatedRuns.add(runFileWriters[idFile]);
                    }
                }

                if (LOGGER.isDebugEnabled()) {
                    int numOfSpilledPart = 0;
                    for (int x = 0; x < numOfTuples.length; x++) {
                        if (numOfTuples[x] > 0) {
                            numOfSpilledPart++;
                        }
                    }
                    LOGGER.debug("level " + level + ":" + "build with " + numOfTuples.length + " partitions"
                            + ", spilled " + numOfSpilledPart + " partitions");
                }
                doPass(partitionTable, runFileWriters, sizeInTuplesNextLevel, writer, level + 1);
            }
        }

        // Temp :

    }

    private int[] buildGroup(RunFileReader reader, ISpillableTable table, RunFileWriter[] runFileWriters, int partition,
            int level) throws HyracksDataException {
        ExternalHashGroupBy groupBy = new ExternalHashGroupBy(this, table, runFileWriters, partialAggRecordDesc);
        reader.open();
        try {
            VSizeFrame frame = new VSizeFrame(ctx);
            while (reader.nextFrame(frame)) {
                groupBy.insert(frame.getBuffer());
            }
            groupBy.flushSpilledPartitions();
        } finally {
            reader.close();
        }
        // Temp :
        String result = groupBy.printInfo();
        int totalSpilledTupleNum = 0;
        for (int i = 0; i < groupBy.getSpilledNumTuples().length; i++) {
            if (groupBy.getSpilledNumTuples()[i] > 0) {
                totalSpilledTupleNum += groupBy.getSpilledNumTuples()[i];
            }
        }
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "buildGroup" + "\tlevel:\t" + level + "\tpartition:\t"
                + partition + "\t#spilled tuples:\t" + totalSpilledTupleNum + "\n" + result);
        //
        return groupBy.getSpilledNumTuples();
    }

    @Override
    public RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference newRun = ctx.getJobletContext()
                .createManagedWorkspaceFile(ExternalGroupOperatorDescriptor.class.getSimpleName());
        return new RunFileWriter(newRun, ctx.getIoManager());
    }
}
