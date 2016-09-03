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

import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

public class HashSpillableTableFactory implements ISpillableTableFactory {

    private static Logger LOGGER = Logger.getLogger(HashSpillableTableFactory.class.getName());
    private static final double FUDGE_FACTOR = 1.1;
    private static final long serialVersionUID = 1L;
    private final IBinaryHashFunctionFamily[] hashFunctionFamilies;

    public HashSpillableTableFactory(IBinaryHashFunctionFamily[] hashFunctionFamilies) {
        this.hashFunctionFamilies = hashFunctionFamilies;
    }

    @Override
    public ISpillableTable buildSpillableTable(final IHyracksTaskContext ctx, int suggestTableSize, long dataBytesSize,
            final int[] keyFields, final IBinaryComparator[] comparators,
            final INormalizedKeyComputer firstKeyNormalizerFactory, IAggregatorDescriptorFactory aggregateFactory,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, final int framesLimit,
            final int seed) throws HyracksDataException {
        final int tableSize = suggestTableSize;

        // We check whether the given table size is within the budget of groupFrameLimit.
        int expectedByteSizeOfHashTableForGroupBy = SerializableHashTable.getExpectedByteSizeOfHashTable(tableSize,
                ctx.getInitialFrameSize());

        // For HashTable, we need to have at least two frames (one for header and one for content).
        // For DataTable, we need to have at least two frames.
        // The expected hash table size should be within the budget.
        if (framesLimit < 4 || expectedByteSizeOfHashTableForGroupBy >= ctx.getInitialFrameSize() * framesLimit) {
            throw new HyracksDataException("The given frame limit is too small to partition the data.");
        }

        final int[] intermediateResultKeys = new int[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            intermediateResultKeys[i] = i;
        }

        final FrameTuplePairComparator ftpcInputCompareToAggregate = new FrameTuplePairComparator(keyFields,
                intermediateResultKeys, comparators);

        final ITuplePartitionComputer tpc = new FieldHashPartitionComputerFamily(keyFields, hashFunctionFamilies)
                .createPartitioner(seed);

        // For calculating hash value from the already aggregated tuples (not incoming tuples)
        // This computer is needed for doing the garbage collection work on Hash Table.
        final ITuplePartitionComputer tpc1 = new FieldHashPartitionComputerFamily(intermediateResultKeys,
                hashFunctionFamilies).createPartitioner(seed);

        final IAggregatorDescriptor aggregator = aggregateFactory.createAggregator(ctx, inRecordDescriptor,
                outRecordDescriptor, keyFields, intermediateResultKeys, null);

        final AggregateState aggregateState = aggregator.createAggregateStates();

        final ArrayTupleBuilder stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        //TODO(jf) research on the optimized partition size
        final int numPartitions = getNumOfPartitions(
                (int) ((dataBytesSize + expectedByteSizeOfHashTableForGroupBy) / ctx.getInitialFrameSize()),
                framesLimit - 1);
        final int entriesPerPartition = (int) Math.ceil(1.0 * tableSize / numPartitions);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("create hashtable, table size:" + tableSize + " file size:" + dataBytesSize + "  partitions:"
                    + numPartitions);
        }

        final ArrayTupleBuilder outputTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        final ISerializableTable hashTableForTuplePointer = new SerializableHashTable(tableSize, ctx);

        return new ISpillableTable() {

            private final TuplePointer pointer = new TuplePointer();
            private final BitSet spilledSet = new BitSet(numPartitions);
            final IPartitionedTupleBufferManager bufferManager = new VPartitionTupleBufferManager(ctx,
                    PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledSet),
                    numPartitions, framesLimit * ctx.getInitialFrameSize());

            final ITuplePointerAccessor bufferAccessor = bufferManager.getTuplePointerAccessor(outRecordDescriptor);

            private final PreferToSpillFullyOccupiedFramePolicy spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(
                    bufferManager, spilledSet, ctx.getInitialFrameSize());

            private final FrameTupleAppender outputAppender = new FrameTupleAppender(new VSizeFrame(ctx));

            @Override
            public void close() throws HyracksDataException {
                hashTableForTuplePointer.close();
                aggregator.close();
            }

            @Override
            public void clear(int partition) throws HyracksDataException {
                for (int p = getFirstEntryInHashTable(partition); p < getLastEntryInHashTable(partition); p++) {
                    hashTableForTuplePointer.delete(p);
                }

                // Garbage Collection on Hash Table
                int numberOfPagesReclaimed = hashTableForTuplePointer.executeGarbageCollection(bufferAccessor, tpc1);
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine(
                            "Garbage Collection on Hash table is done. Deallocated pages:" + numberOfPagesReclaimed);
                }

                bufferManager.clearPartition(partition);
            }

            private int getPartition(int entryInHashTable) {
                return entryInHashTable / entriesPerPartition;
            }

            private int getFirstEntryInHashTable(int partition) {
                return partition * entriesPerPartition;
            }

            private int getLastEntryInHashTable(int partition) {
                return Math.min(tableSize, (partition + 1) * entriesPerPartition);
            }

            @Override
            public InsertResultType insert(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int entryInHashTable = tpc.partition(accessor, tIndex, tableSize);
                for (int i = 0; i < hashTableForTuplePointer.getTupleCount(entryInHashTable); i++) {
                    hashTableForTuplePointer.getTuplePointer(entryInHashTable, i, pointer);
                    bufferAccessor.reset(pointer);
                    int c = ftpcInputCompareToAggregate.compare(accessor, tIndex, bufferAccessor);
                    if (c == 0) {
                        aggregateExistingTuple(accessor, tIndex, bufferAccessor, pointer.getTupleIndex());
                        return InsertResultType.SUCCESS;
                    }
                }
                return insertNewAggregateEntry(entryInHashTable, accessor, tIndex);
            }

            private InsertResultType insertNewAggregateEntry(int entryInHashTable, IFrameTupleAccessor accessor,
                    int tIndex)
                    throws HyracksDataException {
                initStateTupleBuilder(accessor, tIndex);
                int pid = getPartition(entryInHashTable);


                if (!bufferManager.insertTuple(pid, stateTupleBuilder.getByteArray(),
                        stateTupleBuilder.getFieldEndOffsets(), 0, stateTupleBuilder.getSize(), pointer)) {
                    return InsertResultType.FAIL;
                }
                hashTableForTuplePointer.insert(entryInHashTable, pointer);
                // If the number of frames allocated to the data table and hash table exceeds the frame limit,
                // we need to spill a partition to the disk to make some space.
                if (isUsedNumFramesExceedBudget()) {
                    return InsertResultType.SUCCESS_BUT_EXCEEDS_BUDGET;
                } else {
                    return InsertResultType.SUCCESS;
                }
            }

            private void initStateTupleBuilder(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                stateTupleBuilder.reset();
                for (int k = 0; k < keyFields.length; k++) {
                    stateTupleBuilder.addField(accessor, tIndex, keyFields[k]);
                }
                aggregator.init(stateTupleBuilder, accessor, tIndex, aggregateState);
            }

            private void aggregateExistingTuple(IFrameTupleAccessor accessor, int tIndex,
                    ITuplePointerAccessor bufferAccessor, int tupleIndex) throws HyracksDataException {
                aggregator.aggregate(accessor, tIndex, bufferAccessor, tupleIndex, aggregateState);
            }

            @Override
            public int flushFrames(int partition, IFrameWriter writer, AggregateType type) throws HyracksDataException {
                int count = 0;
                for (int hashEntryPid = getFirstEntryInHashTable(partition); hashEntryPid < getLastEntryInHashTable(
                        partition); hashEntryPid++) {
                    count += hashTableForTuplePointer.getTupleCount(hashEntryPid);
                    for (int tid = 0; tid < hashTableForTuplePointer.getTupleCount(hashEntryPid); tid++) {
                        hashTableForTuplePointer.getTuplePointer(hashEntryPid, tid, pointer);
                        bufferAccessor.reset(pointer);
                        outputTupleBuilder.reset();
                        for (int k = 0; k < intermediateResultKeys.length; k++) {
                            outputTupleBuilder.addField(bufferAccessor.getBuffer().array(),
                                    bufferAccessor.getAbsFieldStartOffset(intermediateResultKeys[k]),
                                    bufferAccessor.getFieldLength(intermediateResultKeys[k]));
                        }

                        boolean hasOutput = false;
                        switch (type) {
                            case PARTIAL:
                                hasOutput = aggregator.outputPartialResult(outputTupleBuilder, bufferAccessor,
                                        pointer.getTupleIndex(), aggregateState);
                                break;
                            case FINAL:
                                hasOutput = aggregator.outputFinalResult(outputTupleBuilder, bufferAccessor,
                                        pointer.getTupleIndex(), aggregateState);
                                break;
                        }

                        if (hasOutput && !outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            outputAppender.write(writer, true);
                            if (!outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                    outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                                throw new HyracksDataException("The output item is too large to be fit into a frame.");
                            }
                        }
                    }
                }
                outputAppender.write(writer, true);
                spilledSet.set(partition);
                return count;
            }

            @Override
            public int getNumPartitions() {
                return bufferManager.getNumPartitions();
            }

            @Override
            public int getNumFrames() {
                return bufferManager.getFrameCount() + hashTableForTuplePointer.getFrameCount();
            }

            @Override
            public boolean isUsedNumFramesExceedBudget() {
                return getNumFrames() > framesLimit;
            }

            @Override
            public int findVictimPartition(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int entryInHashTable = tpc.partition(accessor, tIndex, tableSize);
                int partition = getPartition(entryInHashTable);
                return spillPolicy.selectVictimPartition(partition);
            }
        };
    }

    private int getNumOfPartitions(int nubmerOfFramesForDataAndHashTable, int frameLimit) {
        if (frameLimit >= nubmerOfFramesForDataAndHashTable * FUDGE_FACTOR) {
            return 1; // all in memory, we will create a big partition
        }
        // The formula is based on Shapiro's paper - http://cs.stanford.edu/people/chrismre/cs345/rl/shapiro.pdf.
        // Check the page 249 for more details.
        int numberOfPartitions = (int) (Math
                .ceil((nubmerOfFramesForDataAndHashTable * FUDGE_FACTOR - frameLimit) / (frameLimit - 1)));
        // Actually, at this stage, we know that this is not a in-memory hash (#frames required > #frameLimit).
        // So we want to guarantee that the number of partition is at least two because there may be corner cases.
        numberOfPartitions = Math.max(2, numberOfPartitions);
        // If the number of partition is greater than the memory budget, there might be a case that we can't
        // allocate at least one frame for each partition in memory. So, we deal with those cases here.
        if (numberOfPartitions > frameLimit) {
            numberOfPartitions = (int) Math.ceil(Math.sqrt(nubmerOfFramesForDataAndHashTable * FUDGE_FACTOR));
            return Math.max(2, Math.min(numberOfPartitions, frameLimit));
        }
        return numberOfPartitions;
    }

}
