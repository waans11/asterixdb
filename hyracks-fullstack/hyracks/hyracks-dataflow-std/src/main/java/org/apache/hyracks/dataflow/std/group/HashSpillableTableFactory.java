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
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
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
    private static final int MIN_DATA_TABLE_FRAME_LIMT = 2;
    private static final int MIN_HASH_TABLE_FRAME_LIMT = 2;
    private static final int MIN_FRAME_LIMT = MIN_DATA_TABLE_FRAME_LIMT + MIN_HASH_TABLE_FRAME_LIMT;

    public HashSpillableTableFactory(IBinaryHashFunctionFamily[] hashFunctionFamilies) {
        this.hashFunctionFamilies = hashFunctionFamilies;
    }

    @Override
    public ISpillableTable buildSpillableTable(final IHyracksTaskContext ctx, int suggestTableSize,
            long inputDataBytesSize, final int[] keyFields, final IBinaryComparator[] comparators,
            final INormalizedKeyComputer firstKeyNormalizerFactory, IAggregatorDescriptorFactory aggregateFactory,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, final int framesLimit,
            final int seed) throws HyracksDataException {
        final int tableSize = suggestTableSize;

        // We check whether the given table size is within the budget of the groupFrameLimit.
        int expectedFrameCountOfHashTableForGroupBy = SerializableHashTable.getExpectedTableSizeInFrame(tableSize,
                ctx.getInitialFrameSize());

        // For HashTable, we need to have at least two frames (one for header and one for content).
        // For DataTable, we need to have at least two frames.
        // Also, the expected hash table size should be within the budget.
        if (framesLimit < MIN_FRAME_LIMT
                || expectedFrameCountOfHashTableForGroupBy + MIN_DATA_TABLE_FRAME_LIMT > framesLimit) {
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
        // This computer is needed to calculate the hash value of a aggregated tuple
        // while doing the garbage collection work on Hash Table.
        final ITuplePartitionComputer tpcIntermediate = new FieldHashPartitionComputerFamily(intermediateResultKeys,
                hashFunctionFamilies).createPartitioner(seed);

        final IAggregatorDescriptor aggregator = aggregateFactory.createAggregator(ctx, inRecordDescriptor,
                outRecordDescriptor, keyFields, intermediateResultKeys, null);

        final AggregateState aggregateState = aggregator.createAggregateStates();

        final ArrayTupleBuilder stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        //TODO(jf) research on the optimized partition size
        // Since the memory budget is shared between the data table and hash table, when calculating the number of
        // partitions, we should not make a case where the number of partitions is one.
        int memoryBudget = Math.max(MIN_FRAME_LIMT, framesLimit - 1 - expectedFrameCountOfHashTableForGroupBy);

        final int numPartitions = getNumOfPartitions((int) (inputDataBytesSize / ctx.getInitialFrameSize()),
                memoryBudget);
        final int entriesPerPartition = (int) Math.ceil(1.0 * tableSize / numPartitions);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(
                    "created hashtable, table size:" + tableSize + " file size:" + inputDataBytesSize + "  #partitions:"
                    + numPartitions);
        }

        final ArrayTupleBuilder outputTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        return new ISpillableTable() {

            private final TuplePointer pointer = new TuplePointer();
            private final BitSet spilledSet = new BitSet(numPartitions);
            // This frame pool will be shared by both data table and hash table.
            private final IDeallocatableFramePool framePool = new DeallocatableFramePool(ctx,
                    framesLimit * ctx.getInitialFrameSize());
            // buffer manager for hash table
            private final ISimpleFrameBufferManager bufferManagerForHashTable = new FramePoolBackedFrameBufferManager(
                    framePool);

            private final ISerializableTable hashTableForTuplePointer = new SerializableHashTable(tableSize, ctx,
                    bufferManagerForHashTable);

            // buffer manager for data table
            final IPartitionedTupleBufferManager bufferManager = new VPartitionTupleBufferManager(
                    PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledSet),
                    numPartitions, framePool);

            final ITuplePointerAccessor bufferAccessor = bufferManager.getTuplePointerAccessor(outRecordDescriptor);

            private final PreferToSpillFullyOccupiedFramePolicy spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(
                    bufferManager, spilledSet);

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

                // Check whether the garbage collection is required and conduct a garbage collection if so.
                if (hashTableForTuplePointer.isGarbageCollectionNeeded()) {
                    int numberOfFramesReclaimed = hashTableForTuplePointer.executeGarbageCollection(bufferAccessor,
                            tpcIntermediate);
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("Garbage Collection on Hash table is done. Deallocated frames:"
                                + numberOfFramesReclaimed);
                    }
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
            public boolean insert(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int entryInHashTable = tpc.partition(accessor, tIndex, tableSize);
                for (int i = 0; i < hashTableForTuplePointer.getTupleCount(entryInHashTable); i++) {
                    hashTableForTuplePointer.getTuplePointer(entryInHashTable, i, pointer);
                    bufferAccessor.reset(pointer);
                    int c = ftpcInputCompareToAggregate.compare(accessor, tIndex, bufferAccessor);
                    if (c == 0) {
                        aggregateExistingTuple(accessor, tIndex, bufferAccessor, pointer.getTupleIndex());
                        return true;
                    }
                }
                return insertNewAggregateEntry(entryInHashTable, accessor, tIndex);
            }

            /**
             * Insert a new aggregate entry into the data table and hash table.
             * This insertion must be an atomic operation. We cannot have partial success (failure).
             */
            private boolean insertNewAggregateEntry(int entryInHashTable, IFrameTupleAccessor accessor,
                    int tIndex)
                    throws HyracksDataException {
                initStateTupleBuilder(accessor, tIndex);
                int pid = getPartition(entryInHashTable);

                if (!bufferManager.insertTuple(pid, stateTupleBuilder.getByteArray(),
                        stateTupleBuilder.getFieldEndOffsets(), 0, stateTupleBuilder.getSize(), pointer)) {
                    return false;
                }

                if (!hashTableForTuplePointer.insert(entryInHashTable, pointer)) {
                    // To preserve the atomicity of this method, we need to undo the effect
                    // of the above bufferManager.insertTuple() call.
                    bufferManager.cancelInsertTuple(pid);
                    return false;
                }

                return true;
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
            public int findVictimPartition(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int entryInHashTable = tpc.partition(accessor, tIndex, tableSize);
                int partition = getPartition(entryInHashTable);
                return spillPolicy.selectVictimPartition(partition);
            }
        };
    }

    /**
     * Calculate the number of partitions for Data table. The formula is from Shapiro's paper -
     * http://cs.stanford.edu/people/chrismre/cs345/rl/shapiro.pdf. Check the page 249 for more details.
     * If the required number of frames is greater than the number of available frames, we make sure that
     * at least two partitions will be created. Also, if the number of partitions is greater than the memory budget,
     * we may not allocate at least one frame for each partition in memory. So, we also deal with those cases
     * at the final part of the method.
     */
    private int getNumOfPartitions(int nubmerOfInputFrames, int frameLimit) {
        if (frameLimit >= nubmerOfInputFrames * FUDGE_FACTOR) {
            return 1; // all in memory, we will create a big partition.
        }
        int numberOfPartitions = (int) (Math
                .ceil((nubmerOfInputFrames * FUDGE_FACTOR - frameLimit) / (frameLimit - 1)));
        numberOfPartitions = Math.max(2, numberOfPartitions);
        if (numberOfPartitions > frameLimit) {
            numberOfPartitions = (int) Math.ceil(Math.sqrt(nubmerOfInputFrames * FUDGE_FACTOR));
            return Math.max(2, Math.min(numberOfPartitions, frameLimit));
        }
        return numberOfPartitions;
    }

}
