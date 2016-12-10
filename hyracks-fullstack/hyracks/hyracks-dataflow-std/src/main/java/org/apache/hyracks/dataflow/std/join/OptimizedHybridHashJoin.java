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
package org.apache.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

/**
 * This class mainly applies one level of HHJ on a pair of
 * relations. It is always called by the descriptor.
 */
public class OptimizedHybridHashJoin {

    // Used for special probe BigObject which can not be held into the Join memory
    private FrameTupleAppender bigProbeFrameAppender;

    enum SIDE {
        BUILD,
        PROBE
    }

    private IHyracksTaskContext ctx;

    private final String buildRelName;
    private final String probeRelName;

    private final int[] buildKeys;
    private final int[] probeKeys;

    private final IBinaryComparator[] comparators;

    private final ITuplePartitionComputer buildHpc;
    private final ITuplePartitionComputer probeHpc;

    private final RecordDescriptor buildRd;
    private final RecordDescriptor probeRd;

    private RunFileWriter[] buildRFWriters; //writing spilled build partitions
    private RunFileWriter[] probeRFWriters; //writing spilled probe partitions

    private final IPredicateEvaluator predEvaluator;
    private final boolean isLeftOuter;
    private final IMissingWriter[] nonMatchWriters;

    private final BitSet spilledStatus; //0=resident, 1=spilled
    private final int numOfPartitions;
    private final int memSizeInFrames;
    private InMemoryHashJoin inMemJoiner; //Used for joining resident partitions

    private IPartitionedTupleBufferManager bufferManager;
    private PreferToSpillFullyOccupiedFramePolicy spillPolicy;

    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;

    private IDeallocatableFramePool framePool;
    private ISimpleFrameBufferManager bufferManagerForHashTable;

    private boolean isReversed; //Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal

    // stats information
    private int[] buildPSizeInTups;
    private IFrame reloadBuffer;
    private TuplePointer tempPtr = new TuplePointer(); // this is a reusable object to store the pointer,which is not used anywhere.
                                                       // we mainly use it to match the corresponding function signature.
    private int[] probePSizeInTups;

    public OptimizedHybridHashJoin(IHyracksTaskContext ctx, int memSizeInFrames, int numOfPartitions,
            String probeRelName,
            String buildRelName, int[] probeKeys, int[] buildKeys, IBinaryComparator[] comparators,
            RecordDescriptor probeRd, RecordDescriptor buildRd, ITuplePartitionComputer probeHpc,
            ITuplePartitionComputer buildHpc, IPredicateEvaluator predEval, boolean isLeftOuter,
            IMissingWriterFactory[] nullWriterFactories1) {
        this.ctx = ctx;
        this.memSizeInFrames = memSizeInFrames;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;
        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;
        this.comparators = comparators;
        this.buildRelName = buildRelName;
        this.probeRelName = probeRelName;

        this.numOfPartitions = numOfPartitions;
        this.buildRFWriters = new RunFileWriter[numOfPartitions];
        this.probeRFWriters = new RunFileWriter[numOfPartitions];

        this.accessorBuild = new FrameTupleAccessor(buildRd);
        this.accessorProbe = new FrameTupleAccessor(probeRd);

        this.predEvaluator = predEval;
        this.isLeftOuter = isLeftOuter;
        this.isReversed = false;

        this.spilledStatus = new BitSet(numOfPartitions);

        this.nonMatchWriters = isLeftOuter ? new IMissingWriter[nullWriterFactories1.length] : null;
        if (isLeftOuter) {
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nonMatchWriters[i] = nullWriterFactories1[i].createMissingWriter();
            }
        }
    }

    public void initBuild() throws HyracksDataException {
        framePool = new DeallocatableFramePool(ctx, memSizeInFrames * ctx.getInitialFrameSize());
        bufferManagerForHashTable = new FramePoolBackedFrameBufferManager(framePool);
        bufferManager = new VPartitionTupleBufferManager(
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus),
                numOfPartitions, framePool);
        spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(bufferManager, spilledStatus);
        spilledStatus.clear();
        buildPSizeInTups = new int[numOfPartitions];
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tupleCount = accessorBuild.getTupleCount();

        for (int i = 0; i < tupleCount; ++i) {
            int pid = buildHpc.partition(accessorBuild, i, numOfPartitions);
            processTuple(i, pid);
            buildPSizeInTups[pid]++;
        }

    }

    private void processTuple(int tid, int pid) throws HyracksDataException {
        while (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
            selectAndSpillVictim(pid);
        }
    }

    private void selectAndSpillVictim(int pid) throws HyracksDataException {
        int victimPartition = spillPolicy.selectVictimPartition(pid);
        if (victimPartition < 0) {
            throw new HyracksDataException(
                    "No more space left in the memory buffer, please assign more memory to hash-join.");
        }
        spillPartition(victimPartition);
    }

    private void spillPartition(int pid) throws HyracksDataException {
        RunFileWriter writer = getSpillWriterOrCreateNewOneIfNotExist(pid, SIDE.BUILD);
        bufferManager.flushPartition(pid, writer);
        bufferManager.clearPartition(pid);
        spilledStatus.set(pid);
    }

    private RunFileWriter getSpillWriterOrCreateNewOneIfNotExist(int pid, SIDE whichSide) throws HyracksDataException {
        RunFileWriter[] runFileWriters = null;
        String refName = null;
        switch (whichSide) {
            case BUILD:
                runFileWriters = buildRFWriters;
                refName = buildRelName;
                break;
            case PROBE:
                refName = probeRelName;
                runFileWriters = probeRFWriters;
                break;
        }
        RunFileWriter writer = runFileWriters[pid];
        if (writer == null) {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(refName);
            writer = new RunFileWriter(file, ctx.getIOManager());
            writer.open();
            runFileWriters[pid] = writer;
        }
        return writer;
    }

    public void closeBuild() throws HyracksDataException {
        // Flushes the remaining chunks of the all spilled partition to the disk.
        closeAllSpilledPartitions(SIDE.BUILD);

        // Makes the space for the in-memory hash table (some partitions may need to be spilled to the disk
        // during this step in order to makes the space)
        // and tries to bring back as many spilled partitions as possible if there is free space.
        int inMemTupCount = makeSpaceForHashTableAndBringBackSpilledPartitions();

        createInMemoryJoiner(inMemTupCount);

        loadDataInMemJoin();
    }

    /**
     * In case of failure happens, we need to clear up the generated temporary files.
     */
    public void clearBuildTempFiles() {
        for (int i = 0; i < buildRFWriters.length; i++) {
            if (buildRFWriters[i] != null) {
                buildRFWriters[i].getFileReference().delete();
            }
        }
    }

    private void closeAllSpilledPartitions(SIDE whichSide) throws HyracksDataException {
        RunFileWriter[] runFileWriters = null;
        switch (whichSide) {
            case BUILD:
                runFileWriters = buildRFWriters;
                break;
            case PROBE:
                runFileWriters = probeRFWriters;
                break;
        }

        for (int pid = spilledStatus.nextSetBit(0); pid >= 0; pid = spilledStatus.nextSetBit(pid + 1)) {
            if (bufferManager.getNumTuples(pid) > 0) {
                bufferManager.flushPartition(pid, getSpillWriterOrCreateNewOneIfNotExist(pid, whichSide));
                bufferManager.clearPartition(pid);
                runFileWriters[pid].close();
            }
        }
    }

    /**
     * Makes the space for the hash table. If there is no enough space, one or more partitions will be spilled
     * to the disk until the hash table can fit into the memory. After this, bring back spilled partitions
     * if there is available memory.
     *
     * @return the number of tuples in memory after this method is executed.
     * @throws HyracksDataException
     */
    private int makeSpaceForHashTableAndBringBackSpilledPartitions() throws HyracksDataException {
        // we need number of |spilledPartitions| buffers to store the probe data
        int frameSize = ctx.getInitialFrameSize();
        int freeSpace = (memSizeInFrames - spilledStatus.cardinality()) * frameSize;

        // For partitions in main memory, we deduct their size from the free space.
        int inMemTupCount = 0;
        for (int p = spilledStatus.nextClearBit(0); p >= 0
                && p < numOfPartitions; p = spilledStatus.nextClearBit(p + 1)) {
            freeSpace -= bufferManager.getPhysicalSize(p);
            inMemTupCount += buildPSizeInTups[p];
        }

        // Calculates the expected hash table size for the given number of tuples in main memory
        // and deducts it from the free space.
        int hashTableByteSizeForInMemTuples = SerializableHashTable.getExpectedTableSizeInByte(inMemTupCount,
                frameSize);
        freeSpace -= hashTableByteSizeForInMemTuples;

        // In the case where free space is less than zero after considering the hash table size,
        // we need to spill more partitions until we can accommodate the hash table in memory.
        // TODO: there may be different policies (keep spilling minimum, spilling maximum, find a similar size to the
        //                                        hash table, or keep spilling from the first partition)
        boolean moreSpilled = false;

        // No space to accommodate the hash table? Then, we spill one or more partitions to the disk.
        if (freeSpace <= 0) {
            // Tries to find a best-fit partition not to spill many partitions.
            int pidToSpill = selectSinglePartitionToSpill(freeSpace, inMemTupCount, hashTableByteSizeForInMemTuples,
                    frameSize);
            if (pidToSpill >= 0) {
                // There is a suitable one. We spill that partition to the disk.
                inMemTupCount -= buildPSizeInTups[pidToSpill];
                spillPartition(pidToSpill);
                moreSpilled = true;
            } else {
                // There is no single suitable partition. So, we need to spill multiple partitions to the disk
                // in order to accommodate the hash table.
                for (int p = spilledStatus.nextClearBit(0); p >= 0
                        && p < numOfPartitions; p = spilledStatus.nextClearBit(p + 1)) {
                    int spaceToBeReturned = bufferManager.getPhysicalSize(p);
                    int numberOfTuplesToBeSpilled = buildPSizeInTups[p];
                    if (spaceToBeReturned == 0) {
                        continue;
                    }
                    spillPartition(p);
                    moreSpilled = true;
                    inMemTupCount -= numberOfTuplesToBeSpilled;
                    // Since the number of tuples in memory has been decreased,
                    // the hash table size will be decreased, too.
                    int expectedHashTableSizeDecrease = hashTableByteSizeForInMemTuples
                            - SerializableHashTable.getExpectedTableSizeInByte(inMemTupCount, frameSize);
                    freeSpace = freeSpace + spaceToBeReturned + expectedHashTableSizeDecrease;
                    if (freeSpace > 0) {
                        break;
                    }
                }
            }
        }

        // If more partitions have been spilled to the disk, calculate the expected hash table size again
        // before bringing some partitions to main memory.
        if (moreSpilled) {
            hashTableByteSizeForInMemTuples = SerializableHashTable.getExpectedTableSizeInByte(inMemTupCount,
                    frameSize);
        }

        // Bring back some partitions if we have enough free space.
        int pid = 0;
        while ((pid = selectPartitionsToReload(freeSpace, pid, inMemTupCount, hashTableByteSizeForInMemTuples)) >= 0) {
            if (!loadSpilledPartitionToMem(pid, buildRFWriters[pid])) {
                break;
            }
            inMemTupCount += buildPSizeInTups[pid];
            int expectedHashTableSizeIncrease = SerializableHashTable.getExpectedTableSizeInByte(inMemTupCount,
                    frameSize) - hashTableByteSizeForInMemTuples;
            freeSpace = freeSpace - bufferManager.getPhysicalSize(pid) - expectedHashTableSizeIncrease;
        }

        return inMemTupCount;
    }

    /**
     * Finds a best-fit partition that will be spilled to the disk to make enough space to accommodate the hash table.
     *
     * @param currentFreeSpace
     * @param currentInMemTupCount
     * @param currentHashTableByteSizeForInMemTuples
     * @param partPhysicalByteSizesInMem
     * @param partNumTuplesInMem
     * @return the partition id that will be spilled to the disk. Returns -1 if there is no single suitable partition.
     */
    private int selectSinglePartitionToSpill(int currentFreeSpace, int currentInMemTupCount,
            int currentHashTableByteSizeForInMemTuples, int frameSize) {

        int spaceAfterIncrease;
        int minSpaceAfterIncrease = memSizeInFrames * ctx.getInitialFrameSize();
        int minSpaceAfterIncreasePartID = -1;

        for (int p = spilledStatus.nextClearBit(0); p >= 0
                && p < numOfPartitions; p = spilledStatus.nextClearBit(p + 1)) {
            spaceAfterIncrease = currentFreeSpace + bufferManager.getPhysicalSize(p)
                    + (currentHashTableByteSizeForInMemTuples - SerializableHashTable
                            .getExpectedTableSizeInByte(currentInMemTupCount - buildPSizeInTups[p], frameSize));
            if (spaceAfterIncrease == 0) {
                // Found the perfect one. Just return this partition.
                return p;
            } else if (spaceAfterIncrease > 0 && spaceAfterIncrease < minSpaceAfterIncrease) {
                // We want to find the best-fit partition to avoid many partition spills.
                minSpaceAfterIncrease = spaceAfterIncrease;
                minSpaceAfterIncreasePartID = p;
            }
        }

        return minSpaceAfterIncreasePartID;
    }

    private int selectPartitionsToReload(int freeSpace, int pid, int inMemTupCount, int originalHashTableSize) {
        for (int i = spilledStatus.nextSetBit(pid); i >= 0
                && i < numOfPartitions; i = spilledStatus.nextSetBit(i + 1)) {
            assert buildRFWriters[i].getFileSize() > 0 : "How come a spilled partition has size 0?";
            int spilledTupleCount = buildPSizeInTups[i];
            // Expected hash table size increase after reloading this partition
            int expectedHashTableSizeIncrease = SerializableHashTable.getExpectedTableSizeInByte(
                    inMemTupCount + spilledTupleCount, ctx.getInitialFrameSize()) - originalHashTableSize;
            if (freeSpace >= buildRFWriters[i].getFileSize() + expectedHashTableSizeIncrease) {
                return i;
            }
        }
        return -1;
    }

    private boolean loadSpilledPartitionToMem(int pid, RunFileWriter wr) throws HyracksDataException {
        RunFileReader r = wr.createReader();
        r.open();
        if (reloadBuffer == null) {
            reloadBuffer = new VSizeFrame(ctx);
        }
        while (r.nextFrame(reloadBuffer)) {
            accessorBuild.reset(reloadBuffer.getBuffer());
            for (int tid = 0; tid < accessorBuild.getTupleCount(); tid++) {
                if (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
                    // for some reason (e.g. due to fragmentation) if the inserting failed,
                    // we need to clear the occupied frames
                    bufferManager.clearPartition(pid);
                    r.close();
                    return false;
                }
            }
        }

        // delete the runfile if it is already loaded into memory.
        FileUtils.deleteQuietly(wr.getFileReference().getFile());
        r.close();
        spilledStatus.set(pid, false);
        buildRFWriters[pid] = null;
        return true;
    }

    private void createInMemoryJoiner(int inMemTupCount) throws HyracksDataException {
        ISerializableTable table = new SerializableHashTable(inMemTupCount, ctx, bufferManagerForHashTable);
        this.inMemJoiner = new InMemoryHashJoin(ctx, inMemTupCount, new FrameTupleAccessor(probeRd), probeHpc,
                new FrameTupleAccessor(buildRd), buildRd, buildHpc,
                new FrameTuplePairComparator(probeKeys, buildKeys, comparators), isLeftOuter, nonMatchWriters, table,
                predEvaluator, isReversed, bufferManagerForHashTable);
    }

    private void loadDataInMemJoin() throws HyracksDataException {

        for (int pid = 0; pid < numOfPartitions; pid++) {
            if (!spilledStatus.get(pid)) {
                bufferManager.flushPartition(pid, new IFrameWriter() {
                    @Override
                    public void open() throws HyracksDataException {

                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        inMemJoiner.build(buffer);
                    }

                    @Override
                    public void fail() throws HyracksDataException {

                    }

                    @Override
                    public void close() throws HyracksDataException {

                    }
                });
            }
        }
    }

    public void initProbe() throws HyracksDataException {

        probePSizeInTups = new int[numOfPartitions];
        probeRFWriters = new RunFileWriter[numOfPartitions];

    }

    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();

        if (isBuildRelAllInMemory()) {
            inMemJoiner.join(buffer, writer);
            return;
        }
        inMemJoiner.resetAccessorProbe(accessorProbe);
        for (int i = 0; i < tupleCount; ++i) {
            int pid = probeHpc.partition(accessorProbe, i, numOfPartitions);

            if (buildPSizeInTups[pid] > 0 || isLeftOuter) { //Tuple has potential match from previous phase
                if (spilledStatus.get(pid)) { //pid is Spilled
                    while (!bufferManager.insertTuple(pid, accessorProbe, i, tempPtr)) {
                        int victim = pid;
                        if (bufferManager.getNumTuples(pid) == 0) { // current pid is empty, choose the biggest one
                            victim = spillPolicy.findSpilledPartitionWithMaxMemoryUsage();
                        }
                        if (victim < 0) { // current tuple is too big for all the free space
                            flushBigProbeObjectToDisk(pid, accessorProbe, i);
                            break;
                        }
                        RunFileWriter runFileWriter = getSpillWriterOrCreateNewOneIfNotExist(victim, SIDE.PROBE);
                        bufferManager.flushPartition(victim, runFileWriter);
                        bufferManager.clearPartition(victim);
                    }
                } else { //pid is Resident
                    inMemJoiner.join(i, writer);
                }
                probePSizeInTups[pid]++;
            }
        }

    }

    private void flushBigProbeObjectToDisk(int pid, FrameTupleAccessor accessorProbe, int i)
            throws HyracksDataException {
        if (bigProbeFrameAppender == null) {
            bigProbeFrameAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        }
        RunFileWriter runFileWriter = getSpillWriterOrCreateNewOneIfNotExist(pid, SIDE.PROBE);
        if (!bigProbeFrameAppender.append(accessorProbe, i)) {
            throw new HyracksDataException("The given tuple is too big");
        }
        bigProbeFrameAppender.write(runFileWriter, true);
    }

    private boolean isBuildRelAllInMemory() {
        return spilledStatus.nextSetBit(0) < 0;
    }

    public void closeProbe(IFrameWriter writer) throws HyracksDataException {
        //We do NOT join the spilled partitions here, that decision is made at the descriptor level
        //(which join technique to use)
        inMemJoiner.closeJoin(writer);
        inMemJoiner.closeTable();
        closeAllSpilledPartitions(SIDE.PROBE);
        bufferManager.close();
        inMemJoiner = null;
        bufferManager = null;
        bufferManagerForHashTable = null;
    }

    /**
     * In case of failure happens, we need to clear up the generated temporary files.
     */
    public void clearProbeTempFiles() {
        for (int i = 0; i < probeRFWriters.length; i++) {
            if (probeRFWriters[i] != null) {
                probeRFWriters[i].getFileReference().delete();
            }
        }
    }

    public RunFileReader getBuildRFReader(int pid) throws HyracksDataException {
        return ((buildRFWriters[pid] == null) ? null : (buildRFWriters[pid]).createDeleteOnCloseReader());
    }

    public int getBuildPartitionSizeInTup(int pid) {
        return (buildPSizeInTups[pid]);
    }

    public RunFileReader getProbeRFReader(int pid) throws HyracksDataException {
        return ((probeRFWriters[pid] == null) ? null : (probeRFWriters[pid]).createDeleteOnCloseReader());
    }

    public int getProbePartitionSizeInTup(int pid) {
        return (probePSizeInTups[pid]);
    }

    public int getMaxBuildPartitionSize() {
        int max = buildPSizeInTups[0];
        for (int i = 1; i < buildPSizeInTups.length; i++) {
            if (buildPSizeInTups[i] > max) {
                max = buildPSizeInTups[i];
            }
        }
        return max;
    }

    public int getMaxProbePartitionSize() {
        int max = probePSizeInTups[0];
        for (int i = 1; i < probePSizeInTups.length; i++) {
            if (probePSizeInTups[i] > max) {
                max = probePSizeInTups[i];
            }
        }
        return max;
    }

    public BitSet getPartitionStatus() {
        return spilledStatus;
    }

    public void setIsReversed(boolean b) {
        this.isReversed = b;
    }
}
