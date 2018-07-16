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
package org.apache.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.logging.log4j.Level;

public class FrameSorterMergeSort extends AbstractFrameSorter {

    private int[] tPointersTemp;

    // Temp : analysis - called from in-memory sort
    public FrameSorterMergeSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int maxSortFrames,
            int[] sortFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor)
            throws HyracksDataException {
        this(ctx, bufferManager, maxSortFrames, sortFields, keyNormalizerFactories, comparatorFactories,
                recordDescriptor, Integer.MAX_VALUE);
    }

    // Temp : analysis - called from HybridTopKSortRunGenerator
    public FrameSorterMergeSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int maxSortFrames,
            int[] sortFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, int outputLimit)
            throws HyracksDataException {
        super(ctx, bufferManager, maxSortFrames, sortFields, keyNormalizerFactories, comparatorFactories,
                recordDescriptor, outputLimit);
    }

    // Temp :
    public FrameSorterMergeSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int maxSortFrames,
            int[] sortFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, int outputLimit,
            boolean limitMemory) throws HyracksDataException {
        super(ctx, bufferManager, maxSortFrames, sortFields, keyNormalizerFactories, comparatorFactories,
                recordDescriptor, outputLimit, limitMemory);
    }
    //

    @Override
    void sortTupleReferences() throws HyracksDataException {
        if (tPointersTemp == null || tPointersTemp.length < tPointers.length) {
            tPointersTemp = new int[tPointers.length];
        }
        sort(0, tupleCount);
    }

    @Override
    protected long getRequiredMemory(FrameTupleAccessor frameAccessor) {
        // Temp :
        return limitMemory
                ? super.getRequiredMemory(frameAccessor) + ptrSize * frameAccessor.getTupleCount() * Integer.BYTES
                : (long) (frameAccessor.getBuffer().capacity());
        //        return super.getRequiredMemory(frameAccessor) + ptrSize * frameAccessor.getTupleCount() * Integer.BYTES;
    }

    // Temp : for the log purposes - copies the super.reset()
    @Override
    public void reset() throws HyracksDataException {
        // Temp : copies 2 since this is the merge sort.
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "reset" + "\tlimitMemory:\t" + limitMemory
                + "\ttotal_tuple_count:\t" + tupleCount + "\tpointer_array_size(MB):\t"
                + decFormat.format(((double) (2 * ptrSize * tupleCount * Integer.BYTES) / 1048576)) + "\tframe_count:\t"
                + bufferManager.getNumFrames() + "\tsize(MB):\t"
                + decFormat.format(((double) getCurrentSize(bufferManager, info) / 1048576)) + "\ttotal_size(MB)\t"
                + decFormat.format(
                        (2 * ptrSize * tupleCount * Integer.BYTES + (double) getCurrentSize(bufferManager, info))
                                / 1048576));
        //
        tupleCount = 0;
        totalMemoryUsed = 0;
        bufferManager.reset();
    }

    // Temp : for the log purposes - copies the super.printCurrentStatus()
    @Override
    public void printCurrentStatus() {
        // Temp :
        LOGGER.log(Level.INFO,
                this.hashCode() + "\t" + "printCurrentStatus" + "\tlimitMemory:\t" + limitMemory
                        + "\ttotal_tuple_count:\t" + tupleCount + "\tpointer_array_size(MB):\t"
                        + decFormat.format(((double) (2 * ptrSize * tupleCount * Integer.BYTES) / 1048576))
                        + "\tframe_count:\t" + bufferManager.getNumFrames() + "\tsize(MB):\t"
                        + decFormat.format(((double) getCurrentSize(bufferManager, info) / 1048576))
                        + "\ttotal_size(MB)\t" + decFormat.format(((double) (2 * ptrSize * tupleCount * Integer.BYTES)
                                + (double) getCurrentSize(bufferManager, info)) / 1048576));
        //
    }

    @Override
    public void close() {
        // Temp :
        LOGGER.log(Level.INFO,
                this.hashCode() + "\t" + "close" + "\tlimitMemory:\t" + limitMemory + "\ttotal_tuple_count:\t"
                        + tupleCount + "\tpointer_array_size(MB):\t"
                        + decFormat.format(((double) (2 * ptrSize * tupleCount * Integer.BYTES) / 1048576))
                        + "\tframe_count:\t" + bufferManager.getNumFrames() + "\tsize(MB):\t"
                        + decFormat.format(((double) getCurrentSize(bufferManager, info) / 1048576))
                        + "\ttotal_size(MB)\t" + decFormat.format(((double) (2 * ptrSize * tupleCount * Integer.BYTES)
                                + (double) getCurrentSize(bufferManager, info)) / 1048576));
        //
        super.close();
        tPointersTemp = null;
    }

    // Temp : for the log purposes - copies the super.flush()
    @Override
    public int flush(IFrameWriter writer) throws HyracksDataException {
        outputAppender.reset(outputFrame, true);
        int maxFrameSize = outputFrame.getFrameSize();
        int limit = Math.min(tupleCount, outputLimit);
        int io = 0;
        // Temp :
        int totalFlushedBytes = 0;
        //
        for (int ptr = 0; ptr < limit; ++ptr) {
            int i = tPointers[ptr * ptrSize + ID_FRAME_ID];
            int tStart = tPointers[ptr * ptrSize + ID_TUPLE_START];
            int tEnd = tPointers[ptr * ptrSize + ID_TUPLE_END];
            bufferManager.getFrame(i, info);
            inputTupleAccessor.reset(info.getBuffer(), info.getStartOffset(), info.getLength());
            int flushed = FrameUtils.appendToWriter(writer, outputAppender, inputTupleAccessor, tStart, tEnd);
            if (flushed > 0) {
                maxFrameSize = Math.max(maxFrameSize, flushed);
                io++;
                // Temp :
                totalFlushedBytes += flushed;
                //
            }
        }
        maxFrameSize = Math.max(maxFrameSize, outputFrame.getFrameSize());
        outputAppender.write(writer, true);
        // Temp :
        LOGGER.log(Level.INFO,
                this.hashCode() + "\t" + "flush" + "\tlimitMemory:\t" + limitMemory + "\twriter:\t" + writer.toString()
                        + "\t#flushed_records:\t" + limit + "\t/\t" + tupleCount + "\tpointer_array_size(MB):\t"
                        + decFormat.format(((double) (2 * ptrSize * tupleCount * Integer.BYTES) / 1048576))
                        + "\t#flushed_frames:\t" + (io + 1) + "\tflushed_record_byte_sizes:\t" + totalFlushedBytes
                        + "\tflushed_record_size(MB):\t" + decFormat.format((double) totalFlushedBytes / 1048576)
                        + "\tlimitMemory:\t" + limitMemory + "\tmaxFrameSize:\t"
                        + decFormat.format((double) maxFrameSize / 1048576));
        //

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Flushed records:" + limit + " out of " + tupleCount + "; Flushed through " + (io + 1) + " frames");
        }
        return maxFrameSize;
    }

    // Temp : for the log purposes - copies the super.insertFrame()
    @Override
    public boolean insertFrame(ByteBuffer inputBuffer) throws HyracksDataException {
        inputTupleAccessor.reset(inputBuffer);
        long requiredMemory = getRequiredMemory(inputTupleAccessor);
        if (totalMemoryUsed + requiredMemory <= maxSortMemory && bufferManager.insertFrame(inputBuffer) >= 0) {
            // we have enough memory
            totalMemoryUsed += requiredMemory;
            tupleCount += inputTupleAccessor.getTupleCount();
            return true;
        }
        if (getFrameCount() == 0) {
            throw new HyracksDataException(
                    "The input frame is too big for the sorting buffer, please allocate bigger buffer size");
        }
        // Temp :
        LOGGER.log(Level.INFO,
                this.hashCode() + "\t" + "insertFrame" + "\tFAIL\t" + "\tlimitMemory:\t" + limitMemory
                        + "\tmax_sort_memory(MB):\t" + decFormat.format((double) maxSortMemory / 1048576)
                        + "\ttotal_memory_used(MB):\t" + decFormat.format((double) totalMemoryUsed / 1048576)
                        + "\trequiredMemory(MB)\t:" + decFormat.format((double) requiredMemory / 1048576)
                        + "\tnum_frames:\t" + getFrameCount() + "\tnum_tuple_so_far:\t" + tupleCount
                        + "\tincoming_num_tuple:\t" + inputTupleAccessor.getTupleCount());
        printCurrentStatus();
        //
        return false;
    }

    void sort(int offset, int length) throws HyracksDataException {
        int step = 1;
        int end = offset + length;
        /** bottom-up merge */
        while (step < length) {
            /** merge */
            for (int i = offset; i < end; i += 2 * step) {
                int next = i + step;
                if (next < end) {
                    merge(i, next, step, Math.min(step, end - next));
                } else {
                    copy(tPointers, i, tPointersTemp, i, end - i);
                }
            }
            /** prepare next phase merge */
            step *= 2;
            int[] tmp = tPointersTemp;
            tPointersTemp = tPointers;
            tPointers = tmp;
        }
    }

    /**
     * Merge two subarrays into one
     */
    private void merge(int start1, int start2, int len1, int len2) throws HyracksDataException {
        int targetPos = start1;
        int pos1 = start1;
        int pos2 = start2;
        int end1 = start1 + len1 - 1;
        int end2 = start2 + len2 - 1;
        while (pos1 <= end1 && pos2 <= end2) {
            int cmp = compare(pos1, pos2);
            if (cmp <= 0) {
                copy(tPointers, pos1, tPointersTemp, targetPos);
                pos1++;
            } else {
                copy(tPointers, pos2, tPointersTemp, targetPos);
                pos2++;
            }
            targetPos++;
        }
        if (pos1 <= end1) {
            int rest = end1 - pos1 + 1;
            copy(tPointers, pos1, tPointersTemp, targetPos, rest);
        }
        if (pos2 <= end2) {
            int rest = end2 - pos2 + 1;
            copy(tPointers, pos2, tPointersTemp, targetPos, rest);
        }
    }
}
