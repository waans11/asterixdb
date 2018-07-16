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
import java.text.DecimalFormat;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.utils.NormalizedKeyUtils;
import org.apache.hyracks.dataflow.std.buffermanager.BufferInfo;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.hyracks.util.IntSerDeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractFrameSorter implements IFrameSorter {

    protected Logger LOGGER = LogManager.getLogger();
    protected static final int ID_FRAME_ID = 0;
    protected static final int ID_TUPLE_START = 1;
    protected static final int ID_TUPLE_END = 2;
    protected static final int ID_NORMALIZED_KEY = 3;

    // the length of each normalized key (in terms of integers)
    protected final int[] normalizedKeyLength;
    // the total length of the normalized key (in term of integers)
    protected final int normalizedKeyTotalLength;
    // whether the normalized keys can be used to decide orders, even when normalized keys are the same
    protected final boolean normalizedKeysDecisive;

    protected final int ptrSize;

    protected final int[] sortFields;
    protected final IBinaryComparator[] comparators;
    protected final INormalizedKeyComputer[] nkcs;
    protected final IFrameBufferManager bufferManager;
    protected final FrameTupleAccessor inputTupleAccessor;
    protected final IFrameTupleAppender outputAppender;
    protected final IFrame outputFrame;
    protected final int outputLimit;

    protected final long maxSortMemory;
    protected long totalMemoryUsed;
    protected int[] tPointers;
    protected final int[] tmpPointer;
    protected int tupleCount;

    protected final FrameTupleAccessor fta2;
    protected final BufferInfo info = new BufferInfo(null, -1, -1);

    // Temp :
    protected final boolean limitMemory;
    protected final int defaultFrameSize;
    protected final DecimalFormat decFormat = new DecimalFormat("#.######");
    //

    //    public AbstractFrameSorter(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int maxSortFrames,
    //            int[] sortFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
    //            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor)
    //            throws HyracksDataException {
    //        this(ctx, bufferManager, maxSortFrames, sortFields, keyNormalizerFactories, comparatorFactories,
    //                recordDescriptor, Integer.MAX_VALUE);
    //    }

    public AbstractFrameSorter(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int maxSortFrames,
            int[] sortFields, INormalizedKeyComputerFactory[] normalizedKeyComputerFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, int outputLimit)
            throws HyracksDataException {
        this.bufferManager = bufferManager;
        if (maxSortFrames == VariableFramePool.UNLIMITED_MEMORY) {
            maxSortMemory = Long.MAX_VALUE;
        } else {
            maxSortMemory = (long) ctx.getInitialFrameSize() * maxSortFrames;
        }
        this.sortFields = sortFields;

        int runningNormalizedKeyTotalLength = 0;

        if (normalizedKeyComputerFactories != null) {
            int decisivePrefixLength = NormalizedKeyUtils.getDecisivePrefixLength(normalizedKeyComputerFactories);

            // we only take a prefix of the decisive normalized keys, plus at most indecisive normalized keys
            // ideally, the caller should prepare normalizers in this way, but we just guard here to avoid
            // computing unncessary normalized keys
            int normalizedKeys = decisivePrefixLength < normalizedKeyComputerFactories.length ? decisivePrefixLength + 1
                    : decisivePrefixLength;
            nkcs = new INormalizedKeyComputer[normalizedKeys];
            normalizedKeyLength = new int[normalizedKeys];

            for (int i = 0; i < normalizedKeys; i++) {
                nkcs[i] = normalizedKeyComputerFactories[i].createNormalizedKeyComputer();
                normalizedKeyLength[i] =
                        normalizedKeyComputerFactories[i].getNormalizedKeyProperties().getNormalizedKeyLength();
                runningNormalizedKeyTotalLength += normalizedKeyLength[i];
            }
            normalizedKeysDecisive = decisivePrefixLength == comparatorFactories.length;
        } else {
            nkcs = null;
            normalizedKeyLength = null;
            normalizedKeysDecisive = false;
        }
        normalizedKeyTotalLength = runningNormalizedKeyTotalLength;
        ptrSize = ID_NORMALIZED_KEY + normalizedKeyTotalLength;
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        inputTupleAccessor = new FrameTupleAccessor(recordDescriptor);
        outputAppender = new FrameTupleAppender();
        outputFrame = new VSizeFrame(ctx);
        this.outputLimit = outputLimit;
        fta2 = new FrameTupleAccessor(recordDescriptor);
        tmpPointer = new int[ptrSize];
        // Temp :
        limitMemory = true;
        defaultFrameSize = ctx.getInitialFrameSize();
        //
    }

    // Temp :
    public AbstractFrameSorter(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int maxSortFrames,
            int[] sortFields, INormalizedKeyComputerFactory[] normalizedKeyComputerFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, int outputLimit,
            boolean limitMemory) throws HyracksDataException {
        this.bufferManager = bufferManager;
        if (maxSortFrames == VariableFramePool.UNLIMITED_MEMORY) {
            maxSortMemory = Long.MAX_VALUE;
        } else {
            maxSortMemory = (long) ctx.getInitialFrameSize() * maxSortFrames;
        }
        this.sortFields = sortFields;

        int runningNormalizedKeyTotalLength = 0;

        if (normalizedKeyComputerFactories != null) {
            int decisivePrefixLength = NormalizedKeyUtils.getDecisivePrefixLength(normalizedKeyComputerFactories);

            // we only take a prefix of the decisive normalized keys, plus at most indecisive normalized keys
            // ideally, the caller should prepare normalizers in this way, but we just guard here to avoid
            // computing unncessary normalized keys
            int normalizedKeys = decisivePrefixLength < normalizedKeyComputerFactories.length ? decisivePrefixLength + 1
                    : decisivePrefixLength;
            nkcs = new INormalizedKeyComputer[normalizedKeys];
            normalizedKeyLength = new int[normalizedKeys];

            for (int i = 0; i < normalizedKeys; i++) {
                nkcs[i] = normalizedKeyComputerFactories[i].createNormalizedKeyComputer();
                normalizedKeyLength[i] =
                        normalizedKeyComputerFactories[i].getNormalizedKeyProperties().getNormalizedKeyLength();
                runningNormalizedKeyTotalLength += normalizedKeyLength[i];
            }
            normalizedKeysDecisive = decisivePrefixLength == comparatorFactories.length;
        } else {
            nkcs = null;
            normalizedKeyLength = null;
            normalizedKeysDecisive = false;
        }
        normalizedKeyTotalLength = runningNormalizedKeyTotalLength;
        ptrSize = ID_NORMALIZED_KEY + normalizedKeyTotalLength;
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        inputTupleAccessor = new FrameTupleAccessor(recordDescriptor);
        outputAppender = new FrameTupleAppender();
        outputFrame = new VSizeFrame(ctx);
        this.outputLimit = outputLimit;
        fta2 = new FrameTupleAccessor(recordDescriptor);
        tmpPointer = new int[ptrSize];

        // Temp :
        this.limitMemory = limitMemory;
        defaultFrameSize = ctx.getInitialFrameSize();
        //
    }
    //

    @Override
    public void reset() throws HyracksDataException {
        // Temp :
        //        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "reset" + "\ttotal_tuple_count:\t" + tupleCount
        //                + "\tpointer_array_size(MB):\t"
        //                + decFormat.format(((double) (ptrSize * tupleCount * Integer.BYTES) / 1048576)) + "\tframe_count:\t"
        //                + bufferManager.getNumFrames() + "\tsize(MB):\t"
        //                + decFormat.format(((double) getCurrentSize(bufferManager, info) / 1048576)) + "\ttotal_size(MB)\t"
        //                + decFormat.format(
        //                        ((double) (ptrSize * tupleCount * Integer.BYTES) + (double) getCurrentSize(bufferManager, info))
        //                                / 1048576));
        //

        tupleCount = 0;
        totalMemoryUsed = 0;
        bufferManager.reset();
    }

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
        //        LOGGER.log(Level.INFO,
        //                this.hashCode() + "\t" + "insertFrame" + "\tFAIL\tmax_sort_memory(MB):\t"
        //                        + decFormat.format((double) maxSortMemory / 1048576) + "\ttotal_memory_used(MB):\t"
        //                        + decFormat.format((double) totalMemoryUsed / 1048576) + "\trequiredMemory(MB)\t:"
        //                        + decFormat.format((double) requiredMemory / 1048576) + "\tnum_frames:\t" + getFrameCount()
        //                        + "\tnum_tuple_so_far:\t" + tupleCount + "\tincoming_num_tuple:\t"
        //                        + inputTupleAccessor.getTupleCount());
        //        printCurrentStatus();
        //
        return false;
    }

    protected long getRequiredMemory(FrameTupleAccessor frameAccessor) {
        // Temp : the pointer size is not considered if limitMemory is set to false.
        return limitMemory
                ? (long) (frameAccessor.getBuffer().capacity()
                        + ptrSize * frameAccessor.getTupleCount() * Integer.BYTES)
                : (long) (frameAccessor.getBuffer().capacity());
        //
    }

    @Override
    public void sort() throws HyracksDataException {
        if (tPointers == null || tPointers.length < tupleCount * ptrSize) {
            tPointers = new int[tupleCount * ptrSize];
        }
        int ptr = 0;
        for (int i = 0; i < bufferManager.getNumFrames(); ++i) {
            bufferManager.getFrame(i, info);
            inputTupleAccessor.reset(info.getBuffer(), info.getStartOffset(), info.getLength());
            int tCount = inputTupleAccessor.getTupleCount();
            byte[] array = inputTupleAccessor.getBuffer().array();
            int fieldSlotsLength = inputTupleAccessor.getFieldSlotsLength();
            for (int j = 0; j < tCount; ++j, ++ptr) {
                int tStart = inputTupleAccessor.getTupleStartOffset(j);
                int tEnd = inputTupleAccessor.getTupleEndOffset(j);
                tPointers[ptr * ptrSize + ID_FRAME_ID] = i;
                tPointers[ptr * ptrSize + ID_TUPLE_START] = tStart;
                tPointers[ptr * ptrSize + ID_TUPLE_END] = tEnd;
                if (nkcs == null) {
                    continue;
                }
                int keyPos = ptr * ptrSize + ID_NORMALIZED_KEY;
                for (int k = 0; k < nkcs.length; k++) {
                    int sortField = sortFields[k];
                    int fieldStartOffsetRel = inputTupleAccessor.getFieldStartOffset(j, sortField);
                    int fieldEndOffsetRel = inputTupleAccessor.getFieldEndOffset(j, sortField);
                    int fieldStartOffset = fieldStartOffsetRel + tStart + fieldSlotsLength;
                    nkcs[k].normalize(array, fieldStartOffset, fieldEndOffsetRel - fieldStartOffsetRel, tPointers,
                            keyPos);
                    keyPos += normalizedKeyLength[k];
                }
            }
        }
        if (tupleCount > 0) {
            sortTupleReferences();
        }
    }

    abstract void sortTupleReferences() throws HyracksDataException;

    @Override
    public int getFrameCount() {
        return bufferManager.getNumFrames();
    }

    @Override
    public boolean hasRemaining() {
        return getFrameCount() > 0;
    }

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
        //        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "flush" + "\twriter:\t" + writer.toString()
        //                + "\t#flushed_records:\t" + limit + "\t/\t" + tupleCount + "\tpointer_array_size(MB):\t"
        //                + decFormat.format(((double) (ptrSize * tupleCount * Integer.BYTES) / 1048576)) + "\t#flushed_frames:\t"
        //                + (io + 1) + "\tflushed_record_byte_sizes:\t" + totalFlushedBytes + "\tflushed_record_size(MB):\t"
        //                + decFormat.format((double) totalFlushedBytes / 1048576) + "\tlimitMemory:\t" + limitMemory
        //                + "\tmaxFrameSize:\t" + decFormat.format((double) maxFrameSize / 1048576));
        //

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Flushed records:" + limit + " out of " + tupleCount + "; Flushed through " + (io + 1) + " frames");
        }
        return maxFrameSize;
    }

    protected final int compare(int tp1, int tp2) throws HyracksDataException {
        return compare(tPointers, tp1, tPointers, tp2);
    }

    protected final int compare(int[] tPointers1, int tp1, int[] tPointers2, int tp2) throws HyracksDataException {
        if (nkcs != null) {
            int cmpNormalizedKey =
                    NormalizedKeyUtils.compareNormalizeKeys(tPointers1, tp1 * ptrSize + ID_NORMALIZED_KEY, tPointers2,
                            tp2 * ptrSize + ID_NORMALIZED_KEY, normalizedKeyTotalLength);
            if (cmpNormalizedKey != 0 || normalizedKeysDecisive) {
                return cmpNormalizedKey;
            }
        }

        int i1 = tPointers1[tp1 * ptrSize + ID_FRAME_ID];
        int j1 = tPointers1[tp1 * ptrSize + ID_TUPLE_START];
        int i2 = tPointers2[tp2 * ptrSize + ID_FRAME_ID];
        int j2 = tPointers2[tp2 * ptrSize + ID_TUPLE_START];

        bufferManager.getFrame(i1, info);
        byte[] b1 = info.getBuffer().array();
        inputTupleAccessor.reset(info.getBuffer(), info.getStartOffset(), info.getLength());

        bufferManager.getFrame(i2, info);
        byte[] b2 = info.getBuffer().array();
        fta2.reset(info.getBuffer(), info.getStartOffset(), info.getLength());
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int f1Start = fIdx == 0 ? 0 : IntSerDeUtils.getInt(b1, j1 + (fIdx - 1) * 4);
            int f1End = IntSerDeUtils.getInt(b1, j1 + fIdx * 4);
            int s1 = j1 + inputTupleAccessor.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : IntSerDeUtils.getInt(b2, j2 + (fIdx - 1) * 4);
            int f2End = IntSerDeUtils.getInt(b2, j2 + fIdx * 4);
            int s2 = j2 + fta2.getFieldSlotsLength() + f2Start;
            int l2 = f2End - f2Start;
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    protected void swap(int pointers1[], int pos1, int pointers2[], int pos2) {
        System.arraycopy(pointers1, pos1 * ptrSize, tmpPointer, 0, ptrSize);
        System.arraycopy(pointers2, pos2 * ptrSize, pointers1, pos1 * ptrSize, ptrSize);
        System.arraycopy(tmpPointer, 0, pointers2, pos2 * ptrSize, ptrSize);
    }

    protected void copy(int src[], int srcPos, int dest[], int destPos) {
        System.arraycopy(src, srcPos * ptrSize, dest, destPos * ptrSize, ptrSize);
    }

    protected void copy(int src[], int srcPos, int dest[], int destPos, int n) {
        System.arraycopy(src, srcPos * ptrSize, dest, destPos * ptrSize, n * ptrSize);
    }

    @Override
    public void close() {
        // Temp :
        //        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "close" + "\ttotal_tuple_count:\t" + tupleCount
        //                + "\tpointer_array_size(MB):\t"
        //                + decFormat.format(((double) (ptrSize * tupleCount * Integer.BYTES) / 1048576)) + "\tframe_count:\t"
        //                + bufferManager.getNumFrames() + "\tsize(MB):\t"
        //                + decFormat.format(((double) getCurrentSize(bufferManager, info) / 1048576)) + "\ttotal_size(MB)\t"
        //                + decFormat.format(
        //                        ((double) (ptrSize * tupleCount * Integer.BYTES) + (double) getCurrentSize(bufferManager, info))
        //                                / 1048576));
        //
        tupleCount = 0;
        bufferManager.close();
        tPointers = null;
    }

    // Calcuates the size of frames in the buffer manager.
    protected static int getCurrentSize(IFrameBufferManager bufferManager, BufferInfo info) {
        int currentSize = 0;
        for (int i = 0; i < bufferManager.getNumFrames(); i++) {
            bufferManager.getFrame(i, info);
            if (info.getBuffer() != null) {
                currentSize += info.getLength();
            }
        }
        return currentSize;
    }

    @Override
    public void printCurrentStatus() {
        // Temp :
        //        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "printCurrentStatus" + "\ttotal_tuple_count:\t" + tupleCount
        //                + "\tpointer_array_size(MB):\t"
        //                + decFormat.format(((double) (ptrSize * tupleCount * Integer.BYTES) / 1048576)) + "\tframe_count:\t"
        //                + bufferManager.getNumFrames() + "\tsize(MB):\t"
        //                + decFormat.format(((double) getCurrentSize(bufferManager, info) / 1048576)) + "\ttotal_size(MB)\t"
        //                + decFormat.format(
        //                        ((double) (ptrSize * tupleCount * Integer.BYTES) + (double) getCurrentSize(bufferManager, info))
        //                                / 1048576));
        //
    }

}
