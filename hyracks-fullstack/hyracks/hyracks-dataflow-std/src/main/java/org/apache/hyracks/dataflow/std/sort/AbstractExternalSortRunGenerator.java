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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractExternalSortRunGenerator extends AbstractSortRunGenerator {

    protected final IHyracksTaskContext ctx;
    protected final IFrameSorter frameSorter;
    protected final int maxSortFrames;
    static final Logger LOGGER = LogManager.getLogger();
    //

    //    public AbstractExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
    //            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
    //            RecordDescriptor recordDesc, Algorithm alg, int framesLimit) throws HyracksDataException {
    //        this(ctx, sortFields, keyNormalizerFactories, comparatorFactories, recordDesc, alg, EnumFreeSlotPolicy.LAST_FIT,
    //                framesLimit);
    //    }

    //    public AbstractExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
    //            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
    //            RecordDescriptor recordDesc, Algorithm alg, int framesLimit, boolean limitMemory)
    //            throws HyracksDataException {
    //        this(ctx, sortFields, keyNormalizerFactories, comparatorFactories, recordDesc, alg, EnumFreeSlotPolicy.LAST_FIT,
    //                framesLimit, limitMemory);
    //    }

    // Temp : used by ExternalSortGroupByRunGenerator
    public AbstractExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit)
            throws HyracksDataException {
        this(ctx, sortFields, keyNormalizerFactories, comparatorFactories, recordDesc, alg, policy, framesLimit,
                Integer.MAX_VALUE);
    }

    // Temp : used by ExternalSortGroupByRunGenerator
    public AbstractExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit, boolean limitMemory)
            throws HyracksDataException {
        this(ctx, sortFields, keyNormalizerFactories, comparatorFactories, recordDesc, alg, policy, framesLimit,
                Integer.MAX_VALUE, limitMemory);
    }

    public AbstractExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit, int outputLimit)
            throws HyracksDataException {
        // Temp : sets limitMemory here.
        super(true);
        //
        this.ctx = ctx;
        maxSortFrames = framesLimit - 1;

        IFrameFreeSlotPolicy freeSlotPolicy = FrameFreeSlotPolicyFactory.createFreeSlotPolicy(policy, maxSortFrames);
        IFrameBufferManager bufferManager = new VariableFrameMemoryManager(
                new VariableFramePool(ctx, maxSortFrames * ctx.getInitialFrameSize()), freeSlotPolicy);
        if (alg == Algorithm.MERGE_SORT) {
            frameSorter = new FrameSorterMergeSort(ctx, bufferManager, maxSortFrames, sortFields,
                    keyNormalizerFactories, comparatorFactories, recordDesc, outputLimit, limitMemory);
        } else {
            frameSorter = new FrameSorterQuickSort(ctx, bufferManager, maxSortFrames, sortFields,
                    keyNormalizerFactories, comparatorFactories, recordDesc, outputLimit, limitMemory);
        }
    }

    public AbstractExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit, int outputLimit,
            boolean limitMemory) throws HyracksDataException {
        // Temp : sets limitMemory here.
        super(limitMemory);
        //
        this.ctx = ctx;
        maxSortFrames = framesLimit - 1;

        IFrameFreeSlotPolicy freeSlotPolicy = FrameFreeSlotPolicyFactory.createFreeSlotPolicy(policy, maxSortFrames);
        IFrameBufferManager bufferManager = new VariableFrameMemoryManager(
                new VariableFramePool(ctx, maxSortFrames * ctx.getInitialFrameSize()), freeSlotPolicy);
        if (alg == Algorithm.MERGE_SORT) {
            frameSorter = new FrameSorterMergeSort(ctx, bufferManager, maxSortFrames, sortFields,
                    keyNormalizerFactories, comparatorFactories, recordDesc, outputLimit, limitMemory);
        } else {
            frameSorter = new FrameSorterQuickSort(ctx, bufferManager, maxSortFrames, sortFields,
                    keyNormalizerFactories, comparatorFactories, recordDesc, outputLimit, limitMemory);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (!frameSorter.insertFrame(buffer)) {
            // Temp :
            LOGGER.log(Level.INFO, this.hashCode() + "\t" + "nextFrame"
                    + "\tframeSorter.insertFrame() failed. Calling flushFramesToRun()");
            //
            flushFramesToRun();
            if (!frameSorter.insertFrame(buffer)) {
                throw new HyracksDataException("The given frame is too big to insert into the sorting memory.");
            }
        }
    }

    @Override
    public ISorter getSorter() {
        return frameSorter;
    }

}