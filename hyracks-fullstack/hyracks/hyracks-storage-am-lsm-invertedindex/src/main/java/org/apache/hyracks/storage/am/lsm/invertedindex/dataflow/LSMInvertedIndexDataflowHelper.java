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
package org.apache.hyracks.storage.am.lsm.invertedindex.dataflow;

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.util.IndexFileNameUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.dataflow.AbstractLSMIndexDataflowHelper;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public final class LSMInvertedIndexDataflowHelper extends AbstractLSMIndexDataflowHelper {

    private final int[] invertedIndexFields;
    private final int[] filterFieldsForNonBulkLoadOps;
    private final int[] invertedIndexFieldsForNonBulkLoadOps;

    public LSMInvertedIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerFactory opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, int[] invertedIndexFields,
            ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps, boolean durable)
            throws HyracksDataException {
        this(opDesc, ctx, partition, virtualBufferCaches, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE, mergePolicy,
                opTrackerFactory, ioScheduler, ioOpCallbackFactory, invertedIndexFields, filterTypeTraits,
                filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps,
                durable);
    }

    public LSMInvertedIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerFactory opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            int[] invertedIndexFields, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] filterFields, int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps,
            boolean durable) throws HyracksDataException {
        super(opDesc, ctx, partition, virtualBufferCaches, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackFactory, filterTypeTraits, filterCmpFactories, filterFields, durable);
        this.invertedIndexFields = invertedIndexFields;
        this.filterFieldsForNonBulkLoadOps = filterFieldsForNonBulkLoadOps;
        this.invertedIndexFieldsForNonBulkLoadOps = invertedIndexFieldsForNonBulkLoadOps;
    }

    @Override
    public IIndex createIndexInstance() throws HyracksDataException {
        IInvertedIndexOperatorDescriptor invIndexOpDesc = (IInvertedIndexOperatorDescriptor) opDesc;
        try {
            IBufferCache diskBufferCache = opDesc.getStorageManager().getBufferCache(ctx);
            IFileMapProvider diskFileMapProvider = opDesc.getStorageManager().getFileMapProvider(ctx);
            FileReference fileRef = IndexFileNameUtil.getIndexAbsoluteFileRef(invIndexOpDesc, ctx.getTaskAttemptId()
                    .getTaskId().getPartition(), ctx.getIOManager());
            LSMInvertedIndex invIndex = InvertedIndexUtils.createLSMInvertedIndex(ctx.getIOManager(),
                    virtualBufferCaches,
                    diskFileMapProvider, invIndexOpDesc.getInvListsTypeTraits(),
                    invIndexOpDesc.getInvListsComparatorFactories(), invIndexOpDesc.getTokenTypeTraits(),
                    invIndexOpDesc.getTokenComparatorFactories(), invIndexOpDesc.getTokenizerFactory(),
                    diskBufferCache, fileRef.getFile().getAbsolutePath(), bloomFilterFalsePositiveRate, mergePolicy,
                    opTrackerFactory.getOperationTracker(ctx.getJobletContext().getApplicationContext()), ioScheduler,
                    ioOpCallbackFactory.createIoOpCallback(), invertedIndexFields, filterTypeTraits,
                    filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps,
                    invertedIndexFieldsForNonBulkLoadOps, durable, (IMetadataPageManagerFactory) opDesc
                            .getPageManagerFactory());
            return invIndex;
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }
}
