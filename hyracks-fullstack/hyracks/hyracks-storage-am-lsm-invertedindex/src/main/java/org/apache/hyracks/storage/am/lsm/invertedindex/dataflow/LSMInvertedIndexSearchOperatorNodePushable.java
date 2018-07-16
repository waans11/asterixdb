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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class LSMInvertedIndexSearchOperatorNodePushable extends IndexSearchOperatorNodePushable {

    protected final IInvertedIndexSearchModifier searchModifier;
    protected final IBinaryTokenizerFactory binaryTokenizerFactory;
    protected final int queryFieldIndex;
    protected final int numOfFields;
    // Keeps the information whether the given query is a full-text search or not.
    // We need to have this information to stop the search process since we don't allow a phrase search yet.
    protected final boolean isFullTextSearchQuery;
    // Budget-constrained buffer manager for conducting the search operation
    protected final ISimpleFrameBufferManager bufferManagerForSearch;
    protected final IDeallocatableFramePool framePool;

    public LSMInvertedIndexSearchOperatorNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc,
            int partition, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            IInvertedIndexSearchModifier searchModifier, IBinaryTokenizerFactory binaryTokenizerFactory,
            int queryFieldIndex, boolean isFullTextSearchQuery, int numOfFields, boolean appendIndexFilter,
            int frameLimit, long searchLimit) throws HyracksDataException {
        super(ctx, inputRecDesc, partition, minFilterFieldIndexes, maxFilterFieldIndexes, indexHelperFactory,
                retainInput, retainMissing, missingWriterFactory, searchCallbackFactory, appendIndexFilter,
                searchLimit);
        this.searchModifier = searchModifier;
        this.binaryTokenizerFactory = binaryTokenizerFactory;
        this.queryFieldIndex = queryFieldIndex;
        this.isFullTextSearchQuery = isFullTextSearchQuery;
        // If retainInput is true, the frameTuple is created in IndexSearchOperatorNodePushable.open().
        if (!retainInput) {
            frameTuple = new FrameTupleReference();
        }
        this.numOfFields = numOfFields;
        // Intermediate and final search result will use this buffer manager to get frames.
        framePool = new DeallocatableFramePool(ctx, frameLimit * ctx.getInitialFrameSize());
        bufferManagerForSearch = new FramePoolBackedFrameBufferManager(framePool);
        // Keep the buffer manager in the hyracks context so that the search process can get it via the context.
        TaskUtil.put(HyracksConstants.INVERTED_INDEX_SEARCH_FRAME_MANAGER, bufferManagerForSearch, ctx);
        // true: limit text search using the parameter.  fasle: no limit
        TaskUtil.put(HyracksConstants.LIMIT_TEXTSEARCHMEMORY, new Boolean(true), ctx);
    }

    // Temp :
    public LSMInvertedIndexSearchOperatorNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc,
            int partition, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            IInvertedIndexSearchModifier searchModifier, IBinaryTokenizerFactory binaryTokenizerFactory,
            int queryFieldIndex, boolean isFullTextSearchQuery, int numOfFields, boolean appendIndexFilter,
            int frameLimit, boolean limitTextSearchMemory, long searchLimit) throws HyracksDataException {
        super(ctx, inputRecDesc, partition, minFilterFieldIndexes, maxFilterFieldIndexes, indexHelperFactory,
                retainInput, retainMissing, missingWriterFactory, searchCallbackFactory, appendIndexFilter,
                searchLimit);
        this.searchModifier = searchModifier;
        this.binaryTokenizerFactory = binaryTokenizerFactory;
        this.queryFieldIndex = queryFieldIndex;
        this.isFullTextSearchQuery = isFullTextSearchQuery;
        // If retainInput is true, the frameTuple is created in IndexSearchOperatorNodePushable.open().
        if (!retainInput) {
            frameTuple = new FrameTupleReference();
        }
        this.numOfFields = numOfFields;
        // Intermediate and final search result will use this buffer manager to get frames.
        // Temp : allocates 50 GB as the buffer frame pool so that an OOM can happen for the experiment purpose.
        int frameLimitUnlimited = 1048576 / ctx.getInitialFrameSize() * 1024 * 50;
        framePool = limitTextSearchMemory ? new DeallocatableFramePool(ctx, frameLimit * ctx.getInitialFrameSize())
                : new DeallocatableFramePool(ctx, frameLimitUnlimited * ctx.getInitialFrameSize());
        //
        bufferManagerForSearch = new FramePoolBackedFrameBufferManager(framePool);
        // Keep the buffer manager in the hyracks context so that the search process can get it via the context.
        TaskUtil.put(HyracksConstants.INVERTED_INDEX_SEARCH_FRAME_MANAGER, bufferManagerForSearch, ctx);
        // true: limit text search using the parameter.  fasle: no limit
        TaskUtil.put(HyracksConstants.LIMIT_TEXTSEARCHMEMORY, new Boolean(limitTextSearchMemory), ctx);
    }
    //

    @Override
    protected ISearchPredicate createSearchPredicate() {
        return new InvertedIndexSearchPredicate(binaryTokenizerFactory.createTokenizer(), searchModifier, minFilterKey,
                maxFilterKey, isFullTextSearchQuery);
    }

    @Override
    protected void resetSearchPredicate(int tupleIndex) {
        frameTuple.reset(accessor, tupleIndex);
        InvertedIndexSearchPredicate invIndexSearchPred = (InvertedIndexSearchPredicate) searchPred;
        invIndexSearchPred.setQueryTuple(frameTuple);
        invIndexSearchPred.setQueryFieldIndex(queryFieldIndex);
        invIndexSearchPred.setIsFullTextSearchQuery(isFullTextSearchQuery);
        if (minFilterKey != null) {
            minFilterKey.reset(accessor, tupleIndex);
        }
        if (maxFilterKey != null) {
            maxFilterKey.reset(accessor, tupleIndex);
        }
    }

    @Override
    protected int getFieldCount() {
        return numOfFields;
    }

    @Override
    protected void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) throws HyracksDataException {
        iap.getParameters().put(HyracksConstants.HYRACKS_TASK_CONTEXT, ctx);
    }
}
