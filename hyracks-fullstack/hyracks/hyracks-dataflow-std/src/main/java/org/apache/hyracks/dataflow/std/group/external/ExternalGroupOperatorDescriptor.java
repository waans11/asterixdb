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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.ISpillableTableFactory;

/**
 *
 */
public class ExternalGroupOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final int AGGREGATE_ACTIVITY_ID = 0;

    private static final int MERGE_ACTIVITY_ID = 1;

    private static final long serialVersionUID = 1L;
    private final int[] keyFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    private final IAggregatorDescriptorFactory partialAggregatorFactory;
    private final IAggregatorDescriptorFactory intermediateAggregateFactory;

    private final int framesLimit;
    private final ISpillableTableFactory spillableTableFactory;
    private final RecordDescriptor partialRecDesc;
    private final RecordDescriptor outRecDesc;
    private final int tableSize;
    private final long fileSize;

    public ExternalGroupOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputSizeInTuple, long inputFileSize,
            int[] keyFields, int framesLimit, IBinaryComparatorFactory[] comparatorFactories,
            INormalizedKeyComputerFactory firstNormalizerFactory, IAggregatorDescriptorFactory partialAggregatorFactory,
            IAggregatorDescriptorFactory intermediateAggregateFactory, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, ISpillableTableFactory spillableTableFactory) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        if (framesLimit <= 3) {
            /**
             * Minimum of 4 frames: 1 for input records, 1 for output, and 2 for hash table (1 header and 1 content)
             * aggregation results.
             */
            throw new IllegalStateException(
                    "Frame limit for the External Group Operator should at least be 4, but it is " + framesLimit + "!");
        }
        this.partialAggregatorFactory = partialAggregatorFactory;
        this.intermediateAggregateFactory = intermediateAggregateFactory;
        this.keyFields = keyFields;
        this.comparatorFactories = comparatorFactories;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.spillableTableFactory = spillableTableFactory;

        this.partialRecDesc = partialAggRecordDesc;
        this.outRecDesc = outRecordDesc;

        /**
         * Set the record descriptor. Note that since this operator is a unary
         * operator, only the first record descriptor is used here.
         */
        recordDescriptors[0] = outRecordDesc;
        this.tableSize = inputSizeInTuple;
        this.fileSize = inputFileSize;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hyracks.api.dataflow.IOperatorDescriptor#contributeActivities
     * (org.apache.hyracks.api.dataflow.IActivityGraphBuilder)
     */
    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        AggregateActivity aggregateAct = new AggregateActivity(new ActivityId(getOperatorId(), AGGREGATE_ACTIVITY_ID));
        MergeActivity mergeAct = new MergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID));

        builder.addActivity(this, aggregateAct);
        builder.addSourceEdge(0, aggregateAct, 0);

        builder.addActivity(this, mergeAct);
        builder.addTargetEdge(0, mergeAct, 0);

        builder.addBlockingEdge(aggregateAct, mergeAct);
    }

    private class AggregateActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public AggregateActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                        throws HyracksDataException {
            return new ExternalGroupBuildOperatorNodePushable(ctx, new TaskId(getActivityId(), partition), tableSize,
                    fileSize, keyFields, framesLimit, comparatorFactories, firstNormalizerFactory,
                    partialAggregatorFactory, recordDescProvider.getInputRecordDescriptor(getActivityId(), 0),
                    recordDescriptors[0], spillableTableFactory);
        }
    }

    private class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MergeActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                        throws HyracksDataException {
            return new ExternalGroupWriteOperatorNodePushable(ctx,
                    new TaskId(new ActivityId(getOperatorId(), AGGREGATE_ACTIVITY_ID), partition),
                    spillableTableFactory, partialRecDesc, outRecDesc, framesLimit, keyFields, firstNormalizerFactory,
                    comparatorFactories, intermediateAggregateFactory);

        }

    }

}
