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
package org.apache.asterix.runtime.job.listener;

import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.JobId;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.job.IJobletEventListener;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.util.OperatorExecutionTimeProfiler;

public class JobEventListenerFactory implements IJobletEventListenerFactory {

    private static final long serialVersionUID = 1L;
    private final JobId jobId;
    private final boolean transactionalWrite;

    public JobEventListenerFactory(JobId jobId, boolean transactionalWrite) {
        this.jobId = jobId;
        this.transactionalWrite = transactionalWrite;
    }

    public JobId getJobId() {
        return jobId;
    }

    @Override
    public IJobletEventListener createListener(final IHyracksJobletContext jobletContext) {

        return new IJobletEventListener() {

            // Added to measure the execution time when the profiler setting is enabled
            private String nodeJobSignature;

            @Override
            public void jobletFinish(JobStatus jobStatus) {
                try {
                    ITransactionManager txnManager = ((IAppRuntimeContext) jobletContext.getApplicationContext()
                            .getApplicationObject()).getTransactionSubsystem().getTransactionManager();
                    ITransactionContext txnContext = txnManager.getTransactionContext(jobId, false);
                    txnContext.setWriteTxn(transactionalWrite);
                    txnManager.completedTransaction(txnContext, new DatasetId(-1), -1,
                            !(jobStatus == JobStatus.FAILURE));

                    int numPartitions =
                            ((IAppRuntimeContext) jobletContext.getApplicationContext().getApplicationObject())
                                    .getMetadataProperties().getNodePartitions().size();

                    // Temp:
                    System.out.println(nodeJobSignature + " SimilarityJaccardCheckEvaluator" + "\tevaluate_time(ms)\t"
                            + OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.evaluateDurationTime.get()
                            + " evaluate_time_avg "
                            + (OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.evaluateDurationTime.get()
                                    / numPartitions)
                            + "\tcomputeResult_time(ms)\t"
                            + OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.computeResultDurationTime
                                    .get()
                            + " computeResult_time_avg "
                            + (OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.computeResultDurationTime
                                    .get() / numPartitions)
                            + "\tprobeHashMap_time(ms)\t"
                            + OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.probeHashMapDurationTime
                                    .get()
                            + " probeHashMap_time_avg "
                            + (OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.probeHashMapDurationTime
                                    .get() / numPartitions)
                            + "\twriteResult_time(ms)\t"
                            + OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.writeResultDurationTime.get()
                            + " writeResult_time_avg "
                            + (OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.writeResultDurationTime
                                    .get() / numPartitions)
                            + "\tlengthFilter_time(ms)\t"
                            + OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.lengthFilterDurationTime
                                    .get()
                            + " lengthFilter_time_avg "
                            + (OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.lengthFilterDurationTime
                                    .get() / numPartitions)
                            + "\tlengthFilter_count\t"
                            + OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.lengthFilterAppliedTupleCount
                                    .get()
                            + " lengthFilter_count_avg "
                            + (OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.lengthFilterAppliedTupleCount
                                    .get() / numPartitions)
                            + "\tprocessedTupleCount\t"
                            + OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.processTupleCount.get()
                            + " processedTupleCount_avg "
                            + (OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.processTupleCount.get()
                                    / numPartitions)
                            + "\tfiltered_ratio\t"
                            + ((double) OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.lengthFilterAppliedTupleCount
                                    .get()
                                    / OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.processTupleCount
                                            .get())
                            + " numPart:" + numPartitions);
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

            @Override
            public void jobletStart() {
                try {
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.evaluateDurationTime.set(0);
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.computeResultDurationTime.set(0);
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.probeHashMapDurationTime.set(0);
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.writeResultDurationTime.set(0);
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.lengthFilterDurationTime.set(0);
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.lengthFilterAppliedTupleCount.set(0);
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.processTupleCount.set(0);

                    ((IAppRuntimeContext) jobletContext.getApplicationContext().getApplicationObject())
                            .getTransactionSubsystem().getTransactionManager().getTransactionContext(jobId, true);

                    nodeJobSignature = jobletContext.getApplicationContext().getNodeId() + "_"
                            + jobletContext.getJobId() + "_" + +jobletContext.hashCode();

                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

        };
    }
}
