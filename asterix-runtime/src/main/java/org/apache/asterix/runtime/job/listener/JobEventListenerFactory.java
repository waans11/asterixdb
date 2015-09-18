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

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.JobId;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.job.IJobletEventListener;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.util.ExecutionTimeProfiler;
import org.apache.hyracks.api.util.OperatorExecutionTimeProfiler;
import org.apache.hyracks.api.util.ExecutionTimeStopWatch;

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
            private ExecutionTimeStopWatch profilerSW;
            private String nodeJobSignature;
            private String taskId;

            @Override
            public void jobletFinish(JobStatus jobStatus) {
                try {
                    ITransactionManager txnManager = ((IAsterixAppRuntimeContext) jobletContext.getApplicationContext()
                            .getApplicationObject()).getTransactionSubsystem().getTransactionManager();
                    ITransactionContext txnContext = txnManager.getTransactionContext(jobId, false);
                    txnContext.setWriteTxn(transactionalWrite);
                    txnManager.completedTransaction(txnContext, new DatasetId(-1), -1,
                            !(jobStatus == JobStatus.FAILURE));

                    // Added to measure the execution time when the profiler setting is enabled
                    if (ExecutionTimeProfiler.PROFILE_MODE) {
                        profilerSW.suspend();
                        profilerSW.finish();
                        OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(nodeJobSignature, taskId,
                                profilerSW.getMessage("TOTAL_HYRACKS_JOB" + " " + nodeJobSignature + " " + taskId,
                                        profilerSW.getStartTimeStamp()), true);
                        System.out.println("TOTAL_HYRACKS_JOB end " + nodeJobSignature);
                    }

                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

            @Override
            public void jobletStart() {
                try {
                    ((IAsterixAppRuntimeContext) jobletContext.getApplicationContext().getApplicationObject())
                            .getTransactionSubsystem().getTransactionManager().getTransactionContext(jobId, true);

                    // Added to measure the execution time when the profiler setting is enabled
                    if (ExecutionTimeProfiler.PROFILE_MODE) {
                        profilerSW = new ExecutionTimeStopWatch();
                        profilerSW.start();

                        // The key of this job: nodeId + JobId + Joblet hash code
                        nodeJobSignature = jobletContext.getApplicationContext().getNodeId() + "_"
                                + jobletContext.getJobId() + "_" + +jobletContext.hashCode();

                        taskId = "TOTAL_HYRACKS_JOB" + profilerSW.getStartTimeStamp();

                        // taskId: partition + taskId
                        OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(nodeJobSignature, taskId,
                                ExecutionTimeProfiler.INIT, false);
                        System.out.println("TOTAL_HYRACKS_JOB start " + nodeJobSignature);

                    }
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

        };
    }
}
