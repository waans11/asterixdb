/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.algebra.operators.physical;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.ILogManager;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.common.transactions.LogRecord;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.util.ExperimentProfiler;
import edu.uci.ics.hyracks.api.util.OperatorExecutionTimeProfiler;
import edu.uci.ics.hyracks.api.util.StopWatch;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.MurmurHash128Bit;

public class CommitRuntime implements IPushRuntime {

    private final static long SEED = 0L;

    private final IHyracksTaskContext hyracksTaskCtx;
    private final ITransactionManager transactionManager;
    private final ILogManager logMgr;
    private final JobId jobId;
    private final int datasetId;
    private final int[] primaryKeyFields;
    private final boolean isTemporaryDatasetWriteJob;
    private final boolean isWriteTransaction;
    private final long[] longHashes;
    private final LogRecord logRecord;

    private ITransactionContext transactionContext;
    private FrameTupleAccessor frameTupleAccessor;
    private final FrameTupleReference frameTupleReference;

	// For Experiment Profiler
	private StopWatch profilerSW;
	private String nodeJobSignature;
	private String taskId;

    public CommitRuntime(IHyracksTaskContext ctx, JobId jobId, int datasetId, int[] primaryKeyFields,
            boolean isTemporaryDatasetWriteJob, boolean isWriteTransaction) {
        this.hyracksTaskCtx = ctx;
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.transactionManager = runtimeCtx.getTransactionSubsystem().getTransactionManager();
        this.logMgr = runtimeCtx.getTransactionSubsystem().getLogManager();
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.primaryKeyFields = primaryKeyFields;
        this.frameTupleReference = new FrameTupleReference();
        this.isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob;
        this.isWriteTransaction = isWriteTransaction;
        this.longHashes = new long[2];
        this.logRecord = new LogRecord();
    }

    @Override
    public void open() throws HyracksDataException {
		// For Experiment Profiler
		if (ExperimentProfiler.PROFILE_MODE) {
			profilerSW = new StopWatch();
			profilerSW.start();

			// The key of this job: nodeId + JobId + Joblet hash code
			nodeJobSignature = hyracksTaskCtx.getJobletContext()
					.getApplicationContext().getNodeId()
					+ hyracksTaskCtx.getJobletContext().getJobId()
					+ hyracksTaskCtx.getJobletContext().hashCode();

			// taskId: partition + taskId
			taskId = hyracksTaskCtx.getTaskAttemptId()
					+ this.toString()
					+ profilerSW.getStartTimeStamp();

			// Initialize the counter for this runtime instance
			OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler
					.add(nodeJobSignature, taskId, "init", false);
			System.out.println("EXTENSION_OPERATOR start "
					+ nodeJobSignature + " " + taskId);
		}
        try {
            transactionContext = transactionManager.getTransactionContext(jobId, false);
            transactionContext.setWriteTxn(isWriteTransaction);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
		// For Experiment Profiler
		if (ExperimentProfiler.PROFILE_MODE) {
			profilerSW.resume();
		}
        int pkHash = 0;
        frameTupleAccessor.reset(buffer);
        int nTuple = frameTupleAccessor.getTupleCount();
        for (int t = 0; t < nTuple; t++) {
            if (isTemporaryDatasetWriteJob) {
                /**
                 * This "if branch" is for writes over temporary datasets.
                 * A temporary dataset does not require any lock and does not generate any write-ahead
                 * update and commit log but generates flush log and job commit log.
                 * However, a temporary dataset still MUST guarantee no-steal policy so that this
                 * notification call should be delivered to PrimaryIndexOptracker and used correctly in order
                 * to decrement number of active operation count of PrimaryIndexOptracker.
                 * By maintaining the count correctly and only allowing flushing when the count is 0, it can
                 * guarantee the no-steal policy for temporary datasets, too.
                 */
                transactionContext.notifyOptracker(false);
            } else {
                frameTupleReference.reset(frameTupleAccessor, t);
                pkHash = computePrimaryKeyHashValue(frameTupleReference, primaryKeyFields);
                logRecord.formEntityCommitLogRecord(transactionContext, datasetId, pkHash, frameTupleReference,
                        primaryKeyFields);
                try {
                    logMgr.log(logRecord);
                } catch (ACIDException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
		// For Experiment Profiler
		if (ExperimentProfiler.PROFILE_MODE) {
			profilerSW.suspend();
		}
    }

    private int computePrimaryKeyHashValue(ITupleReference tuple, int[] primaryKeyFields) {
        MurmurHash128Bit.hash3_x64_128(tuple, primaryKeyFields, SEED, longHashes);
        return Math.abs((int) longHashes[0]);
    }

    @Override
    public void fail() throws HyracksDataException {
		// For Experiment Profiler
		if (ExperimentProfiler.PROFILE_MODE) {
			profilerSW.finish();
			OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler
					.add(nodeJobSignature, taskId, profilerSW
							.getMessage(
									"EXTENSION_OPERATOR fail",
									profilerSW.getStartTimeStamp()),
							false);
			System.out.println("EXTENSION_OPERATOR fail end " + taskId);

		}
    }

    @Override
    public void close() throws HyracksDataException {
		// For Experiment Profiler
		if (ExperimentProfiler.PROFILE_MODE) {
			profilerSW.finish();
			OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler
					.add(nodeJobSignature, taskId, profilerSW
							.getMessage(
									"EXTENSION_OPERATOR",
									profilerSW.getStartTimeStamp()),
							false);
			System.out.println("EXTENSION_OPERATOR end " + taskId);

		}
    }

    @Override
    public void setFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        throw new IllegalStateException();
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        this.frameTupleAccessor = new FrameTupleAccessor(recordDescriptor);
    }
}
