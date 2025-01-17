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
package org.apache.hyracks.algebricks.runtime.operators.std;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class RunningAggregateRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final int[] outColumns;
    private final IRunningAggregateEvaluatorFactory[] runningAggregates;

    /**
     * @param outColumns
     *            a sorted array of columns into which the result is written to
     * @param runningAggregates
     * @param projectionList
     *            an array of columns to be projected
     */

    public RunningAggregateRuntimeFactory(int[] outColumns, IRunningAggregateEvaluatorFactory[] runningAggregates,
            int[] projectionList) {
        super(projectionList);
        this.outColumns = outColumns;
        this.runningAggregates = runningAggregates;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("running-aggregate [");
        for (int i = 0; i < outColumns.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(outColumns[i]);
        }
        sb.append("] := [");
        for (int i = 0; i < runningAggregates.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(runningAggregates[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        final int[] projectionToOutColumns = new int[projectionList.length];
        for (int j = 0; j < projectionList.length; j++) {
            projectionToOutColumns[j] = Arrays.binarySearch(outColumns, projectionList[j]);
        }

        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            private final IPointable p = VoidPointable.FACTORY.createPointable();
            private final IRunningAggregateEvaluator[] raggs = new IRunningAggregateEvaluator[runningAggregates.length];
            private final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(projectionList.length);
            private boolean first = true;
            private boolean isOpen = false;

            @Override
            public void open() throws HyracksDataException {
                initAccessAppendRef(ctx);
                if (first) {
                    first = false;
                    int n = runningAggregates.length;
                    for (int i = 0; i < n; i++) {
                        raggs[i] = runningAggregates[i].createRunningAggregateEvaluator(ctx);
                    }
                }
                for (int i = 0; i < runningAggregates.length; i++) {
                    raggs[i].init();
                }
                isOpen = true;
                writer.open();
            }

            @Override
            public void close() throws HyracksDataException {
                if (isOpen) {
                    super.close();
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                if (isOpen) {
                    super.fail();
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    produceTuple(tupleBuilder, tAccess, t, tRef);
                    appendToFrameFromTupleBuilder(tupleBuilder);
                }
            }

            private void produceTuple(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
                    FrameTupleReference tupleRef) throws HyracksDataException {
                tb.reset();
                for (int f = 0; f < projectionList.length; f++) {
                    int k = projectionToOutColumns[f];
                    if (k >= 0) {
                        raggs[k].step(tupleRef, p);
                        tb.addField(p.getByteArray(), p.getStartOffset(), p.getLength());
                    } else {
                        tb.addField(accessor, tIndex, projectionList[f]);
                    }
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                appender.flush(writer);
            }
        };
    }
}
