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
package org.apache.hyracks.algebricks.runtime.operators.aggreg;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class AggregateRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    // private int[] outColumns;
    private IAggregateEvaluatorFactory[] aggregFactories;

    public AggregateRuntimeFactory(IAggregateEvaluatorFactory[] aggregFactories) {
        super(null);
        // this.outColumns = outColumns;
        this.aggregFactories = aggregFactories;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("assign [");
        for (int i = 0; i < aggregFactories.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(aggregFactories[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            private IAggregateEvaluator[] aggregs = new IAggregateEvaluator[aggregFactories.length];
            private IPointable result = VoidPointable.FACTORY.createPointable();
            private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(aggregs.length);

            private boolean first = true;
            private boolean isOpen = false;

            // Temp: for debug purpose only
            protected int totalResultCount = 0;

            // For the index search only.
            protected long startTime = 0;
            protected long endTime = 0;
            protected long elapsedTime = 0;

            // For the entire duration between open() and close()
            protected long durationStartTime = 0;
            protected long durationEndTime = 0;
            protected long durationElapsedTime = 0;

            @Override
            public void open() throws HyracksDataException {
                // Temp:
                durationStartTime = System.currentTimeMillis();

                if (first) {
                    first = false;
                    initAccessAppendRef(ctx);
                    for (int i = 0; i < aggregFactories.length; i++) {
                        aggregs[i] = aggregFactories[i].createAggregateEvaluator(ctx);
                    }
                }
                for (int i = 0; i < aggregFactories.length; i++) {
                    aggregs[i].init();
                }
                isOpen = true;
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // Temp: debug
                startTime = System.currentTimeMillis();

                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    processTuple(tRef);
                }

                // Temp: debug
                endTime = System.currentTimeMillis();
                elapsedTime = elapsedTime + (endTime - startTime);
            }

            @Override
            public void close() throws HyracksDataException {
                if (isOpen) {
                    try {
                        // Temp: debug
                        startTime = System.currentTimeMillis();
                        computeAggregate();
                        // Temp: debug
                        endTime = System.currentTimeMillis();
                        elapsedTime = elapsedTime + (endTime - startTime);
                        appendToFrameFromTupleBuilder(tupleBuilder);
                    } finally {
                        super.close();
                    }
                }

                // Temp:
                durationEndTime = System.currentTimeMillis();
                durationElapsedTime = durationEndTime - durationStartTime;

                // Temp:
                String dateTimeNow = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss.SSS"));
                System.out.println(dateTimeNow + " AggregateRuntimeFactory.close() " + this.toString() + " "
                        + "\taggregation time(ms)\t" + elapsedTime + "\tduration(ms)\t" + durationElapsedTime
                        + "\tcount\t" + totalResultCount);

            }

            private void computeAggregate() throws HyracksDataException {
                tupleBuilder.reset();
                for (int f = 0; f < aggregs.length; f++) {
                    aggregs[f].finish(result);
                    tupleBuilder.addField(result.getByteArray(), result.getStartOffset(), result.getLength());
                }
            }

            private void processTuple(FrameTupleReference tupleRef) throws HyracksDataException {
                for (int f = 0; f < aggregs.length; f++) {
                    aggregs[f].step(tupleRef);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                if (isOpen) {
                    writer.fail();
                }
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder();
                sb.append("aggregate [");
                for (int i = 0; i < aggregFactories.length; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(aggregFactories[i]);
                }
                sb.append("]");
                return sb.toString();
            }
        };
    }
}
