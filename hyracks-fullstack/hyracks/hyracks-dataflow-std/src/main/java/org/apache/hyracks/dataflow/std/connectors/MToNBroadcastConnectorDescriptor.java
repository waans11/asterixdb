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
package org.apache.hyracks.dataflow.std.connectors;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;
import org.apache.hyracks.dataflow.std.collectors.NonDeterministicChannelReader;
import org.apache.hyracks.dataflow.std.collectors.NonDeterministicFrameReader;
import org.apache.hyracks.dataflow.std.collectors.PartitionCollector;

public class MToNBroadcastConnectorDescriptor extends AbstractMToNConnectorDescriptor {

    private static final long serialVersionUID = 1L;

    public MToNBroadcastConnectorDescriptor(IConnectorDescriptorRegistry spec) {
        super(spec);
    }

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        final IFrameWriter[] epWriters = new IFrameWriter[nConsumerPartitions];
        final boolean[] isOpen = new boolean[nConsumerPartitions];
        for (int i = 0; i < nConsumerPartitions; ++i) {
            epWriters[i] = edwFactory.createFrameWriter(i);
        }
        return new IFrameWriter() {

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
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // Temp: debug
                startTime = System.currentTimeMillis();

                totalResultCount++;

                // Record the current position, instead of using buffer.mark().
                // The latter will be problematic because epWriters[i].nextFrame(buffer)
                // can flip or clear the buffer.
                int pos = buffer.position();
                for (int i = 0; i < epWriters.length; ++i) {
                    if (i != 0) {
                        buffer.position(pos);
                    }
                    // Temp: debug
                    endTime = System.currentTimeMillis();
                    elapsedTime = elapsedTime + (endTime - startTime);

                    epWriters[i].nextFrame(buffer);

                    // Temp: debug
                    startTime = System.currentTimeMillis();
                }

                // Temp: debug
                endTime = System.currentTimeMillis();
                elapsedTime = elapsedTime + (endTime - startTime);
            }

            @Override
            public void fail() throws HyracksDataException {
                HyracksDataException failException = null;
                for (int i = 0; i < epWriters.length; ++i) {
                    if (isOpen[i]) {
                        try {
                            epWriters[i].fail();
                        } catch (Throwable th) {
                            if (failException == null) {
                                failException = new HyracksDataException(th);
                            } else {
                                failException.addSuppressed(th);
                            }
                        }
                    }
                }
                if (failException != null) {
                    throw failException;
                }
            }

            @Override
            public void close() throws HyracksDataException {
                HyracksDataException closeException = null;
                for (int i = 0; i < epWriters.length; ++i) {
                    if (isOpen[i]) {
                        try {
                            epWriters[i].close();
                        } catch (Throwable th) {
                            if (closeException == null) {
                                closeException = new HyracksDataException(th);
                            } else {
                                closeException.addSuppressed(th);
                            }
                        }
                    }
                }
                if (closeException != null) {
                    throw closeException;
                }

                // Temp:
                durationEndTime = System.currentTimeMillis();
                durationElapsedTime = durationEndTime - durationStartTime;

                // Temp:
                String dateTimeNow = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss.SSS"));
                System.out.println(dateTimeNow + " MToNBroadcastConnectorDescriptor.close() " + getDisplayName() + " "
                        + "\tMtoN time(ms)\t" + elapsedTime + "\tduration(ms)\t" + durationElapsedTime
                        + "\tproducer count\t" + nProducerPartitions + "\t consumer count\t" + nConsumerPartitions
                        + "\tframe count\t" + totalResultCount);
            }

            @Override
            public void open() throws HyracksDataException {
                // Temp:
                durationStartTime = System.currentTimeMillis();

                for (int i = 0; i < epWriters.length; ++i) {
                    isOpen[i] = true;
                    epWriters[i].open();
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                for (IFrameWriter writer : epWriters) {
                    writer.flush();
                }
            }
        };
    }

    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc, int index,
            int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        BitSet expectedPartitions = new BitSet(nProducerPartitions);
        expectedPartitions.set(0, nProducerPartitions);
        NonDeterministicChannelReader channelReader = new NonDeterministicChannelReader(nProducerPartitions,
                expectedPartitions);
        NonDeterministicFrameReader frameReader = new NonDeterministicFrameReader(channelReader);
        return new PartitionCollector(ctx, getConnectorId(), index, expectedPartitions, frameReader, channelReader);
    }
}
