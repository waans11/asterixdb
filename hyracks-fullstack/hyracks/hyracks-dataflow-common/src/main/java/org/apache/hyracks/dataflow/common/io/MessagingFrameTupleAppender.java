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
package org.apache.hyracks.dataflow.common.io;

import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.util.IntSerDeUtils;

/**
 * A frame tuple appender that appends messages stored in the task context when pushing frames forward
 * This appender must only be used on network boundary
 */
public class MessagingFrameTupleAppender extends FrameTupleAppender {

    private final IHyracksTaskContext ctx;
    private static final int NULL_MESSAGE_SIZE = 1;
    public static final byte NULL_FEED_MESSAGE = 0x01;
    public static final byte ACK_REQ_FEED_MESSAGE = 0x02;
    public static final byte MARKER_MESSAGE = 0x03;
    private boolean initialized = false;
    private VSizeFrame message;

    public MessagingFrameTupleAppender(IHyracksTaskContext ctx) {
        this.ctx = ctx;
    }

    public static void printMessage(VSizeFrame message, PrintStream out) throws HyracksDataException {
        out.println(getMessageString(message));
    }

    public static String getMessageString(VSizeFrame message) throws HyracksDataException {
        StringBuilder aString = new StringBuilder();
        aString.append("Message Type: ");
        switch (getMessageType(message)) {
            case NULL_FEED_MESSAGE:
                aString.append("Null, ");
                break;
            case ACK_REQ_FEED_MESSAGE:
                aString.append("Ack Request, ");
                break;
            case MARKER_MESSAGE:
                aString.append("Marker, ");
                break;
            default:
                aString.append("Unknown, ");
                break;
        }
        aString.append("Message Length: ");
        int messageLength = message.getBuffer().remaining();
        aString.append(messageLength);
        return aString.toString();
    }

    public static byte getMessageType(VSizeFrame message) throws HyracksDataException {
        switch (message.getBuffer().array()[0]) {
            case NULL_FEED_MESSAGE:
                return NULL_FEED_MESSAGE;
            case ACK_REQ_FEED_MESSAGE:
                return ACK_REQ_FEED_MESSAGE;
            case MARKER_MESSAGE:
                return MARKER_MESSAGE;
            default:
                throw new HyracksDataException("Unknown message type");
        }
    }

    @Override
    protected boolean canHoldNewTuple(int fieldCount, int dataLength) throws HyracksDataException {
        if (hasEnoughSpace(fieldCount + 1, dataLength + NULL_MESSAGE_SIZE)) {
            return true;
        }
        if (tupleCount == 0) {
            frame.ensureFrameSize(FrameHelper.calcAlignedFrameSizeToStore(fieldCount + 1,
                    dataLength + NULL_MESSAGE_SIZE, frame.getMinSize()));
            reset(frame.getBuffer(), true);
            return true;
        }
        return false;
    }

    @Override
    public int getTupleCount() {
        // if message is set, there is always a message. that message could be a null message (TODO: optimize)
        return tupleCount + ((message == null) ? 0 : 1);
    }

    @Override
    public void write(IFrameWriter outWriter, boolean clearFrame) throws HyracksDataException {
        if (!initialized) {
            message = TaskUtil.<VSizeFrame> get(HyracksConstants.KEY_MESSAGE, ctx);
            initialized = true;
        }
        // If message fits, we append it, otherwise, we append a null message, then send a message only
        // frame with the message
        if (message == null) {
            if (tupleCount > 0) {
                appendNullMessage();
                forward(outWriter);
            }
        } else {
            ByteBuffer buffer = message.getBuffer();
            int messageSize = buffer.limit() - buffer.position();
            if (hasEnoughSpace(1, messageSize)) {
                appendMessage(buffer);
                forward(outWriter);
            } else {
                if (tupleCount > 0) {
                    appendNullMessage();
                    forward(outWriter);
                }
                if (!hasEnoughSpace(1, messageSize)) {
                    frame.ensureFrameSize(FrameHelper.calcAlignedFrameSizeToStore(1, messageSize, frame.getMinSize()));
                    reset(frame.getBuffer(), true);
                }
                appendMessage(buffer);
                forward(outWriter);
            }
        }
    }

    private void forward(IFrameWriter outWriter) throws HyracksDataException {
        getBuffer().clear();
        outWriter.nextFrame(getBuffer());
        frame.reset();
        reset(getBuffer(), true);
    }

    private void appendMessage(ByteBuffer message) {
        int messageLength = message.limit() - message.position();
        System.arraycopy(message.array(), message.position(), array, tupleDataEndOffset, messageLength);
        tupleDataEndOffset += messageLength;
        IntSerDeUtils.putInt(getBuffer().array(),
                FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1), tupleDataEndOffset);
        ++tupleCount;
        IntSerDeUtils.putInt(getBuffer().array(), FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);
    }

    private void appendNullMessage() {
        array[tupleDataEndOffset] = NULL_FEED_MESSAGE;
        tupleDataEndOffset++;
        IntSerDeUtils.putInt(getBuffer().array(),
                FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1), tupleDataEndOffset);
        ++tupleCount;
        IntSerDeUtils.putInt(getBuffer().array(), FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);
    }
}
