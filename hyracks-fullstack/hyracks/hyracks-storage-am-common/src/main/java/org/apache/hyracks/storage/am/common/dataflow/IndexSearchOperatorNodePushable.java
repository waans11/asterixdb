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
package org.apache.hyracks.storage.am.common.dataflow;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;

public abstract class IndexSearchOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    protected final IIndexOperatorDescriptor opDesc;
    protected final IHyracksTaskContext ctx;
    protected final IIndexDataflowHelper indexHelper;
    protected FrameTupleAccessor accessor;

    protected FrameTupleAppender appender;
    protected ArrayTupleBuilder tb;
    protected DataOutput dos;

    protected IIndex index;
    protected ISearchPredicate searchPred;
    protected IIndexCursor cursor;
    protected IIndexAccessor indexAccessor;

    protected final RecordDescriptor inputRecDesc;
    protected final boolean retainInput;
    protected FrameTupleReference frameTuple;

    protected final boolean retainMissing;
    protected ArrayTupleBuilder nonMatchTupleBuild;
    protected IMissingWriter nonMatchWriter;

    protected final int[] minFilterFieldIndexes;
    protected final int[] maxFilterFieldIndexes;
    protected PermutingFrameTupleReference minFilterKey;
    protected PermutingFrameTupleReference maxFilterKey;

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

    public IndexSearchOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            IRecordDescriptorProvider recordDescProvider, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes)
            throws HyracksDataException {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.indexHelper = opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(opDesc, ctx, partition);
        this.retainInput = opDesc.getRetainInput();
        this.retainMissing = opDesc.getRetainMissing();
        if (this.retainMissing) {
            this.nonMatchWriter = opDesc.getMissingWriterFactory().createMissingWriter();
        }
        this.inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        this.minFilterFieldIndexes = minFilterFieldIndexes;
        this.maxFilterFieldIndexes = maxFilterFieldIndexes;
        if (minFilterFieldIndexes != null && minFilterFieldIndexes.length > 0) {
            minFilterKey = new PermutingFrameTupleReference();
            minFilterKey.setFieldPermutation(minFilterFieldIndexes);
        }
        if (maxFilterFieldIndexes != null && maxFilterFieldIndexes.length > 0) {
            maxFilterKey = new PermutingFrameTupleReference();
            maxFilterKey.setFieldPermutation(maxFilterFieldIndexes);
        }
        // Temp:
        totalResultCount = 0;
        // For the index search only.
        startTime = 0;
        endTime = 0;
        elapsedTime = 0;
        // For the entire duration between open() and close()
        durationStartTime = 0;
        durationEndTime = 0;
        durationElapsedTime = 0;
    }

    protected abstract ISearchPredicate createSearchPredicate();

    protected abstract void resetSearchPredicate(int tupleIndex);

    protected IIndexCursor createCursor() {
        return indexAccessor.createSearchCursor(false);
    }

    protected abstract int getFieldCount();

    @Override
    public void open() throws HyracksDataException {
        // Temp:
        durationStartTime = System.currentTimeMillis();

        writer.open();
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        accessor = new FrameTupleAccessor(inputRecDesc);
        if (retainMissing) {
            int fieldCount = getFieldCount();
            nonMatchTupleBuild = new ArrayTupleBuilder(fieldCount);
            DataOutput out = nonMatchTupleBuild.getDataOutput();
            for (int i = 0; i < fieldCount; i++) {
                try {
                    nonMatchWriter.writeMissing(out);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                nonMatchTupleBuild.addFieldEndOffset();
            }
        } else {
            nonMatchTupleBuild = null;
        }

        try {
            searchPred = createSearchPredicate();
            tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(new VSizeFrame(ctx), true);
            ISearchOperationCallback searchCallback = opDesc.getSearchOpCallbackFactory()
                    .createSearchOperationCallback(indexHelper.getResource().getId(), ctx, null);
            indexAccessor = index.createAccessor(NoOpOperationCallback.INSTANCE, searchCallback);
            cursor = createCursor();
            if (retainInput) {
                frameTuple = new FrameTupleReference();
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    protected void writeSearchResults(int tupleIndex) throws Exception {
        boolean matched = false;
        while (cursor.hasNext()) {
            startTime = System.currentTimeMillis();
            matched = true;
            tb.reset();
            cursor.next();
            totalResultCount++;
            if (retainInput) {
                frameTuple.reset(accessor, tupleIndex);
                for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                    dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                }
            }
            ITupleReference tuple = cursor.getTuple();
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }

            // Temp:
            endTime = System.currentTimeMillis();
            elapsedTime = elapsedTime + (endTime - startTime);

            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        }

        if (!matched && retainInput && retainMissing) {
            FrameUtils.appendConcatToWriter(writer, appender, accessor, tupleIndex,
                    nonMatchTupleBuild.getFieldEndOffsets(), nonMatchTupleBuild.getByteArray(), 0,
                    nonMatchTupleBuild.getSize());
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // Temp: debug
        startTime = System.currentTimeMillis();

        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        // Temp: debug
        endTime = System.currentTimeMillis();

        elapsedTime = elapsedTime + (endTime - startTime);
        try {
            for (int i = 0; i < tupleCount; i++) {
                startTime = System.currentTimeMillis();
                resetSearchPredicate(i);
                cursor.reset();
                indexAccessor.search(cursor, searchPred);
                endTime = System.currentTimeMillis();
                elapsedTime = elapsedTime + (endTime - startTime);
                writeSearchResults(i);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        // Temp: debug
        long end = System.currentTimeMillis();
    }

    @Override
    public void flush() throws HyracksDataException {
        appender.flush(writer);
    }

    @Override
    public void close() throws HyracksDataException {
        HyracksDataException closeException = null;
        if (index != null) {
            // if index == null, then the index open was not successful
            try {
                if (appender.getTupleCount() > 0) {
                    appender.write(writer, true);
                }
            } catch (Throwable th) {
                closeException = new HyracksDataException(th);
            }

            try {
                cursor.close();
            } catch (Throwable th) {
                if (closeException == null) {
                    closeException = new HyracksDataException(th);
                } else {
                    closeException.addSuppressed(th);
                }
            }
            try {
                indexHelper.close();
            } catch (Throwable th) {
                if (closeException == null) {
                    closeException = new HyracksDataException(th);
                } else {
                    closeException.addSuppressed(th);
                }
            }
        }
        try {
            // will definitely be called regardless of exceptions
            writer.close();
        } catch (Throwable th) {
            if (closeException == null) {
                closeException = new HyracksDataException(th);
            } else {
                closeException.addSuppressed(th);
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
        System.out.println(dateTimeNow + " IndexSearchOperatorNodePushable.close() " + index.toString() + " "
                + searchPred.toString() + "\tsearch time(ms)\t" + elapsedTime + "\tduration(ms)\t" + durationElapsedTime
                + "\tcount\t" + totalResultCount);
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}
