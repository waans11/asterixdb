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
package org.apache.asterix.runtime.evaluators.common;

import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.OperatorExecutionTimeProfiler;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.BinaryEntry;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SimilarityJaccardCheckEvaluator extends SimilarityJaccardEvaluator {

    protected final IScalarEvaluator jaccThreshEval;
    protected float jaccThresh = -1f;
    protected IPointable jaccThreshPointable = new VoidPointable();

    protected OrderedListBuilder listBuilder;
    protected ArrayBackedValueStorage inputVal;
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ABoolean> booleanSerde = SerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    protected final AOrderedListType listType = new AOrderedListType(BuiltinType.ANY, "list");

    // Temp:
    protected long evaluateDurationStartTime = 0;
    protected long evaluateDurationEndTime = 0;
    protected long probeHashMapDurationStartTime = 0;
    protected long probeHashMapDurationEndTime = 0;
    protected long computeResultDurationStartTime = 0;
    protected long computeResultDurationEndTime = 0;
    protected long writeResultDurationStartTime = 0;
    protected long writeResultDurationEndTime = 0;
    protected long lengthFilterDurationStartTime = 0;
    protected long lengthFilterDurationEndTime = 0;

    public SimilarityJaccardCheckEvaluator(IScalarEvaluatorFactory[] args, IHyracksTaskContext context)
            throws HyracksDataException {
        super(args, context);
        jaccThreshEval = args[2].createScalarEvaluator(context);
        listBuilder = new OrderedListBuilder();
        inputVal = new ArrayBackedValueStorage();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        // Temp:
        evaluateDurationStartTime = System.currentTimeMillis();
        OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.processTupleCount.incrementAndGet();

        resultStorage.reset();

        firstOrdListEval.evaluate(tuple, argPtr1);
        secondOrdListEval.evaluate(tuple, argPtr2);
        jaccThreshEval.evaluate(tuple, jaccThreshPointable);

        firstTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                .deserialize(argPtr1.getByteArray()[argPtr1.getStartOffset()]);
        secondTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                .deserialize(argPtr2.getByteArray()[argPtr2.getStartOffset()]);

        firstItemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                .deserialize(argPtr1.getByteArray()[argPtr1.getStartOffset() + 1]);
        secondItemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                .deserialize(argPtr2.getByteArray()[argPtr2.getStartOffset() + 1]);

        jaccThresh = AFloatSerializerDeserializer.getFloat(jaccThreshPointable.getByteArray(),
                jaccThreshPointable.getStartOffset() + TYPE_INDICATOR_SIZE);

        if (!checkArgTypes(firstTypeTag, secondTypeTag)) {
            result.set(resultStorage);
            return;
        }
        if (prepareLists(argPtr1, argPtr2)) {
            // Temp:
            computeResultDurationStartTime = System.currentTimeMillis();
            jaccSim = computeResult();
            // Temp:
            computeResultDurationEndTime = System.currentTimeMillis();
            OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.computeResultDurationTime
                    .addAndGet(computeResultDurationEndTime - computeResultDurationStartTime);

        } else {
            jaccSim = 0.0f;
        }
        try {
            writeResult(jaccSim);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        result.set(resultStorage);

        // Temp:
        evaluateDurationEndTime = System.currentTimeMillis();
        OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.evaluateDurationTime
                .addAndGet(evaluateDurationEndTime - evaluateDurationStartTime);
    }

    @Override
    protected int probeHashMap(AbstractAsterixListIterator probeIter, int buildListSize, int probeListSize)
            throws HyracksDataException {
        // Temp:
        probeHashMapDurationStartTime = System.currentTimeMillis();
        lengthFilterDurationStartTime = probeHashMapDurationStartTime;

        // Apply length filter.
        int lengthLowerBound = (int) Math.ceil(jaccThresh * probeListSize);
        if ((lengthLowerBound > buildListSize)
                || (buildListSize > (int) Math.floor(1.0f / jaccThresh * probeListSize))) {
            lengthFilterDurationEndTime = System.currentTimeMillis();
            OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.lengthFilterDurationTime
                    .addAndGet(lengthFilterDurationEndTime - lengthFilterDurationStartTime);

            OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.lengthFilterAppliedTupleCount
                    .incrementAndGet();
            return -1;
        }
        // Probe phase: Probe items from second list, and compute intersection size.
        int intersectionSize = 0;
        int probeListCount = 0;
        int minUnionSize = buildListSize;
        while (probeIter.hasNext()) {
            probeListCount++;
            byte[] buf = probeIter.getData();
            int off = probeIter.getPos();
            int len = probeIter.getItemLen();
            keyEntry.set(buf, off, len);
            BinaryEntry entry = hashMap.get(keyEntry);
            if (entry != null) {
                // Increment second value.
                int firstValInt = IntegerPointable.getInteger(entry.getBuf(), entry.getOffset());
                // Irrelevant for the intersection size.
                if (firstValInt == 0) {
                    continue;
                }
                int secondValInt = IntegerPointable.getInteger(entry.getBuf(), entry.getOffset() + 4);
                // Subtract old min value.
                intersectionSize -= (firstValInt < secondValInt) ? firstValInt : secondValInt;
                secondValInt++;
                // Add new min value.
                intersectionSize += (firstValInt < secondValInt) ? firstValInt : secondValInt;
                IntegerPointable.setInteger(entry.getBuf(), entry.getOffset() + 4, secondValInt);
            } else {
                // Could not find element in other set. Increase min union size by 1.
                minUnionSize++;
                // Check whether jaccThresh can still be satisfied if there was a mismatch.
                int maxIntersectionSize = Math.min(buildListSize, intersectionSize + (probeListSize - probeListCount));
                int lowerBound = (int) Math.floor(jaccThresh * minUnionSize);
                if (maxIntersectionSize < lowerBound) {
                    // Cannot satisfy jaccThresh.
                    return -1;
                }
            }
            probeIter.next();
        }

        // Temp:
        probeHashMapDurationEndTime = System.currentTimeMillis();
        OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.probeHashMapDurationTime
                .addAndGet(probeHashMapDurationEndTime - probeHashMapDurationStartTime);

        return intersectionSize;
    }

    @Override
    protected void writeResult(float jacc) throws IOException {
        // Temp:
        writeResultDurationStartTime = System.currentTimeMillis();

        listBuilder.reset(listType);
        boolean matches = (jacc < jaccThresh) ? false : true;
        // Temp:
        if (matches) {
            System.out.println(matches);
        }
        inputVal.reset();
        booleanSerde.serialize(matches ? ABoolean.TRUE : ABoolean.FALSE, inputVal.getDataOutput());
        listBuilder.addItem(inputVal);

        inputVal.reset();
        aFloat.setValue((matches) ? jacc : 0.0f);
        floatSerde.serialize(aFloat, inputVal.getDataOutput());
        listBuilder.addItem(inputVal);

        listBuilder.write(out, true);

        // Temp:
        writeResultDurationEndTime = System.currentTimeMillis();
        OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.writeResultDurationTime
                .getAndAdd(writeResultDurationEndTime - writeResultDurationStartTime);

    }
}
