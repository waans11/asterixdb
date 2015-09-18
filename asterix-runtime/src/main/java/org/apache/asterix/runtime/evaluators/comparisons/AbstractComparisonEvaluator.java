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
package org.apache.asterix.runtime.evaluators.comparisons;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.comparators.ABinaryComparator;
import org.apache.asterix.dataflow.data.nontagged.comparators.ACirclePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ADurationPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ALinePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APoint3DPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APointPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APolygonPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ARectanglePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlBinaryTokenizerFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.BinaryComparatorConstant.ComparableResultCode;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringLowercasePointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.BinaryHashSet;
import org.apache.hyracks.data.std.util.BinaryHashSet.BinaryEntry;
import org.apache.hyracks.data.std.util.BinaryHashSet.BinaryHashSetIterator;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;

public abstract class AbstractComparisonEvaluator implements ICopyEvaluator {

    // CONTAINS is added for full-text search and it means true case
    protected enum ComparisonResult {
        LESS_THAN,
        EQUAL,
        GREATER_THAN,
        UNKNOWN,
        CONTAINS
    }

    ;

    protected DataOutput out;
    protected ArrayBackedValueStorage outLeft = new ArrayBackedValueStorage();
    protected ArrayBackedValueStorage outRight = new ArrayBackedValueStorage();
    protected ICopyEvaluator evalLeft;
    protected ICopyEvaluator evalRight;
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ABoolean> serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);
    protected IBinaryComparator strBinaryComp = AqlBinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE
            .createBinaryComparator();
    // For full-text search, we convert all strings to the lower case.
    // In addition, since each token does not include the length byte (2 bytes) in the beginning,
    // We need to have a different binary comparator that uses length parameter
    protected IBinaryComparator strLowerCaseCmp = AqlBinaryComparatorFactoryProvider.UTF8STRING_LOWERCASE_POINTABLE_INSTANCE
            .createBinaryWithoutLengthByteComparator();
    protected IBinaryComparator circleBinaryComp = ACirclePartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator durationBinaryComp = ADurationPartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator intervalBinaryComp = AIntervalPartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator lineBinaryComparator = ALinePartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator pointBinaryComparator = APointPartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator point3DBinaryComparator = APoint3DPartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator polygonBinaryComparator = APolygonPartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator rectangleBinaryComparator = ARectanglePartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected final IBinaryComparator byteArrayComparator = new PointableBinaryComparatorFactory(
            ByteArrayPointable.FACTORY).createBinaryComparator();

    // Following variables are added to conduct full-text search

    // To conduct the full-text search, we need a Tokenizer
    protected IBinaryTokenizer strTokenizerForLeftArray = AqlBinaryTokenizerFactoryProvider.INSTANCE
            .getWordTokenizerFactory(ATypeTag.STRING, false, true).createTokenizer();

    protected IBinaryTokenizer strTokenizerForRightArray = AqlBinaryTokenizerFactoryProvider.INSTANCE
            .getWordTokenizerFactory(ATypeTag.STRING, false, true).createTokenizer();

    // Case insensitive hash
    protected IBinaryHashFunction hashFunc = new PointableBinaryHashFunctionFactory(
            UTF8StringLowercasePointable.FACTORY).createBinaryHashWithoutLengthByteFunction();

    // Parameter: number of bucket, frame size, hashFunction, Comparator, byte array that contains the key
    //    protected BinaryHashSet leftHashSet = new BinaryHashSet(100, 32768, hashFunc, strLowerCaseCmp, null);
    protected BinaryHashSet rightHashSet = new BinaryHashSet(100, 32768, hashFunc, strLowerCaseCmp, null);

    // Checks whether the query array has been changed
    protected byte[] previousQueryArray = null;
    protected int previousQueryArrayLength = 0;

    // Query token count
    protected int queryTokenCount = 0;
    protected int occurrenceThreshold = 0;

    // keyEntry used in the hash-set
    protected BinaryEntry keyEntry = new BinaryEntry();

    public AbstractComparisonEvaluator(DataOutput out, ICopyEvaluatorFactory evalLeftFactory,
            ICopyEvaluatorFactory evalRightFactory) throws AlgebricksException {
        this.out = out;
        this.evalLeft = evalLeftFactory.createEvaluator(outLeft);
        this.evalRight = evalRightFactory.createEvaluator(outRight);
    }

    protected void evalInputs(IFrameTupleReference tuple) throws AlgebricksException {
        outLeft.reset();
        evalLeft.evaluate(tuple);
        outRight.reset();
        evalRight.evaluate(tuple);
    }

    // checks whether we can apply >, >=, <, and <= operations to the given type
    // since
    // these operations can not be defined for certain types.
    protected void checkTotallyOrderable() throws AlgebricksException {
        if (outLeft.getLength() != 0) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outLeft.getByteArray()[0]);
            switch (typeTag) {
                case DURATION:
                case INTERVAL:
                case LINE:
                case POINT:
                case POINT3D:
                case POLYGON:
                case CIRCLE:
                case RECTANGLE:
                    throw new AlgebricksException("Comparison operations (>, >=, <, and <=) for the " + typeTag
                            + " type are not defined.");
                default:
                    return;
            }
        }
    }

    // checks whether two types are comparable
    protected ComparableResultCode comparabilityCheck() {
        // just check TypeTags
        return ABinaryComparator.isComparable(outLeft.getByteArray(), 0, 1, outRight.getByteArray(), 0, 1);
    }

    protected ComparisonResult compareResults() throws AlgebricksException {
        boolean isLeftNull = false;
        boolean isRightNull = false;
        ATypeTag typeTag1 = null;
        ATypeTag typeTag2 = null;

        if (outLeft.getLength() == 0) {
            isLeftNull = true;
        } else {
            typeTag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outLeft.getByteArray()[0]);
            if (typeTag1 == ATypeTag.NULL) {
                isLeftNull = true;
            }
        }
        if (outRight.getLength() == 0) {
            isRightNull = true;
        } else {
            typeTag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outRight.getByteArray()[0]);
            if (typeTag2 == ATypeTag.NULL) {
                isRightNull = true;
            }
        }

        if (isLeftNull || isRightNull) {
            return ComparisonResult.UNKNOWN;
        }

        switch (typeTag1) {
            case INT8: {
                return compareInt8WithArg(typeTag2);
            }
            case INT16: {
                return compareInt16WithArg(typeTag2);
            }
            case INT32: {
                return compareInt32WithArg(typeTag2);
            }
            case INT64: {
                return compareInt64WithArg(typeTag2);
            }
            case FLOAT: {
                return compareFloatWithArg(typeTag2);
            }
            case DOUBLE: {
                return compareDoubleWithArg(typeTag2);
            }
            case STRING: {
                return compareStringWithArg(typeTag2);
            }
            case BOOLEAN: {
                return compareBooleanWithArg(typeTag2);
            }

            default: {
                return compareStrongTypedWithArg(typeTag1, typeTag2);
            }
        }
    }

    // Full-text search
    protected ComparisonResult containsResults() throws AlgebricksException {
        boolean isLeftNull = false;
        boolean isRightNull = false;
        ATypeTag typeTag1 = null;
        ATypeTag typeTag2 = null;

        if (outLeft.getLength() == 0) {
            isLeftNull = true;
        } else {
            typeTag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outLeft.getByteArray()[0]);
            if (typeTag1 == ATypeTag.NULL || outLeft.getByteArray()[0] == 0) {
                isLeftNull = true;
            }
        }
        if (outRight.getLength() == 0) {
            isRightNull = true;
        } else {
            typeTag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outRight.getByteArray()[0]);
            if (typeTag2 == ATypeTag.NULL || outRight.getByteArray()[0] == 0) {
                isRightNull = true;
            }
        }

        if (isLeftNull || isRightNull) {
            return ComparisonResult.UNKNOWN;
        }

        // Since the caller only requires CONTAINS as a true result,
        // we can return any value other than CONTAINS for a false result.
        if (typeTag1 != ATypeTag.STRING || typeTag2 != ATypeTag.STRING) {
            if (typeTag1.ordinal() < typeTag2.ordinal()) {
                return ComparisonResult.LESS_THAN;
            } else if (typeTag1.ordinal() > typeTag2.ordinal()) {
                return ComparisonResult.GREATER_THAN;
            }
        } else {
            return containsString();
        }
        return null;
    }

    // Full-text search evaluation
    private ComparisonResult containsString() throws AlgebricksException {
        try {

            // left: document,
            //			leftHashSet.clear();
            //			leftHashSet.setRefArray(outLeft.getByteArray());

            // tokenizer for the given keywords in a document
            strTokenizerForLeftArray.reset(outLeft.getByteArray(), 1, outLeft.getLength() - 1);

            BinaryHashSetIterator rightHashItr = (BinaryHashSetIterator) rightHashSet.iterator();

            // right: query. If the right side remains the same, we don't need to tokenize it again.
            if (previousQueryArray != outRight.getByteArray() || previousQueryArrayLength != outRight.getLength()) {
                rightHashSet.clear();
                rightHashSet.setRefArray(outRight.getByteArray());
                previousQueryArray = outRight.getByteArray();
                previousQueryArrayLength = outRight.getLength();
                queryTokenCount = 0;

                // tokenizer for the given keywords in the given query
                strTokenizerForRightArray.reset(outRight.getByteArray(), 1, outRight.getLength() - 1);

                // Create tokens from the given query
                while (strTokenizerForRightArray.hasNext()) {
                    strTokenizerForRightArray.next();
                    queryTokenCount++;

                    // Record the starting position and the length of the current token in the hash set
                    keyEntry.set(strTokenizerForRightArray.getToken().getStart(), strTokenizerForRightArray.getToken()
                            .getTokenLength());
                    rightHashSet.put(keyEntry);
                }

            } else {
                // The query remains the same. However, the count of each key should be reset to zero.
                while (rightHashItr.hasNextElement(true)) {
                    // hasNextElement(true) will clear the count of each key.
                    // Normal hasNext() is hasNextElement(false).
                    // Here, just wait until this loop finishes.
                }
            }

            int foundCount = 0;

            // This is where we apply a disjunctive or disjunctive condition.
            // Right now, it's the disjunctive condition (count:1)
            // For the conjunctive condition, it should be the number of query tokens.
            occurrenceThreshold = 1;

            // Create tokens from a document
            while (strTokenizerForLeftArray.hasNext()) {
                strTokenizerForLeftArray.next();

                // Checks whether this key exists in the query hash-set.
                // Record the starting position and the length of the current token in the hash set
                keyEntry.set(strTokenizerForLeftArray.getToken().getStart(), strTokenizerForLeftArray.getToken()
                        .getTokenLength());

                if (rightHashSet.find(keyEntry, outLeft.getByteArray()) == 1) {
                    foundCount++;
                    // This is where we apply a disjunctive or disjunctive condition.
                    // Right now, it's the disjunctive condition
                    if (foundCount >= occurrenceThreshold) {
                        return ComparisonResult.CONTAINS;
                    }
                }
            }

            // Traversed all tokens. However, we couldn't find all tokens.
            // Doesn't matter whether we return -1 or 1 as the false value.
            return ComparisonResult.LESS_THAN;

        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    private ComparisonResult compareStrongTypedWithArg(ATypeTag expectedTypeTag, ATypeTag actualTypeTag)
            throws AlgebricksException {
        if (expectedTypeTag != actualTypeTag) {
            throw new AlgebricksException("Comparison is undefined between " + expectedTypeTag + " and "
                    + actualTypeTag + ".");
        }
        int result = 0;
        try {
            switch (actualTypeTag) {
                case YEARMONTHDURATION:
                case TIME:
                case DATE:
                    result = Integer.compare(AInt32SerializerDeserializer.getInt(outLeft.getByteArray(), 1),
                            AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1));
                    break;
                case DAYTIMEDURATION:
                case DATETIME:
                    result = Long.compare(AInt64SerializerDeserializer.getLong(outLeft.getByteArray(), 1),
                            AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1));
                    break;
                case CIRCLE:
                    result = circleBinaryComp.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                            outRight.getByteArray(), 1, outRight.getLength() - 1);
                    break;
                case LINE:
                    result = lineBinaryComparator.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                            outRight.getByteArray(), 1, outRight.getLength() - 1);
                    break;
                case POINT:
                    result = pointBinaryComparator.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                            outRight.getByteArray(), 1, outRight.getLength() - 1);
                    break;
                case POINT3D:
                    result = point3DBinaryComparator.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                            outRight.getByteArray(), 1, outRight.getLength() - 1);
                    break;
                case POLYGON:
                    result = polygonBinaryComparator.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                            outRight.getByteArray(), 1, outRight.getLength() - 1);
                    break;
                case DURATION:
                    result = durationBinaryComp.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                            outRight.getByteArray(), 1, outRight.getLength() - 1);
                    break;
                case INTERVAL:
                    result = intervalBinaryComp.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                            outRight.getByteArray(), 1, outRight.getLength() - 1);
                    break;
                case RECTANGLE:
                    result = rectangleBinaryComparator.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                            outRight.getByteArray(), 1, outRight.getLength() - 1);
                    break;
                case BINARY:
                    result = byteArrayComparator.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                            outRight.getByteArray(), 1, outRight.getLength() - 1);
                    break;
                default:
                    throw new AlgebricksException("Comparison for " + actualTypeTag + " is not supported.");
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        if (result == 0) {
            return ComparisonResult.EQUAL;
        } else if (result < 0) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private ComparisonResult compareBooleanWithArg(ATypeTag typeTag2) throws AlgebricksException {
        if (typeTag2 == ATypeTag.BOOLEAN) {
            byte b0 = outLeft.getByteArray()[1];
            byte b1 = outRight.getByteArray()[1];
            return compareByte(b0, b1);
        }
        throw new AlgebricksException("Comparison is undefined between types ABoolean and " + typeTag2 + " .");
    }

    private ComparisonResult compareStringWithArg(ATypeTag typeTag2) throws AlgebricksException {
        if (typeTag2 == ATypeTag.STRING) {
            int result;
            try {
                result = strBinaryComp.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                        outRight.getByteArray(), 1, outRight.getLength() - 1);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            if (result == 0) {
                return ComparisonResult.EQUAL;
            } else if (result < 0) {
                return ComparisonResult.LESS_THAN;
            } else {
                return ComparisonResult.GREATER_THAN;
            }
        }
        throw new AlgebricksException("Comparison is undefined between types AString and " + typeTag2 + " .");
    }

    private ComparisonResult compareDoubleWithArg(ATypeTag typeTag2) throws AlgebricksException {
        double s = ADoubleSerializerDeserializer.getDouble(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types ADouble and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareFloatWithArg(ATypeTag typeTag2) throws AlgebricksException {
        float s = FloatPointable.getFloat(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AFloat and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt64WithArg(ATypeTag typeTag2) throws AlgebricksException {
        long s = AInt64SerializerDeserializer.getLong(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt64 and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt32WithArg(ATypeTag typeTag2) throws AlgebricksException {
        int s = IntegerPointable.getInteger(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareInt(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareInt(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareInt(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt32 and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt16WithArg(ATypeTag typeTag2) throws AlgebricksException {
        short s = AInt16SerializerDeserializer.getShort(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareShort(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareShort(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareInt(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt16 and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt8WithArg(ATypeTag typeTag2) throws AlgebricksException {
        byte s = AInt8SerializerDeserializer.getByte(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareByte(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareShort(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareInt(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt16 and " + typeTag2 + " .");
            }
        }
    }

    private final ComparisonResult compareByte(int v1, int v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareShort(int v1, int v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareInt(int v1, int v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareLong(long v1, long v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareFloat(float v1, float v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareDouble(double v1, double v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

}
