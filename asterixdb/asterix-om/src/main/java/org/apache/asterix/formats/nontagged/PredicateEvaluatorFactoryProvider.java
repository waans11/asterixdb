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

package org.apache.asterix.formats.nontagged;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;

/*
Provides PredicateEvaluator for equi-join cases to properly take care of NULL fields, being compared with each other.
If any of the join keys, from either side, is NULL, record should not pass equi-join condition.
*/
public class PredicateEvaluatorFactoryProvider implements IPredicateEvaluatorFactoryProvider {

    private static final long serialVersionUID = 1L;
    public static final PredicateEvaluatorFactoryProvider INSTANCE = new PredicateEvaluatorFactoryProvider();

    @Override
    public IPredicateEvaluatorFactory getPredicateEvaluatorFactory(final int[] keys0, final int[] keys1) {

        return new IPredicateEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IPredicateEvaluator createPredicateEvaluator() {
                return new IPredicateEvaluator() {

                    @Override
                    public boolean evaluate(IFrameTupleAccessor fta0, int tupId0, IFrameTupleAccessor fta1,
                            int tupId1) {

                        int tStart0 = fta0.getTupleStartOffset(tupId0);
                        int fStartOffset0 = fta0.getFieldSlotsLength() + tStart0;

                        for (int k0 : keys0) {
                            int fieldStartIx = fta0.getFieldStartOffset(tupId0, k0);
                            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                                    .deserialize(fta0.getBuffer().array()[fieldStartIx + fStartOffset0]);
                            if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
                                return false;
                            }
                        }

                        int tStart1 = fta1.getTupleStartOffset(tupId1);
                        int fStartOffset1 = fta1.getFieldSlotsLength() + tStart1;

                        for (int k1 : keys1) {
                            int fieldStartIx = fta1.getFieldStartOffset(tupId1, k1);
                            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                                    .deserialize(fta1.getBuffer().array()[fieldStartIx + fStartOffset1]);
                            if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
                                return false;
                            }
                        }

                        return true; //none of the fields (from both sides) is NULL
                    }
                };
            }
        };
    }

}
