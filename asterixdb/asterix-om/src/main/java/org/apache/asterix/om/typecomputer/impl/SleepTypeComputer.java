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
package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.om.exceptions.TypeMismatchException;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class SleepTypeComputer extends AbstractResultTypeComputer {
    public static final SleepTypeComputer INSTANCE = new SleepTypeComputer();

    @Override
    public void checkArgType(String funcName, int argIndex, IAType type) throws AlgebricksException {
        if (argIndex == 1) {
            switch (type.getTypeTag()) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                    break;
                default:
                    throw new TypeMismatchException(funcName, argIndex, type.getTypeTag(), ATypeTag.INT8,
                            ATypeTag.INT16, ATypeTag.INT32, ATypeTag.INT64);
            }
        }
    }

    @Override
    public IAType getResultType(ILogicalExpression expr, IAType... types) throws AlgebricksException {
        return types[0];
    }
}
