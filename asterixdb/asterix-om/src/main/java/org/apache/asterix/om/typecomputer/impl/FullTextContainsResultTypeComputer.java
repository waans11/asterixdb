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
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class FullTextContainsResultTypeComputer extends AbstractResultTypeComputer {

    public static final FullTextContainsResultTypeComputer INSTANCE = new FullTextContainsResultTypeComputer();

    private FullTextContainsResultTypeComputer() {
    }

    @Override
    protected void checkArgType(String funcName, int argIndex, IAType type) throws AlgebricksException {
        ATypeTag actualTypeTag = type.getTypeTag();
        // Expression1 should be a string.
        if (argIndex == 0 && actualTypeTag != ATypeTag.STRING && actualTypeTag != ATypeTag.ANY) {
            throw new TypeMismatchException(funcName, argIndex, actualTypeTag, ATypeTag.STRING);
        }
        // Expression2 should be a string, or an (un)ordered list.
        if (argIndex == 1 && (actualTypeTag != ATypeTag.STRING && actualTypeTag != ATypeTag.UNORDEREDLIST
                && actualTypeTag != ATypeTag.ORDEREDLIST && actualTypeTag != ATypeTag.ANY)) {
            throw new TypeMismatchException(funcName, argIndex, actualTypeTag, ATypeTag.STRING, ATypeTag.UNORDEREDLIST,
                    ATypeTag.ORDEREDLIST);
        }
        // Each option name should be a string if it is already processed by FullTextContainsParameterCheckRule.
        // Before, the third argument should be a record if exists.
        // The structure is: arg2 = optionName1, arg3 = optionValue1, arg4 = optionName1, arg5 = optionValue2, ...
        if (argIndex > 1 && argIndex % 2 == 0 && (actualTypeTag != ATypeTag.STRING && actualTypeTag != ATypeTag.RECORD
                && actualTypeTag != ATypeTag.ANY)) {
            throw new TypeMismatchException(funcName, argIndex, actualTypeTag, ATypeTag.STRING, ATypeTag.RECORD);
        }
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        return BuiltinType.ABOOLEAN;
    }

}
