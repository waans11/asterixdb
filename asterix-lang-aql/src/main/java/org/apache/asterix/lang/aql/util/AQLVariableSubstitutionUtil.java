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
package org.apache.asterix.lang.aql.util;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.aql.visitor.AQLCloneAndSubstituteVariablesVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;

public class AQLVariableSubstitutionUtil {

    public static ILangExpression substituteVariable(ILangExpression expression,
            Map<VariableExpr, Expression> varExprMap) throws AsterixException {
        AQLCloneAndSubstituteVariablesVisitor visitor = new AQLCloneAndSubstituteVariablesVisitor(
                new LangRewritingContext(0));
        VariableSubstitutionEnvironment env = new VariableSubstitutionEnvironment(varExprMap);
        return expression.accept(visitor, env).first;
    }

}
