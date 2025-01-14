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

package org.apache.asterix.lang.sqlpp.clause;

import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.commons.lang3.ObjectUtils;

public abstract class AbstractBinaryCorrelateClause implements Clause {

    private JoinType joinType;
    private Expression rightExpr;
    private VariableExpr rightVar;
    private VariableExpr rightPosVar;

    public AbstractBinaryCorrelateClause(JoinType joinType, Expression rightExpr, VariableExpr rightVar,
            VariableExpr rightPosVar) {
        this.joinType = joinType;
        this.rightExpr = rightExpr;
        this.rightVar = rightVar;
        this.rightPosVar = rightPosVar;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Expression getRightExpression() {
        return rightExpr;
    }

    public void setRightExpression(Expression rightExpr) {
        this.rightExpr = rightExpr;
    }

    public VariableExpr getRightVariable() {
        return rightVar;
    }

    public VariableExpr getPositionalVariable() {
        return rightPosVar;
    }

    public boolean hasPositionalVariable() {
        return rightPosVar != null;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(joinType, rightExpr, rightPosVar, rightVar);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof AbstractBinaryCorrelateClause)) {
            return false;
        }
        AbstractBinaryCorrelateClause target = (AbstractBinaryCorrelateClause) object;
        return ObjectUtils.equals(joinType, target.joinType) && ObjectUtils.equals(rightExpr, target.rightExpr)
                && ObjectUtils.equals(rightPosVar, target.rightPosVar) && ObjectUtils.equals(rightVar, target.rightVar);
    }

}
