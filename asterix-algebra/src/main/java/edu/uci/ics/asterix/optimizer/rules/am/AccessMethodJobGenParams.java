/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;

/**
 * Helper class for reading and writing job-gen parameters for access methods to
 * and from a list of function arguments, typically of an unnest-map.
 */
public class AccessMethodJobGenParams {
    protected String indexName;
    protected IndexType indexType;
    protected String dataverseName;
    protected String datasetName;
    protected boolean retainInput;
    protected boolean retainNull;
    protected boolean requiresBroadcast;
    protected boolean isPrimaryIndex;
	// Does this index can generate false-positive results?
	// i.e., requires another verification (R-Tree, Length-partitioned n-gram or
	// keyword index)
    protected boolean canProduceFalsePositive;
    // In index-only plan, for a secondary index-search, we need to let the index know
    // that it needs to generate a variable that keeps the result of tryLock on PKs that are found.
    protected boolean splitValueForIndexOnlyPlanRequired;

    private final int NUM_PARAMS = 9;

    public AccessMethodJobGenParams() {
    }

    public AccessMethodJobGenParams(String indexName, IndexType indexType, String dataverseName, String datasetName,
            boolean retainInput, boolean retainNull, boolean requiresBroadcast) {
    	this(indexName, indexType, dataverseName, datasetName, retainInput, retainNull, requiresBroadcast, false);
    }

    public AccessMethodJobGenParams(String indexName, IndexType indexType, String dataverseName, String datasetName,
            boolean retainInput, boolean retainNull, boolean requiresBroadcast, boolean splitValueForIndexOnlyPlanRequired) {
        this.indexName = indexName;
        this.indexType = indexType;
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.retainInput = retainInput;
        this.retainNull = retainNull;
        this.requiresBroadcast = requiresBroadcast;
        this.isPrimaryIndex = datasetName.equals(indexName);
        this.canProduceFalsePositive = Index.canProduceFalsePositive(indexType);
        this.splitValueForIndexOnlyPlanRequired = splitValueForIndexOnlyPlanRequired;
    }

    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(indexName)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createInt32Constant(indexType.ordinal())));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(dataverseName)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(datasetName)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(retainInput)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(retainNull)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(requiresBroadcast)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(canProduceFalsePositive)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(splitValueForIndexOnlyPlanRequired)));
    }

    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        indexName = AccessMethodUtils.getStringConstant(funcArgs.get(0));
        indexType = IndexType.values()[AccessMethodUtils.getInt32Constant(funcArgs.get(1))];
        dataverseName = AccessMethodUtils.getStringConstant(funcArgs.get(2));
        datasetName = AccessMethodUtils.getStringConstant(funcArgs.get(3));
        retainInput = AccessMethodUtils.getBooleanConstant(funcArgs.get(4));
        retainNull = AccessMethodUtils.getBooleanConstant(funcArgs.get(5));
        requiresBroadcast = AccessMethodUtils.getBooleanConstant(funcArgs.get(6));
        isPrimaryIndex = datasetName.equals(indexName);
        canProduceFalsePositive = AccessMethodUtils.getBooleanConstant(funcArgs.get(7));
        splitValueForIndexOnlyPlanRequired = AccessMethodUtils.getBooleanConstant(funcArgs.get(8));
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public boolean getRetainInput() {
        return retainInput;
    }

    public boolean getRetainNull() {
        return retainNull;
    }

    public boolean getRequiresBroadcast() {
        return requiresBroadcast;
    }

    public boolean getCanProduceFalsePositive() {
        return canProduceFalsePositive;
    }

    public boolean getIsIndexOnlyPlanEnabled() {
        return splitValueForIndexOnlyPlanRequired;
    }

    protected void writeVarList(List<LogicalVariable> varList, List<Mutable<ILogicalExpression>> funcArgs) {
        Mutable<ILogicalExpression> numKeysRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                new AsterixConstantValue(new AInt32(varList.size()))));
        funcArgs.add(numKeysRef);
        for (LogicalVariable keyVar : varList) {
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(
                    new VariableReferenceExpression(keyVar));
            funcArgs.add(keyVarRef);
        }
    }

    protected int readVarList(List<Mutable<ILogicalExpression>> funcArgs, int index, List<LogicalVariable> varList) {
        int numLowKeys = AccessMethodUtils.getInt32Constant(funcArgs.get(index));
        if (numLowKeys > 0) {
            for (int i = 0; i < numLowKeys; i++) {
                LogicalVariable var = ((VariableReferenceExpression) funcArgs.get(index + 1 + i).getValue())
                        .getVariableReference();
                varList.add(var);
            }
        }
        return index + numLowKeys + 1;
    }

    protected int getNumParams() {
        return NUM_PARAMS;
    }

    public boolean isPrimaryIndex() {
        return isPrimaryIndex;
    }
}
