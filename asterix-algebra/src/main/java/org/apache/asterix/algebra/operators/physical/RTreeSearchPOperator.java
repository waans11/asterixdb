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
package org.apache.asterix.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.RTreeJobGenParams;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;

/**
 * Contributes the runtime operator for an unnest-map representing a RTree
 * search.
 */
public class RTreeSearchPOperator extends IndexSearchPOperator {

    public RTreeSearchPOperator(IDataSourceIndex<String, AqlSourceId> idx, boolean requiresBroadcast) {
        super(idx, requiresBroadcast);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.RTREE_SEARCH;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        // We have two types of unnest-map operator - UNNEST_MAP and LEFT_OUTER_UNNEST_MAP
        UnnestMapOperator unnestMapOp = null;
        LeftOuterUnnestMapOperator leftOuterUnnestMapOp = null;
        ILogicalOperator unnestMap = null;
        ILogicalExpression unnestExpr = null;
        List<LogicalVariable> minFilterVarList = null;
        List<LogicalVariable> maxFilterVarList = null;
        List<LogicalVariable> outputVars = null;
        if (op.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            unnestMapOp = (UnnestMapOperator) op;
            unnestExpr = unnestMapOp.getExpressionRef().getValue();
            minFilterVarList = unnestMapOp.getMinFilterVars();
            maxFilterVarList = unnestMapOp.getMaxFilterVars();
            outputVars = unnestMapOp.getVariables();
            unnestMap = unnestMapOp;
        } else if (op.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP) {
            leftOuterUnnestMapOp = (LeftOuterUnnestMapOperator) op;
            unnestExpr = leftOuterUnnestMapOp.getExpressionRef().getValue();
            minFilterVarList = leftOuterUnnestMapOp.getMinFilterVars();
            maxFilterVarList = leftOuterUnnestMapOp.getMaxFilterVars();
            outputVars = leftOuterUnnestMapOp.getVariables();
            unnestMap = leftOuterUnnestMapOp;
        }
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
        FunctionIdentifier funcIdent = unnestFuncExpr.getFunctionIdentifier();
        if (!funcIdent.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
            return;
        }
        RTreeJobGenParams jobGenParams = new RTreeJobGenParams();
        jobGenParams.readFromFuncArgs(unnestFuncExpr.getArguments());
        int[] keyIndexes = getKeyIndexes(jobGenParams.getKeyVarList(), inputSchemas);

        int[] minFilterFieldIndexes = getKeyIndexes(minFilterVarList, inputSchemas);
        int[] maxFilterFieldIndexes = getKeyIndexes(maxFilterVarList, inputSchemas);

        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        Dataset dataset = mp.findDataset(jobGenParams.getDataverseName(), jobGenParams.getDatasetName());
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(unnestMap);
        if (jobGenParams.getRetainInput()) {
            outputVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(unnestMap, outputVars);
        }
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> rtreeSearch = mp.buildRtreeRuntime(
                builder.getJobSpec(), outputVars, opSchema, typeEnv, context, jobGenParams.getRetainInput(),
                jobGenParams.getRetainNull(), dataset, jobGenParams.getIndexName(), keyIndexes, minFilterFieldIndexes,
                maxFilterFieldIndexes, jobGenParams.getRequireSplitValueForIndexOnlyPlan(),
                jobGenParams.getLimitNumberOfResult());

        builder.contributeHyracksOperator(unnestMap, rtreeSearch.first);
        builder.contributeAlgebricksPartitionConstraint(rtreeSearch.first, rtreeSearch.second);
        ILogicalOperator srcExchange = unnestMap.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMap, 0);
    }
}
