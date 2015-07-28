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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Quadruple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;

/**
 * Class for helping rewrite rules to choose and apply RTree indexes.
 */
public class RTreeAccessMethod implements IAccessMethod {

    // Second boolean value means that this function can produce false positive results.
    // That is, index-search alone cannot replace SELECT condition and
    // SELECT condition needs to be applied afterwards to get the correct results.
    // In R-Tree case, depends on the parameters of the SPATIAL_INTERSECT function, it may/may not produce false positive results.
    // Thus, we need to have one more step to check whether the SPATIAL_INTERSECT generates the false positive or not.
    private static List<Pair<FunctionIdentifier, Boolean>> funcIdents = new ArrayList<Pair<FunctionIdentifier, Boolean>>();
    static {
        funcIdents.add(new Pair<FunctionIdentifier, Boolean>(AsterixBuiltinFunctions.SPATIAL_INTERSECT, false));
    }

    public static RTreeAccessMethod INSTANCE = new RTreeAccessMethod();

    @Override
    public List<Pair<FunctionIdentifier, Boolean>> getOptimizableFunctions() {
        return funcIdents;
    }

    @Override
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests, AccessMethodAnalysisContext analysisCtx) {
        boolean matches = AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx);
        if (!matches) {
            matches = AccessMethodUtils.analyzeFuncExprArgsForTwoVars(funcExpr, analysisCtx);
        }
        return matches;
    }

    @Override
    public boolean matchAllIndexExprs() {
        return true;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        return false;
    }

    @Override
    public boolean applySelectPlanTransformation(List<Mutable<ILogicalOperator>> aboveSelectRefs,
            Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context) throws AlgebricksException {
        SelectOperator select = (SelectOperator) selectRef.getValue();
        Mutable<ILogicalExpression> conditionRef = select.getCondition();
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) conditionRef;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();

        ARecordType recordType = subTree.recordType;

        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(chosenIndex, analysisCtx);

        int optFieldIdx = AccessMethodUtils.chooseFirstOptFuncVar(chosenIndex, analysisCtx);
        Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(optFuncExpr.getFieldType(optFieldIdx),
                optFuncExpr.getFieldName(optFieldIdx), recordType);
        if (keyPairType == null) {
            return false;
        }

        // Check whether assign (unnest) operator exists before the select operator
        Mutable<ILogicalOperator> assignBeforeSelectOpRef = (subTree.assignsAndUnnestsRefs.isEmpty()) ? null
                : subTree.assignsAndUnnestsRefs.get(0);
        ILogicalOperator assignBeforeSelectOp = null;
        if (assignBeforeSelectOpRef != null) {
            assignBeforeSelectOp = assignBeforeSelectOpRef.getValue();
        }

        // index-only plan possible?
        boolean isIndexOnlyPlanPossible = false;

        // secondary key field usage in the select condition
        boolean secondaryKeyFieldUsedInSelectCondition = false;

        // secondary key field usage after the select operator
        boolean secondaryKeyFieldUsedAfterSelectOp = false;

        // For R-Tree only: whether a verification is required after the secondary index search
        boolean verificationAfterSIdxSearchRequired = true;

        // Can the chosen method generate any false positive results?
        // Currently, for the B+ Tree index, there cannot be any false positive results.
        boolean noFalsePositiveResultsFromSIdxSearch = true;
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            ILogicalExpression argExpr = arg.getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) argExpr;
            FunctionIdentifier argFuncIdent = argFuncExpr.getFunctionIdentifier();
            for (int i = 0; i < funcIdents.size(); i++) {
                if (argFuncIdent == funcIdents.get(i).first) {
                    noFalsePositiveResultsFromSIdxSearch = funcIdents.get(i).second;
                    if (!noFalsePositiveResultsFromSIdxSearch) {
                        break;
                    }
                }
            }
            if (!noFalsePositiveResultsFromSIdxSearch) {
                break;
            }
        }

        // Preliminary index-only condition for R-Tree:
        // If an index is not built on a POINT or a RECTANGLE field, the query result can include false positives.
        // And the result from secondary index search is an MBR, we can't construct original secondary field value
        // to remove any false positive results.
        if (keyPairType.first != BuiltinType.APOINT && keyPairType.first != BuiltinType.ARECTANGLE) {
            isIndexOnlyPlanPossible = false;
            noFalsePositiveResultsFromSIdxSearch = false;
        } else {
            isIndexOnlyPlanPossible = true;
            noFalsePositiveResultsFromSIdxSearch = true;
        }

        Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanCheck = new Quadruple<Boolean, Boolean, Boolean, Boolean>(
                isIndexOnlyPlanPossible, secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp,
                verificationAfterSIdxSearchRequired);

        Dataset dataset = subTree.dataset;

        // Is this plan an index-only plan?
        if (isIndexOnlyPlanPossible && noFalsePositiveResultsFromSIdxSearch) {
            if (dataset.getDatasetType() == DatasetType.INTERNAL && noFalsePositiveResultsFromSIdxSearch) {
                indexOnlyPlanCheck = AccessMethodUtils.isIndexOnlyPlan(aboveSelectRefs, selectRef, subTree,
                        chosenIndex, analysisCtx, context);

                if (indexOnlyPlanCheck == null) {
                    isIndexOnlyPlanPossible = false;
                } else {
                    isIndexOnlyPlanPossible = indexOnlyPlanCheck.first;
                    secondaryKeyFieldUsedInSelectCondition = indexOnlyPlanCheck.second;
                    secondaryKeyFieldUsedAfterSelectOp = indexOnlyPlanCheck.third;
                    verificationAfterSIdxSearchRequired = indexOnlyPlanCheck.fourth;
                }
            } else {
                // For a index on an external dataset can't be optimized for the index-only plan.
                isIndexOnlyPlanPossible = false;
            }
        }

        if (isIndexOnlyPlanPossible) {
            analysisCtx.setIndexOnlyPlanEnabled(true);
        } else {
            analysisCtx.setIndexOnlyPlanEnabled(false);
        }

        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(aboveSelectRefs, selectRef,
                assignBeforeSelectOpRef, subTree, null, chosenIndex, optFuncExpr, analysisCtx, false, false, false,
                context, isIndexOnlyPlanPossible, verificationAfterSIdxSearchRequired,
                secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp,
                noFalsePositiveResultsFromSIdxSearch);
        if (primaryIndexUnnestOp == null) {
            return false;
        }
        // Replace the datasource scan with the new plan rooted at primaryIndexUnnestMap.

        if (!isIndexOnlyPlanPossible) {
            subTree.dataSourceRef.setValue(primaryIndexUnnestOp);
        } else {
            selectRef.setValue(primaryIndexUnnestOp);
        }
        return true;
    }

    @Override
    public boolean applyJoinPlanTransformation(Mutable<ILogicalOperator> joinRef,
            OptimizableOperatorSubTree leftSubTree, OptimizableOperatorSubTree rightSubTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context, boolean isLeftOuterJoin,
            boolean hasGroupBy) throws AlgebricksException {
        // Determine if the index is applicable on the left or right side (if both, we arbitrarily prefer the left side).
        Dataset dataset = analysisCtx.indexDatasetMap.get(chosenIndex);
        // Determine probe and index subtrees based on chosen index.
        OptimizableOperatorSubTree indexSubTree = null;
        OptimizableOperatorSubTree probeSubTree = null;
        if (!isLeftOuterJoin && leftSubTree.hasDataSourceScan()
                && dataset.getDatasetName().equals(leftSubTree.dataset.getDatasetName())) {
            indexSubTree = leftSubTree;
            probeSubTree = rightSubTree;
        } else if (rightSubTree.hasDataSourceScan()
                && dataset.getDatasetName().equals(rightSubTree.dataset.getDatasetName())) {
            indexSubTree = rightSubTree;
            probeSubTree = leftSubTree;
        }
        if (indexSubTree == null) {
            //This may happen for left outer join case
            return false;
        }

        LogicalVariable newNullPlaceHolderVar = null;
        if (isLeftOuterJoin) {
            //get a new null place holder variable that is the first field variable of the primary key
            //from the indexSubTree's datasourceScanOp
            newNullPlaceHolderVar = indexSubTree.getDataSourceVariables().get(0);
        }

        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(chosenIndex, analysisCtx);
        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(null, joinRef, null, indexSubTree,
                probeSubTree, chosenIndex, optFuncExpr, analysisCtx, true, isLeftOuterJoin, true, context, false,
                false, false, false, false);
        if (primaryIndexUnnestOp == null) {
            return false;
        }

        if (isLeftOuterJoin && hasGroupBy) {
            //reset the null place holder variable
            AccessMethodUtils.resetLOJNullPlaceholderVariableInGroupByOp(analysisCtx, newNullPlaceHolderVar, context);
        }

        indexSubTree.dataSourceRef.setValue(primaryIndexUnnestOp);
        // Change join into a select with the same condition.
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinRef.getValue();
        SelectOperator topSelect = new SelectOperator(joinOp.getCondition(), isLeftOuterJoin, newNullPlaceHolderVar);
        topSelect.getInputs().add(indexSubTree.rootRef);
        topSelect.setExecutionMode(ExecutionMode.LOCAL);
        context.computeAndSetTypeEnvironmentForOperator(topSelect);
        // Replace the original join with the new subtree rooted at the select op.
        joinRef.setValue(topSelect);
        return true;
    }

    private ILogicalOperator createSecondaryToPrimaryPlan(List<Mutable<ILogicalOperator>> afterTopRefs,
            Mutable<ILogicalOperator> topRef, Mutable<ILogicalOperator> assignBeforeTopRef,
            OptimizableOperatorSubTree indexSubTree, OptimizableOperatorSubTree probeSubTree, Index chosenIndex,
            IOptimizableFuncExpr optFuncExpr, AccessMethodAnalysisContext analysisCtx, boolean retainInput,
            boolean retainNull, boolean requiresBroadcast, IOptimizationContext context,
            boolean isIndexOnlyPlanPossible, boolean verificationAfterSIdxSearchRequired,
            boolean secondaryKeyFieldUsedInSelectCondition, boolean secondaryKeyFieldUsedAfterSelectOp,
            boolean noFalsePositiveResultsFromSIdxSearch) throws AlgebricksException {
        Dataset dataset = indexSubTree.dataset;
        ARecordType recordType = indexSubTree.recordType;

        int optFieldIdx = AccessMethodUtils.chooseFirstOptFuncVar(chosenIndex, analysisCtx);
        Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(optFuncExpr.getFieldType(optFieldIdx),
                optFuncExpr.getFieldName(optFieldIdx), recordType);
        if (keyPairType == null) {
            return null;
        }

        // Get the number of dimensions corresponding to the field indexed by chosenIndex.
        IAType spatialType = keyPairType.first;
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numSecondaryKeys = numDimensions * 2;
        boolean isIndexOnlyPlanEnabled = analysisCtx.isIndexOnlyPlanEnabled();

        // we made sure indexSubTree has datasource scan
        AbstractDataSourceOperator dataSourceOp = (AbstractDataSourceOperator) indexSubTree.dataSourceRef.getValue();
        RTreeJobGenParams jobGenParams = new RTreeJobGenParams(chosenIndex.getIndexName(), IndexType.RTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, retainNull, requiresBroadcast,
                isIndexOnlyPlanEnabled);
        // A spatial object is serialized in the constant of the func expr we are optimizing.
        // The R-Tree expects as input an MBR represented with 1 field per dimension.
        // Here we generate vars and funcs for extracting MBR fields from the constant into fields of a tuple (as the R-Tree expects them).
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        // List of expressions for the assign.
        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        //        Pair<ILogicalExpression, Boolean> returnedSearchKeyExpr = AccessMethodUtils.createSearchKeyExpr(optFuncExpr,
        //                indexSubTree, probeSubTree);
        ILogicalExpression searchKeyExpr = AccessMethodUtils.createSearchKeyExpr(optFuncExpr, indexSubTree,
                probeSubTree).first;

        for (int i = 0; i < numSecondaryKeys; i++) {
            // The create MBR function "extracts" one field of an MBR around the given spatial object.
            AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CREATE_MBR));
            // Spatial object is the constant from the func expr we are optimizing.
            createMBR.getArguments().add(new MutableObject<ILogicalExpression>(searchKeyExpr));
            // The number of dimensions.
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(
                            numDimensions)))));
            // Which part of the MBR to extract.
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(
                            new AsterixConstantValue(new AInt32(i)))));
            // Add a variable and its expr to the lists which will be passed into an assign op.
            LogicalVariable keyVar = context.newVar();
            keyVarList.add(keyVar);
            keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
        }
        jobGenParams.setKeyVarList(keyVarList);

        // Assign operator that "extracts" the MBR fields from the func-expr constant into a tuple.
        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        if (probeSubTree == null) {
            // We are optimizing a selection query.
            // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
            assignSearchKeys.getInputs().add(dataSourceOp.getInputs().get(0));
            assignSearchKeys.setExecutionMode(dataSourceOp.getExecutionMode());
        } else {
            // We are optimizing a join, place the assign op top of the probe subtree.
            assignSearchKeys.getInputs().add(probeSubTree.rootRef);
        }

        boolean outputPrimaryKeysOnlyFromSIdxSearch = false;
        UnnestMapOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType,
                chosenIndex, assignSearchKeys, jobGenParams, context, outputPrimaryKeysOnlyFromSIdxSearch, retainInput,
                isIndexOnlyPlanEnabled, noFalsePositiveResultsFromSIdxSearch);

        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            ExternalDataLookupOperator externalDataAccessOp = AccessMethodUtils.createExternalDataLookupUnnestMap(
                    dataSourceOp, dataset, recordType, secondaryIndexUnnestOp, context, chosenIndex, retainInput,
                    retainNull, outputPrimaryKeysOnlyFromSIdxSearch);
            return externalDataAccessOp;
        } else {
            //            UnnestMapOperator primaryIndexUnnestOp = AccessMethodUtils.createPrimaryIndexUnnestMap(dataSourceOp,
            //                    dataset, recordType, secondaryIndexUnnestOp, context, true, retainInput, false, false, chosenIndex);

            ILogicalOperator primaryIndexUnnestOp = (AbstractLogicalOperator) AccessMethodUtils
                    .createPrimaryIndexUnnestMap(afterTopRefs, topRef, assignBeforeTopRef, dataSourceOp, dataset,
                            recordType, secondaryIndexUnnestOp, context, true, retainInput, false, false, chosenIndex,
                            analysisCtx, outputPrimaryKeysOnlyFromSIdxSearch, verificationAfterSIdxSearchRequired,
                            secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp, indexSubTree,
                            noFalsePositiveResultsFromSIdxSearch);

            if (isIndexOnlyPlanEnabled) {
                // Right now, the order of opertors is: union -> select -> assign -> unnest-map
                AbstractLogicalOperator dataSourceRefOp = (AbstractLogicalOperator) primaryIndexUnnestOp.getInputs()
                        .get(0).getValue(); // select
                dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map

                indexSubTree.dataSourceRef.setValue(dataSourceRefOp);
            } else {
                indexSubTree.dataSourceRef.setValue(primaryIndexUnnestOp);
            }

            return primaryIndexUnnestOp;
        }
    }

    @Override
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        if (optFuncExpr.getFuncExpr().getAnnotations()
                .containsKey(SkipSecondaryIndexSearchExpressionAnnotation.INSTANCE)) {
            return false;
        }
        // No additional analysis required.
        return true;
    }
}
