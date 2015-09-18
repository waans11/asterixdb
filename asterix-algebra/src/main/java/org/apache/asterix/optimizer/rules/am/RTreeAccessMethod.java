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
package org.apache.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.aql.util.FunctionUtils;
import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quintuple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * Class for helping rewrite rules to choose and apply RTree indexes.
 */
public class RTreeAccessMethod implements IAccessMethod {

    // The second boolean value means that this function can produce false positive results if this value is set to false.
    // That is, an index-search alone cannot replace SELECT condition and
    // SELECT condition needs to be applied afterwards to get the correct results.
    // In R-Tree case, depends on the parameters of the SPATIAL_INTERSECT function, it may/may not produce false positive results.
    // Thus, we need to have one more step to check whether the SPATIAL_INTERSECT generates false positive results or not.
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
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) conditionRef.getValue();

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
        boolean verificationAfterSIdxSearchRequired = false;

        // Can the chosen method generate any false positive results?
        boolean noFalsePositiveResultsFromSIdxSearch = false;

        // Check whether the given function-call can generate false positive results.
        FunctionIdentifier argFuncIdent = funcExpr.getFunctionIdentifier();
        boolean functionFound = false;
        for (int i = 0; i < funcIdents.size(); i++) {
            if (argFuncIdent == funcIdents.get(i).first) {
                functionFound = true;
                noFalsePositiveResultsFromSIdxSearch = funcIdents.get(i).second;
                if (!noFalsePositiveResultsFromSIdxSearch) {
                    break;
                }
            }
        }

        // If function-call itself is not an index-based access method, we check its arguments.
        if (!functionFound) {
            for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                ILogicalExpression argExpr = arg.getValue();
                if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    continue;
                }
                AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) argExpr;
                FunctionIdentifier argExprFuncIdent = argFuncExpr.getFunctionIdentifier();
                for (int i = 0; i < funcIdents.size(); i++) {
                    if (argExprFuncIdent == funcIdents.get(i).first) {
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

        Quintuple<Boolean, Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo = new Quintuple<Boolean, Boolean, Boolean, Boolean, Boolean>(
                isIndexOnlyPlanPossible, secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp,
                verificationAfterSIdxSearchRequired, noFalsePositiveResultsFromSIdxSearch);

        Dataset dataset = subTree.dataset;

        // Is this plan an index-only plan?
        if (isIndexOnlyPlanPossible) {
            if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                boolean indexOnlyPlancheck = AccessMethodUtils.indexOnlyPlanCheck(aboveSelectRefs, selectRef, subTree,
                        null, chosenIndex, analysisCtx, context, indexOnlyPlanInfo);

                if (!indexOnlyPlancheck) {
                    return false;
                } else {
                    isIndexOnlyPlanPossible = indexOnlyPlanInfo.first;
                    secondaryKeyFieldUsedInSelectCondition = indexOnlyPlanInfo.second;
                    secondaryKeyFieldUsedAfterSelectOp = indexOnlyPlanInfo.third;
                    verificationAfterSIdxSearchRequired = indexOnlyPlanInfo.fourth;
                    noFalsePositiveResultsFromSIdxSearch = indexOnlyPlanInfo.fifth;
                }
            } else {
                // For a index on an external dataset can't be optimized for the index-only plan.
                isIndexOnlyPlanPossible = false;
                noFalsePositiveResultsFromSIdxSearch = false;
            }
        }

        if (isIndexOnlyPlanPossible) {
            analysisCtx.setIndexOnlyPlanEnabled(true);
        } else {
            analysisCtx.setIndexOnlyPlanEnabled(false);
        }

        // R-Tree specific: if the verification after a SIdx search is required, then
        // there are false positives from a secondary index search.
        if (verificationAfterSIdxSearchRequired) {
            noFalsePositiveResultsFromSIdxSearch = false;
        }

        SelectOperator selectOp = (SelectOperator) selectRef.getValue();

        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(aboveSelectRefs, selectRef,
                selectOp.getCondition(), subTree.assignsAndUnnestsRefs, subTree, null, chosenIndex, optFuncExpr,
                analysisCtx, false, false, false, context, verificationAfterSIdxSearchRequired,
                secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp,
                noFalsePositiveResultsFromSIdxSearch);
        if (primaryIndexUnnestOp == null) {
            return false;
        }

        // Replace the datasource scan with the new plan rooted at primaryIndexUnnestMap.
        if ((!isIndexOnlyPlanPossible && !noFalsePositiveResultsFromSIdxSearch)
                || dataset.getDatasetType() == DatasetType.EXTERNAL) {
            subTree.dataSourceRef.setValue(primaryIndexUnnestOp);
        } else {
            // If an index-only plan or reducing the number of SELECT operators were applied, the topmost operator returned
            // is UNIONALL operator.
            if (primaryIndexUnnestOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                selectRef.setValue(primaryIndexUnnestOp);
            } else {
                subTree.dataSourceRef.setValue(primaryIndexUnnestOp);
            }
        }
        return true;
    }

    @Override
    public boolean applyJoinPlanTransformation(List<Mutable<ILogicalOperator>> aboveJoinRefs,
            Mutable<ILogicalOperator> joinRef, OptimizableOperatorSubTree leftSubTree,
            OptimizableOperatorSubTree rightSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, boolean isLeftOuterJoin, boolean hasGroupBy) throws AlgebricksException {

        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinRef.getValue();
        Mutable<ILogicalExpression> conditionRef = joinOp.getCondition();

        AbstractFunctionCallExpression funcExpr = null;
        FunctionIdentifier funcIdent = null;
        if (conditionRef.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            funcExpr = (AbstractFunctionCallExpression) conditionRef.getValue();
            funcIdent = funcExpr.getFunctionIdentifier();
        }

        // Determine if the index is applicable on the left or right side (if both, we prefer the right (inner) side).
        Dataset dataset = analysisCtx.indexDatasetMap.get(chosenIndex);

        // Determine probe and index subtrees based on chosen index.
        OptimizableOperatorSubTree indexSubTree = null;
        OptimizableOperatorSubTree probeSubTree = null;
        if ((rightSubTree.hasDataSourceScan() && dataset.getDatasetName().equals(rightSubTree.dataset.getDatasetName()))
                || isLeftOuterJoin) {
            indexSubTree = rightSubTree;
            probeSubTree = leftSubTree;
        } else if (!isLeftOuterJoin && leftSubTree.hasDataSourceScan()
                && dataset.getDatasetName().equals(leftSubTree.dataset.getDatasetName())) {
            indexSubTree = leftSubTree;
            probeSubTree = rightSubTree;
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

        // Check whether assign (unnest) operator exists before the join operator
        Mutable<ILogicalOperator> assignBeforeJoinOpRef = (indexSubTree.assignsAndUnnestsRefs.isEmpty()) ? null
                : indexSubTree.assignsAndUnnestsRefs.get(0);
        ILogicalOperator assignBeforeJoinOp = null;
        if (assignBeforeJoinOpRef != null) {
            assignBeforeJoinOp = assignBeforeJoinOpRef.getValue();
        }

        // index-only plan possible?
        boolean isIndexOnlyPlanPossible = false;

        // secondary key field usage in the join condition
        boolean secondaryKeyFieldUsedInJoinCondition = false;

        // secondary key field usage after the join operator
        boolean secondaryKeyFieldUsedAfterJoinOp = false;

        // For R-Tree only: whether a verification is required after the secondary index search
        boolean verificationAfterSIdxSearchRequired = false;

        // Can the chosen method generate any false positive results?
        // Currently, for the R Tree index, we also need to check the arguments
        // since the function itself can generate false positive results depends on the arguments.
        boolean noFalsePositiveResultsFromSIdxSearch = true;
        if (funcExpr != null) {
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
        }

        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(chosenIndex, analysisCtx);

        Quintuple<Boolean, Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo = new Quintuple<Boolean, Boolean, Boolean, Boolean, Boolean>(
                isIndexOnlyPlanPossible, secondaryKeyFieldUsedInJoinCondition, secondaryKeyFieldUsedAfterJoinOp,
                verificationAfterSIdxSearchRequired, noFalsePositiveResultsFromSIdxSearch);

        // If there can be any false positive results, an index-only plan is not possible
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            boolean indexOnlyPlanCheck = AccessMethodUtils.indexOnlyPlanCheck(aboveJoinRefs, joinRef, indexSubTree,
                    probeSubTree, chosenIndex, analysisCtx, context, indexOnlyPlanInfo);

            if (!indexOnlyPlanCheck) {
                return false;
            } else {
                isIndexOnlyPlanPossible = indexOnlyPlanInfo.first;
                secondaryKeyFieldUsedInJoinCondition = indexOnlyPlanInfo.second;
                secondaryKeyFieldUsedAfterJoinOp = indexOnlyPlanInfo.third;
                verificationAfterSIdxSearchRequired = indexOnlyPlanInfo.fourth;
                noFalsePositiveResultsFromSIdxSearch = indexOnlyPlanInfo.fifth;
            }
        } else {
            // We don't consider an index on an external dataset to be an index-only plan.
            isIndexOnlyPlanPossible = false;
        }

        if (isIndexOnlyPlanPossible) {
            analysisCtx.setIndexOnlyPlanEnabled(true);
        } else {
            analysisCtx.setIndexOnlyPlanEnabled(false);
        }

        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(aboveJoinRefs, joinRef, conditionRef,
                indexSubTree.assignsAndUnnestsRefs, indexSubTree, probeSubTree, chosenIndex, optFuncExpr, analysisCtx,
                true, isLeftOuterJoin, true, context, verificationAfterSIdxSearchRequired,
                secondaryKeyFieldUsedInJoinCondition, secondaryKeyFieldUsedAfterJoinOp,
                noFalsePositiveResultsFromSIdxSearch);
        if (primaryIndexUnnestOp == null) {
            return false;
        }

        if (isLeftOuterJoin && hasGroupBy) {
            //reset the null place holder variable
            AccessMethodUtils.resetLOJNullPlaceholderVariableInGroupByOp(analysisCtx, newNullPlaceHolderVar, context);
        }

        indexSubTree.dataSourceRef.setValue(primaryIndexUnnestOp);

        if (conditionRef.getValue() != null) {
            if (assignBeforeJoinOp != null) {
                // If a tryLock() on PK optimization is possible,
                // the whole plan is changed. Replace the current path with the new plan.
                if (analysisCtx.isIndexOnlyPlanEnabled() && dataset.getDatasetType() == DatasetType.INTERNAL) {
                    // Get the revised dataSourceRef operator - unnest-map (PK, record)
                    // Right now, the order of operators is: union <- select <- assign <- unnest-map (primary index look-up)
                    ILogicalOperator dataSourceRefOp = (ILogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                            .getValue(); // select
                    for (int i = 0; i < indexSubTree.assignsAndUnnestsRefs.size(); i++) {
                        if (indexSubTree.assignsAndUnnestsRefs.get(i) != null) {
                            dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                        }
                    }
                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                    indexSubTree.dataSourceRef.setValue(dataSourceRefOp);
                    // Replace the current operator with the newly created operator
                    joinRef.setValue(primaryIndexUnnestOp);
                } else if (noFalsePositiveResultsFromSIdxSearch && dataset.getDatasetType() == DatasetType.INTERNAL) {
                    // If there are no false positives, still there can be
                    // Right now, the order of operators is:
                    // union <- select <- split <- assign <- unnest-map (Pidx) <- project <- stable_sort <- unnest-map (Sidx) <- ...
                    //             or
                    // select <- assign <- unnest-map ...

                    // Case 1: we have UNION
                    if (primaryIndexUnnestOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                        // union <- select <- split <- assign <- unnest-map (Pidx) <- project <- stable_sort <- unnest-map (Sidx) <- ...
                        ILogicalOperator dataSourceRefOp = (ILogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                                .getValue(); // select
                        dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // split

                        for (int i = 0; i < indexSubTree.assignsAndUnnestsRefs.size(); i++) {
                            if (indexSubTree.assignsAndUnnestsRefs.get(i) != null) {
                                dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                            }
                        }
                        dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                        indexSubTree.dataSourceRef.setValue(dataSourceRefOp);
                    } else {
                        // select <- assign? <- unnest-map ...
                        ILogicalOperator dataSourceRefOp = (ILogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                                .getValue(); // assign
                        // Do we have more ASSIGNs?
                        for (int i = 1; i < indexSubTree.assignsAndUnnestsRefs.size(); i++) {
                            if (indexSubTree.assignsAndUnnestsRefs.get(i) != null) {
                                dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                            }
                        }
                        dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                        indexSubTree.dataSourceRef.setValue(dataSourceRefOp);
                    }

                    // Replace the current operator with the newly created operator
                    joinRef.setValue(primaryIndexUnnestOp);
                }
                //                } else {
                // Index-only optimization and reducing the number of SELECT optimization are not possible.
                // Right now, the order of operators is: select <- assign <- unnest-map (primary index look-up)
                //                    joinOp.getInputs().clear();
                //                    indexSubTree.dataSourceRef.setValue(primaryIndexUnnestOp);
                //                    joinOp.getInputs().add(new MutableObject<ILogicalOperator>(assignBeforeJoinOp));
                //                }
            }

            if (!isIndexOnlyPlanPossible && !noFalsePositiveResultsFromSIdxSearch) {
                SelectOperator topSelect = new SelectOperator(conditionRef, isLeftOuterJoin, newNullPlaceHolderVar);
                topSelect.getInputs().add(indexSubTree.rootRef);
                topSelect.setExecutionMode(ExecutionMode.LOCAL);
                context.computeAndSetTypeEnvironmentForOperator(topSelect);
                joinRef.setValue(topSelect);
            }
        } else {
            // Replace the original join with the new subtree rooted at the select op.
            if (primaryIndexUnnestOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                joinRef.setValue(primaryIndexUnnestOp);
            } else {
                joinRef.setValue(indexSubTree.rootRef.getValue());
            }
        }

        // Change join into a select with the same condition.
        //        SelectOperator topSelect = new SelectOperator(joinOp.getCondition(), isLeftOuterJoin, newNullPlaceHolderVar);
        //        topSelect.getInputs().add(indexSubTree.rootRef);
        //        topSelect.setExecutionMode(ExecutionMode.LOCAL);
        //        context.computeAndSetTypeEnvironmentForOperator(topSelect);
        // Replace the original join with the new subtree rooted at the select op.
        //        joinRef.setValue(topSelect);
        return true;
    }

    private ILogicalOperator createSecondaryToPrimaryPlan(List<Mutable<ILogicalOperator>> afterTopRefs,
            Mutable<ILogicalOperator> topRef, Mutable<ILogicalExpression> conditionRef,
            List<Mutable<ILogicalOperator>> assignBeforeTopRefs, OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, IOptimizableFuncExpr optFuncExpr,
            AccessMethodAnalysisContext analysisCtx, boolean retainInput, boolean retainNull,
            boolean requiresBroadcast, IOptimizationContext context, boolean verificationAfterSIdxSearchRequired,
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

        // Set the LIMIT push-down if it is possible.
        long limitNumberOfResult = -1;

        if (noFalsePositiveResultsFromSIdxSearch) {
            limitNumberOfResult = analysisCtx.getLimitNumberOfResult();
        }

        // we made sure indexSubTree has datasource scan
        AbstractDataSourceOperator dataSourceOp = (AbstractDataSourceOperator) indexSubTree.dataSourceRef.getValue();
        RTreeJobGenParams jobGenParams = new RTreeJobGenParams(chosenIndex.getIndexName(), IndexType.RTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, retainNull, requiresBroadcast,
                isIndexOnlyPlanEnabled || noFalsePositiveResultsFromSIdxSearch, limitNumberOfResult);
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
            OperatorPropertiesUtil.typeOpRec(probeSubTree.rootRef, context);
            context.computeAndSetTypeEnvironmentForOperator(assignSearchKeys);
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
            ILogicalOperator primaryIndexUnnestOp;

            // If only reducing the number of SELECT optimization is possible,
            // then we need to keep the input to the primary index look-up since there is a variable that keeps
            // the result of tryLock on a PK during a secondary index search. The SPLIT operator needs to see this variable.
            if (noFalsePositiveResultsFromSIdxSearch && !isIndexOnlyPlanEnabled) {
                primaryIndexUnnestOp = (AbstractLogicalOperator) AccessMethodUtils.createPrimaryIndexUnnestMap(
                        afterTopRefs, topRef, conditionRef, assignBeforeTopRefs, dataSourceOp, dataset, recordType,
                        secondaryIndexUnnestOp, context, true, true, retainNull, false, chosenIndex, analysisCtx,
                        outputPrimaryKeysOnlyFromSIdxSearch, verificationAfterSIdxSearchRequired,
                        secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp, indexSubTree,
                        noFalsePositiveResultsFromSIdxSearch);
            } else {
                primaryIndexUnnestOp = (AbstractLogicalOperator) AccessMethodUtils.createPrimaryIndexUnnestMap(
                        afterTopRefs, topRef, conditionRef, assignBeforeTopRefs, dataSourceOp, dataset, recordType,
                        secondaryIndexUnnestOp, context, true, retainInput, retainNull, false, chosenIndex,
                        analysisCtx, outputPrimaryKeysOnlyFromSIdxSearch, verificationAfterSIdxSearchRequired,
                        secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp, indexSubTree,
                        noFalsePositiveResultsFromSIdxSearch);
            }

            if (isIndexOnlyPlanEnabled) {
                // Right now, the order of opertors is: union -> select -> assign -> unnest-map
                AbstractLogicalOperator dataSourceRefOp = (AbstractLogicalOperator) primaryIndexUnnestOp.getInputs()
                        .get(0).getValue(); // select
                if (indexSubTree.assignsAndUnnests != null) {
                    for (int i = 0; i < indexSubTree.assignsAndUnnests.size(); i++) {
                        dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                    }
                }
                // dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map

                indexSubTree.dataSourceRef.setValue(dataSourceRefOp);
            } else if (noFalsePositiveResultsFromSIdxSearch) {
                // For this case, we still have an optimization that can reduce the number of SELECT operations.
                if (primaryIndexUnnestOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                    // Right now, the order of opertors is: union -> select -> split -> assign? -> unnest-map
                    AbstractLogicalOperator dataSourceRefOp = (AbstractLogicalOperator) primaryIndexUnnestOp
                            .getInputs().get(0).getValue(); // select
                    dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // split

                    if (indexSubTree.assignsAndUnnests != null) {
                        for (int i = 0; i < indexSubTree.assignsAndUnnests.size(); i++) {
                            dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                        }
                    }
                    dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map

                    indexSubTree.dataSourceRef.setValue(dataSourceRefOp);

                } else {
                    // No optimization is possible - then the top operator is unnest-map.
                    indexSubTree.dataSourceRef.setValue(primaryIndexUnnestOp);
                }
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
