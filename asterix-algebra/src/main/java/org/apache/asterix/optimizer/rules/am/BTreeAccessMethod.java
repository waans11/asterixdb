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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.aql.util.FunctionUtils;
import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.IndexedNLJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;

/**
 * Class for helping rewrite rules to choose and apply BTree indexes.
 */
public class BTreeAccessMethod implements IAccessMethod {

    // Describes whether a search predicate is an open/closed interval.
    private enum LimitType {
        LOW_INCLUSIVE,
        LOW_EXCLUSIVE,
        HIGH_INCLUSIVE,
        HIGH_EXCLUSIVE,
        EQUAL
    }

    // TODO: There is some redundancy here, since these are listed in AlgebricksBuiltinFunctions as well.
    //
    // Second boolean value means that this function can produce false positive results if it is set to false.
    // That is, an index-search alone cannot replace SELECT condition and
    // SELECT condition needs to be applied after the index-search to get the correct results.
    private static List<Pair<FunctionIdentifier, Boolean>> funcIdents = new ArrayList<Pair<FunctionIdentifier, Boolean>>();
    static {
        funcIdents.add(new Pair<FunctionIdentifier, Boolean>(AlgebricksBuiltinFunctions.EQ, true));
        funcIdents.add(new Pair<FunctionIdentifier, Boolean>(AlgebricksBuiltinFunctions.LE, true));
        funcIdents.add(new Pair<FunctionIdentifier, Boolean>(AlgebricksBuiltinFunctions.GE, true));
        funcIdents.add(new Pair<FunctionIdentifier, Boolean>(AlgebricksBuiltinFunctions.LT, true));
        funcIdents.add(new Pair<FunctionIdentifier, Boolean>(AlgebricksBuiltinFunctions.GT, true));
    }

    public static BTreeAccessMethod INSTANCE = new BTreeAccessMethod();

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
        return false;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        return true;
    }

    @Override
    public boolean applySelectPlanTransformation(List<Mutable<ILogicalOperator>> aboveSelectRefs,
            Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context) throws AlgebricksException {
        SelectOperator select = (SelectOperator) selectRef.getValue();
        Mutable<ILogicalExpression> conditionRef = select.getCondition();
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) conditionRef.getValue();

        // Check whether assign (unnest) operator exists before the select operator
        Mutable<ILogicalOperator> assignBeforeSelectOpRef = (subTree.assignsAndUnnestsRefs.isEmpty()) ? null
                : subTree.assignsAndUnnestsRefs.get(0);
        ILogicalOperator assignBeforeSelectOp = null;
        if (assignBeforeSelectOpRef != null) {
            assignBeforeSelectOp = assignBeforeSelectOpRef.getValue();
        }

        Dataset dataset = subTree.dataset;

        // index-only plan possible?
        boolean isIndexOnlyPlan = false;

        // secondary key field usage in the select condition
        boolean secondaryKeyFieldUsedInSelectOpCondition = false;

        // secondary key field usage after the select operator
        boolean secondaryKeyFieldUsedAfterSelectOp = false;

        // Whether a verification is required after the secondary index search
        boolean verificationAfterSIdxSearchRequired = false;

        // Can the chosen method generate any false positive results?
        // Currently, for the B+ Tree index, there cannot be any false positive results.
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

        Quintuple<Boolean, Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo = new Quintuple<Boolean, Boolean, Boolean, Boolean, Boolean>(
                isIndexOnlyPlan, secondaryKeyFieldUsedInSelectOpCondition, secondaryKeyFieldUsedAfterSelectOp,
                verificationAfterSIdxSearchRequired, noFalsePositiveResultsFromSIdxSearch);

        // If there can be any false positive results, an index-only plan is not possible
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            boolean indexOnlyPlancheck = AccessMethodUtils.indexOnlyPlanCheck(aboveSelectRefs, selectRef, subTree,
                    null, chosenIndex, analysisCtx, context, indexOnlyPlanInfo);

            if (!indexOnlyPlancheck) {
                return false;
            } else {
                isIndexOnlyPlan = indexOnlyPlanInfo.first;
                secondaryKeyFieldUsedInSelectOpCondition = indexOnlyPlanInfo.second;
                secondaryKeyFieldUsedAfterSelectOp = indexOnlyPlanInfo.third;
                verificationAfterSIdxSearchRequired = indexOnlyPlanInfo.fourth;
                noFalsePositiveResultsFromSIdxSearch = indexOnlyPlanInfo.fifth;
            }
        } else {
            // We don't consider an index on an external dataset to be an index-only plan.
            isIndexOnlyPlan = false;
        }

        if (isIndexOnlyPlan) {
            analysisCtx.setIndexOnlyPlanEnabled(true);
        } else {
            analysisCtx.setIndexOnlyPlanEnabled(false);

        }

        // Transform the current path to the path that is utilizing the corresponding indexes
        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(aboveSelectRefs, selectRef, conditionRef,
                subTree.assignsAndUnnestsRefs, subTree, null, chosenIndex, analysisCtx, false, false, false, context,
                verificationAfterSIdxSearchRequired, secondaryKeyFieldUsedInSelectOpCondition,
                secondaryKeyFieldUsedAfterSelectOp, noFalsePositiveResultsFromSIdxSearch);
        if (primaryIndexUnnestOp == null) {
            return false;
        }

        // Generate new path using the new condition.
        if (conditionRef.getValue() != null) {
            if (assignBeforeSelectOp != null) {
                // If a tryLock() on PK optimization is possible,
                // the whole plan is changed. replace the current path with the new plan.
                if (analysisCtx.isIndexOnlyPlanEnabled() && dataset.getDatasetType() == DatasetType.INTERNAL) {
                    // Get the revised dataSourceRef operator - unnest-map (PK, record)
                    // Right now, the order of operators is: union <- select <- assign <- unnest-map (primary index look-up)
                    ILogicalOperator dataSourceRefOp = (ILogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                            .getValue(); // select
                    //                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                    for (int i = 0; i < subTree.assignsAndUnnestsRefs.size(); i++) {
                        if (subTree.assignsAndUnnestsRefs.get(i) != null) {
                            dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                        }
                    }

                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                    subTree.dataSourceRef.setValue(dataSourceRefOp);
                    // Replace the current operator with the newly created operator
                    selectRef.setValue(primaryIndexUnnestOp);
                } else if (noFalsePositiveResultsFromSIdxSearch && dataset.getDatasetType() == DatasetType.INTERNAL) {
                    // If there are no false positives, still there can be
                    // Right now, the order of operators is: union <- select <- split <- assign <- unnest-map (primary index look-up) <- [A]
                    //                                       [A] <- stream_project <- stable_sort <- unnest-map (secondary index look-up) <- ...
                    //             or
                    //                                       select <- assign <- unnest-map ...

                    // Case 1: we have UNION
                    if (primaryIndexUnnestOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                        ILogicalOperator dataSourceRefOp = (ILogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                                .getValue(); // select
                        dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // split

                        for (int i = 0; i < subTree.assignsAndUnnestsRefs.size(); i++) {
                            if (subTree.assignsAndUnnestsRefs.get(i) != null) {
                                dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                            }
                        }
                        dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                        subTree.dataSourceRef.setValue(dataSourceRefOp);
                    } else {
                        ILogicalOperator dataSourceRefOp = (ILogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                                .getValue(); // assign
                        // Do we have more ASSIGNs?
                        for (int i = 1; i < subTree.assignsAndUnnestsRefs.size(); i++) {
                            if (subTree.assignsAndUnnestsRefs.get(i) != null) {
                                dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                            }
                        }
                        dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                        subTree.dataSourceRef.setValue(dataSourceRefOp);
                    }

                    // Replace the current operator with the newly created operator
                    selectRef.setValue(primaryIndexUnnestOp);
                } else {
                    // Index-only optimization and reducing the number of SELECT optimization are not possible.
                    // Right now, the order of operators is: select <- assign <- unnest-map (primary index look-up)
                    select.getInputs().clear();
                    subTree.dataSourceRef.setValue(primaryIndexUnnestOp);
                    select.getInputs().add(new MutableObject<ILogicalOperator>(assignBeforeSelectOp));
                }
            } else {
                // If a tryLock() on PK is possible, we don't need to use select operator.
                if (analysisCtx.isIndexOnlyPlanEnabled()) {
                    ILogicalOperator dataSourceRefOp = (ILogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                            .getValue(); // select
                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                    subTree.dataSourceRef.setValue(dataSourceRefOp);
                    selectRef.setValue(primaryIndexUnnestOp);
                } else if (noFalsePositiveResultsFromSIdxSearch) {
                    // If there are no false positives, still there can be an optimization - reducing the number of SELECT operations
                    // Right now, the order of operators is: union <- select <- split <- unnest-map (primary index look-up) <- [A]
                    //                                       [A] <- stream_project <- stable_sort <- unnest-map (secondary index look-up) <- ...
                    ILogicalOperator dataSourceRefOp = (ILogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                            .getValue(); // select
                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // split
                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                    subTree.dataSourceRef.setValue(dataSourceRefOp);
                    // Replace the current operator with the newly created operator
                    selectRef.setValue(primaryIndexUnnestOp);
                } else {
                    select.getInputs().clear();
                    select.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestOp));
                }
            }
        } else {
            ((AbstractLogicalOperator) primaryIndexUnnestOp).setExecutionMode(ExecutionMode.PARTITIONED);
            if (assignBeforeSelectOp != null) {
                subTree.dataSourceRef.setValue(primaryIndexUnnestOp);
                selectRef.setValue(assignBeforeSelectOp);
            } else {
                selectRef.setValue(primaryIndexUnnestOp);
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
        System.out.println("subTree.assignsAndUnnestsRefs.size() " + indexSubTree.assignsAndUnnestsRefs.size());
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
        // Currently, for the B+ Tree index, there cannot be any false positive results except the composite index case.
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
                indexSubTree.assignsAndUnnestsRefs, indexSubTree, probeSubTree, chosenIndex, analysisCtx, true,
                isLeftOuterJoin, true, context, verificationAfterSIdxSearchRequired,
                secondaryKeyFieldUsedInJoinCondition, secondaryKeyFieldUsedAfterJoinOp,
                noFalsePositiveResultsFromSIdxSearch);
        if (primaryIndexUnnestOp == null) {
            return false;
        }

        if (isLeftOuterJoin && hasGroupBy) {
            //reset the null place holder variable
            AccessMethodUtils.resetLOJNullPlaceholderVariableInGroupByOp(analysisCtx, newNullPlaceHolderVar, context);
        }

        // If there are conditions left, add a new select operator on top.
        indexSubTree.dataSourceRef.setValue(primaryIndexUnnestOp);
        if (conditionRef.getValue() != null) {
            if (assignBeforeJoinOp != null) {
                // If a tryLock() on PK optimization is possible,
                // the whole plan is changed. replace the current path with the new plan.
                if (analysisCtx.isIndexOnlyPlanEnabled()) {
                    // Get the revised dataSourceRef operator - unnest-map (PK, record)
                    // Right now, the order of operators is: union <- select <- assign <- unnest-map (primary index look-up)
                    ILogicalOperator dataSourceRefOp = (ILogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                            .getValue(); // select
                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                    indexSubTree.dataSourceRef.setValue(dataSourceRefOp);
                    // Replace the current operator with the newly created operator
                    joinRef.setValue(primaryIndexUnnestOp);
                } else if (noFalsePositiveResultsFromSIdxSearch && dataset.getDatasetType() == DatasetType.INTERNAL) {
                    // If there are no false positives, still there can be
                    // Right now, the order of operators is: union <- select <- split <- assign <- unnest-map (primary index look-up) <- [A]
                    //                                       [A] <- stream_project <- stable_sort <- unnest-map (secondary index look-up) <- ...
                    //             or
                    //                                       select <- assign <- unnest-map ...

                    // Case 1: we have UNION
                    if (primaryIndexUnnestOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
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
                } else {
                    // Index-only optimization and reducing the number of SELECT optimization are not possible.
                    // Right now, the order of operators is: select <- assign <- unnest-map (primary index look-up)
                    joinOp.getInputs().clear();
                    indexSubTree.dataSourceRef.setValue(primaryIndexUnnestOp);
                    joinOp.getInputs().add(new MutableObject<ILogicalOperator>(assignBeforeJoinOp));
                }
            }

            if (!isIndexOnlyPlanPossible && !noFalsePositiveResultsFromSIdxSearch) {
                SelectOperator topSelect = new SelectOperator(conditionRef, isLeftOuterJoin, newNullPlaceHolderVar);
                topSelect.getInputs().add(indexSubTree.rootRef);
                topSelect.setExecutionMode(ExecutionMode.LOCAL);
                context.computeAndSetTypeEnvironmentForOperator(topSelect);
                joinRef.setValue(topSelect);
            }
            //            // Replace the original join with the new subtree rooted at the select op.
        } else {
            if (primaryIndexUnnestOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                joinRef.setValue(primaryIndexUnnestOp);
            } else {
                joinRef.setValue(indexSubTree.rootRef.getValue());
            }
        }
        return true;
    }

    /**
     * Create a secondary index lookup optimization.
     * Case A) index-only select plan:
     * ......... union <- select <- assign? <- unnest-map (primary index look-up) <- split <- unnest-map (secondary index look-up) <- assign? <- datasource-scan
     * ............... <- ....................................................... <- split
     * Case B) reducing the number of select plan:
     * ......... union <- select <- split <- assign? <- unnest-map (primary index look-up) <- stable_sort <- unnest-map (secondary index look-up) <- assign? <- datasource-scan
     * ............... <- ...... <- split
     * Case C) only secondary look-up optimization
     * select <- assign? <- unnest-map (primary index look-up) <- stable_sort <- unnest-map (secondary index look-up) <- assign? <- datasource-scan
     * <-
     */
    private ILogicalOperator createSecondaryToPrimaryPlan(List<Mutable<ILogicalOperator>> aboveTopOpRefs,
            Mutable<ILogicalOperator> opRef, Mutable<ILogicalExpression> conditionRef,
            List<Mutable<ILogicalOperator>> assignBeforeTheOpRefs, OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            boolean retainInput, boolean retainNull, boolean requiresBroadcast, IOptimizationContext context,
            boolean verificationAfterSIdxSearchRequired, boolean secondaryKeyFieldUsedInSelectCondition,
            boolean secondaryKeyFieldUsedAfterSelectOp, boolean noFalsePositiveResultsFromSIdxSearch)
            throws AlgebricksException {

        Dataset dataset = indexSubTree.dataset;
        ARecordType recordType = indexSubTree.recordType;
        // we made sure indexSubTree has datasource scan
        AbstractDataSourceOperator dataSourceOp = (AbstractDataSourceOperator) indexSubTree.dataSourceRef.getValue();
        List<Pair<Integer, Integer>> exprAndVarList = analysisCtx.indexExprsAndVars.get(chosenIndex);
        List<IOptimizableFuncExpr> matchedFuncExprs = analysisCtx.matchedFuncExprs;
        int numSecondaryKeys = analysisCtx.indexNumMatchedKeys.get(chosenIndex);
        boolean isIndexOnlyPlanEnabled = analysisCtx.isIndexOnlyPlanEnabled();

        // Set the LIMIT push-down if it is possible.
        long limitNumberOfResult = -1;

        if (noFalsePositiveResultsFromSIdxSearch) {
            limitNumberOfResult = analysisCtx.getLimitNumberOfResult();
        }

        // List of function expressions that will be replaced by the secondary-index search.
        // These func exprs will be removed from the select condition at the very end of this method.
        Set<ILogicalExpression> replacedFuncExprs = new HashSet<ILogicalExpression>();

        // Info on high and low keys for the BTree search predicate.
        ILogicalExpression[] lowKeyExprs = new ILogicalExpression[numSecondaryKeys];
        ILogicalExpression[] highKeyExprs = new ILogicalExpression[numSecondaryKeys];
        LimitType[] lowKeyLimits = new LimitType[numSecondaryKeys];
        LimitType[] highKeyLimits = new LimitType[numSecondaryKeys];
        boolean[] lowKeyInclusive = new boolean[numSecondaryKeys];
        boolean[] highKeyInclusive = new boolean[numSecondaryKeys];

        // TODO: For now we don't do any sophisticated analysis of the func exprs to come up with "the best" range predicate.
        // If we can't figure out how to integrate a certain funcExpr into the current predicate, we just bail by setting this flag.
        boolean couldntFigureOut = false;
        boolean doneWithExprs = false;
        boolean isEqCondition = false;
        // TODO: For now don't consider prefix searches.
        BitSet setLowKeys = new BitSet(numSecondaryKeys);
        BitSet setHighKeys = new BitSet(numSecondaryKeys);
        // Go through the func exprs listed as optimizable by the chosen index,
        // and formulate a range predicate on the secondary-index keys.

        // checks whether a type casting happened from a real (FLOAT, DOUBLE) value to an INT value
        // since we have a round issues when dealing with LT(<) OR GT(>) operator.
        //        boolean realTypeConvertedToIntegerType = false;

        for (Pair<Integer, Integer> exprIndex : exprAndVarList) {
            // Position of the field of matchedFuncExprs.get(exprIndex) in the chosen index's indexed exprs.
            IOptimizableFuncExpr optFuncExpr = matchedFuncExprs.get(exprIndex.first);
            int keyPos = indexOf(optFuncExpr.getFieldName(0), chosenIndex.getKeyFieldNames());
            if (keyPos < 0) {
                if (optFuncExpr.getNumLogicalVars() > 1) {
                    // If we are optimizing a join, the matching field may be the second field name.
                    keyPos = indexOf(optFuncExpr.getFieldName(1), chosenIndex.getKeyFieldNames());
                }
            }
            if (keyPos < 0) {
                throw new AlgebricksException(
                        "Could not match optimizable function expression to any index field name.");
            }

            LimitType limit = getLimitType(optFuncExpr, probeSubTree);

            // Two values will be returned only when we are creating an EQ search predicate with a FLOAT or a DOUBLE constant
            // on an INT index.
            Pair<ILogicalExpression, ILogicalExpression> returnedSearchKeyExpr = AccessMethodUtils.createSearchKeyExpr(
                    optFuncExpr, indexSubTree, probeSubTree);
            ILogicalExpression searchKeyExpr = returnedSearchKeyExpr.first;
            ILogicalExpression searchKeyExpr2 = null;

            // the given search predicate = EQ and we have two type-casted values from FLOAT or DOUBLE to INT constant.
            if (limit == LimitType.EQUAL && returnedSearchKeyExpr.second != null) {
                searchKeyExpr2 = returnedSearchKeyExpr.second;
            }

            //            ILogicalExpression searchKeyExpr = AccessMethodUtils.createSearchKeyExpr(
            //                    optFuncExpr, indexSubTree, probeSubTree);

            // If a DOUBLE or FLOAT constant is converted to an INT type value,
            // we need to check a corner case where two real values are located between an INT value.
            // For example, for the following query,
            //
            // for $emp in dataset empDataset
            // where $emp.age > double("2.3") and $emp.age < double("3.3")
            // return $emp.id;
            //
            // It should generate a result if there is a tuple that satisfies the condition, which is 3,
            // however, it does not generate the desired result since finding candidates
            // fail after truncating the fraction part (there is no INT whose value is greater than 2 and less than 3.)
            //
            // Therefore, we convert LT(<) to LE(<=) and GT(>) to GE(>=) to find candidates.
            // This does not change the result of an actual comparison since this conversion is only applied
            // for finding candidates from an index.
            //
            //            if (realTypeConvertedToIntegerType) {
            //                if (limit == LimitType.HIGH_EXCLUSIVE) {
            //                    limit = LimitType.HIGH_INCLUSIVE;
            //                } else if (limit == LimitType.LOW_EXCLUSIVE) {
            //                    limit = LimitType.LOW_INCLUSIVE;
            //                }
            //            }

            switch (limit) {
                case EQUAL: {
                    if (lowKeyLimits[keyPos] == null && highKeyLimits[keyPos] == null) {
                        lowKeyLimits[keyPos] = highKeyLimits[keyPos] = limit;
                        lowKeyInclusive[keyPos] = highKeyInclusive[keyPos] = true;
                        if (searchKeyExpr2 == null) {
                            lowKeyExprs[keyPos] = highKeyExprs[keyPos] = searchKeyExpr;
                        } else {
                            // We have two type-casted FLOAT or DOUBLE values to be fed into an INT index
                            lowKeyExprs[keyPos] = searchKeyExpr;
                            highKeyExprs[keyPos] = searchKeyExpr2;
                        }
                        setLowKeys.set(keyPos);
                        setHighKeys.set(keyPos);
                        isEqCondition = true;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (lowKeyLimits[keyPos] == limit && lowKeyInclusive[keyPos] == true
                                && lowKeyExprs[keyPos].equals(searchKeyExpr) && highKeyLimits[keyPos] == limit
                                && highKeyInclusive[keyPos] == true && highKeyExprs[keyPos].equals(searchKeyExpr)) {
                            isEqCondition = true;
                            break;
                        }
                        couldntFigureOut = true;
                    }
                    // TODO: For now don't consider prefix searches.
                    // If high and low keys are set, we exit for now.
                    if (setLowKeys.cardinality() == numSecondaryKeys && setHighKeys.cardinality() == numSecondaryKeys) {
                        doneWithExprs = true;
                    }
                    break;
                }
                case HIGH_EXCLUSIVE: {
                    if (highKeyLimits[keyPos] == null || (highKeyLimits[keyPos] != null && highKeyInclusive[keyPos])) {
                        highKeyLimits[keyPos] = limit;
                        highKeyExprs[keyPos] = searchKeyExpr;
                        highKeyInclusive[keyPos] = false;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (highKeyLimits[keyPos] == limit && highKeyInclusive[keyPos] == false
                                && highKeyExprs[keyPos].equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case HIGH_INCLUSIVE: {
                    if (highKeyLimits[keyPos] == null) {
                        highKeyLimits[keyPos] = limit;
                        highKeyExprs[keyPos] = searchKeyExpr;
                        highKeyInclusive[keyPos] = true;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (highKeyLimits[keyPos] == limit && highKeyInclusive[keyPos] == true
                                && highKeyExprs[keyPos].equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case LOW_EXCLUSIVE: {
                    if (lowKeyLimits[keyPos] == null || (lowKeyLimits[keyPos] != null && lowKeyInclusive[keyPos])) {
                        lowKeyLimits[keyPos] = limit;
                        lowKeyExprs[keyPos] = searchKeyExpr;
                        lowKeyInclusive[keyPos] = false;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (lowKeyLimits[keyPos] == limit && lowKeyInclusive[keyPos] == false
                                && lowKeyExprs[keyPos].equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case LOW_INCLUSIVE: {
                    if (lowKeyLimits[keyPos] == null) {
                        lowKeyLimits[keyPos] = limit;
                        lowKeyExprs[keyPos] = searchKeyExpr;
                        lowKeyInclusive[keyPos] = true;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (lowKeyLimits[keyPos] == limit && lowKeyInclusive[keyPos] == true
                                && lowKeyExprs[keyPos].equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
            if (!couldntFigureOut) {
                // Remember to remove this funcExpr later.
                replacedFuncExprs.add(matchedFuncExprs.get(exprIndex.first).getFuncExpr());
            }
            if (doneWithExprs) {
                break;
            }
        }
        if (couldntFigureOut) {
            return null;
        }

        // If the select condition contains mixed open/closed intervals on multiple keys, then we make all intervals closed to obtain a superset of answers and leave the original selection in place.
        boolean primaryIndexPostProccessingIsNeeded = false;
        for (int i = 1; i < numSecondaryKeys; ++i) {
            if (lowKeyInclusive[i] != lowKeyInclusive[0]) {
                Arrays.fill(lowKeyInclusive, true);
                primaryIndexPostProccessingIsNeeded = true;
                break;
            }
        }
        for (int i = 1; i < numSecondaryKeys; ++i) {
            if (highKeyInclusive[i] != highKeyInclusive[0]) {
                Arrays.fill(highKeyInclusive, true);
                primaryIndexPostProccessingIsNeeded = true;
                break;
            }
        }

        // determine cases when prefix search could be applied
        for (int i = 1; i < lowKeyExprs.length; i++) {
            if (lowKeyLimits[0] == null && lowKeyLimits[i] != null || lowKeyLimits[0] != null
                    && lowKeyLimits[i] == null || highKeyLimits[0] == null && highKeyLimits[i] != null
                    || highKeyLimits[0] != null && highKeyLimits[i] == null) {
                numSecondaryKeys--;
                primaryIndexPostProccessingIsNeeded = true;
            }
        }
        if (lowKeyLimits[0] == null) {
            lowKeyInclusive[0] = true;
        }
        if (highKeyLimits[0] == null) {
            highKeyInclusive[0] = true;
        }

        // Here we generate vars and funcs for assigning the secondary-index keys to be fed into the secondary-index search.
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        // List of variables and expressions for the assign.
        ArrayList<LogicalVariable> assignKeyVarList = new ArrayList<LogicalVariable>();
        ArrayList<Mutable<ILogicalExpression>> assignKeyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        int numLowKeys = createKeyVarsAndExprs(numSecondaryKeys, lowKeyLimits, lowKeyExprs, assignKeyVarList,
                assignKeyExprList, keyVarList, context);
        int numHighKeys = createKeyVarsAndExprs(numSecondaryKeys, highKeyLimits, highKeyExprs, assignKeyVarList,
                assignKeyExprList, keyVarList, context);

        BTreeJobGenParams jobGenParams = new BTreeJobGenParams(chosenIndex.getIndexName(), IndexType.BTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, retainNull, requiresBroadcast,
                isIndexOnlyPlanEnabled || noFalsePositiveResultsFromSIdxSearch, limitNumberOfResult);
        jobGenParams.setLowKeyInclusive(lowKeyInclusive[0]);
        jobGenParams.setHighKeyInclusive(highKeyInclusive[0]);
        jobGenParams.setIsEqCondition(isEqCondition);
        jobGenParams.setLowKeyVarList(keyVarList, 0, numLowKeys);
        jobGenParams.setHighKeyVarList(keyVarList, numLowKeys, numHighKeys);

        ILogicalOperator inputOp = null;
        if (!assignKeyVarList.isEmpty()) {
            // Assign operator that sets the constant secondary-index search-key fields if necessary.
            AssignOperator assignConstantSearchKeys = new AssignOperator(assignKeyVarList, assignKeyExprList);
            // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
            assignConstantSearchKeys.getInputs().add(dataSourceOp.getInputs().get(0));
            assignConstantSearchKeys.setExecutionMode(dataSourceOp.getExecutionMode());
            inputOp = assignConstantSearchKeys;
        } else {
            // All index search keys are variables.
            inputOp = probeSubTree.root;
        }

        // Create an unnest-map for the secondary index search
        // The result: SK, PK, [Optional: The result of a Trylock on PK]
        boolean outputPrimaryKeysOnlyFromSIdxSearch = false;
        UnnestMapOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType,
                chosenIndex, inputOp, jobGenParams, context, outputPrimaryKeysOnlyFromSIdxSearch, retainInput,
                isIndexOnlyPlanEnabled, noFalsePositiveResultsFromSIdxSearch);

        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        UnnestMapOperator primaryIndexUnnestOp = null;
        AbstractLogicalOperator tmpPrimaryIndexUnnestOp = null;

        boolean isPrimaryIndex = chosenIndex.isPrimaryIndex();
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            // External dataset
            ExternalDataLookupOperator externalDataAccessOp = AccessMethodUtils.createExternalDataLookupUnnestMap(
                    dataSourceOp, dataset, recordType, secondaryIndexUnnestOp, context, chosenIndex, retainInput,
                    retainNull, outputPrimaryKeysOnlyFromSIdxSearch);
            indexSubTree.dataSourceRef.setValue(externalDataAccessOp);
            return externalDataAccessOp;
        } else if (!isPrimaryIndex) {
            if (noFalsePositiveResultsFromSIdxSearch && !isIndexOnlyPlanEnabled) {
                tmpPrimaryIndexUnnestOp = (AbstractLogicalOperator) AccessMethodUtils.createPrimaryIndexUnnestMap(
                        aboveTopOpRefs, opRef, conditionRef, assignBeforeTheOpRefs, dataSourceOp, dataset, recordType,
                        secondaryIndexUnnestOp, context, true, true, retainNull, false, chosenIndex, analysisCtx,
                        outputPrimaryKeysOnlyFromSIdxSearch, verificationAfterSIdxSearchRequired,
                        secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp, indexSubTree,
                        noFalsePositiveResultsFromSIdxSearch);
            } else {
                // pass retainInput variable's value
                tmpPrimaryIndexUnnestOp = (AbstractLogicalOperator) AccessMethodUtils.createPrimaryIndexUnnestMap(
                        aboveTopOpRefs, opRef, conditionRef, assignBeforeTheOpRefs, dataSourceOp, dataset, recordType,
                        secondaryIndexUnnestOp, context, true, retainInput, retainNull, false, chosenIndex,
                        analysisCtx, outputPrimaryKeysOnlyFromSIdxSearch, verificationAfterSIdxSearchRequired,
                        secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp, indexSubTree,
                        noFalsePositiveResultsFromSIdxSearch);
            }

            // Replace the datasource scan with the new plan rooted at
            // Get dataSourceRef operator - unnest-map (PK, record)
            if (isIndexOnlyPlanEnabled) {
                // Right now, the order of opertors is: union -> select -> assign -> unnest-map
                AbstractLogicalOperator dataSourceRefOp = (AbstractLogicalOperator) tmpPrimaryIndexUnnestOp.getInputs()
                        .get(0).getValue(); // select
                dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map

                //                indexSubTree.dataSourceRef.setValue(tmpPrimaryIndexUnnestOp);
                indexSubTree.dataSourceRef.setValue(dataSourceRefOp);
            } else if (noFalsePositiveResultsFromSIdxSearch) {
                // Right now, the order of opertors is: union -> select -> split -> assign -> unnest-map
                AbstractLogicalOperator dataSourceRefOp = (AbstractLogicalOperator) tmpPrimaryIndexUnnestOp.getInputs()
                        .get(0).getValue(); // select
                dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // split
                dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
            } else {
                indexSubTree.dataSourceRef.setValue(tmpPrimaryIndexUnnestOp);
            }
        } else {
            List<Object> primaryIndexOutputTypes = new ArrayList<Object>();
            try {
                AccessMethodUtils.appendPrimaryIndexTypes(dataset, recordType, primaryIndexOutputTypes);
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
            List<LogicalVariable> scanVariables = dataSourceOp.getVariables();
            primaryIndexUnnestOp = new UnnestMapOperator(scanVariables, secondaryIndexUnnestOp.getExpressionRef(),
                    primaryIndexOutputTypes, retainInput);
            primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));

            if (!primaryIndexPostProccessingIsNeeded) {
                List<Mutable<ILogicalExpression>> remainingFuncExprs = new ArrayList<Mutable<ILogicalExpression>>();
                getNewConditionExprs(conditionRef, replacedFuncExprs, remainingFuncExprs);
                // Generate new condition.
                if (!remainingFuncExprs.isEmpty()) {
                    ILogicalExpression pulledCond = createSelectCondition(remainingFuncExprs);
                    conditionRef.setValue(pulledCond);
                } else {
                    conditionRef.setValue(null);
                }
            }

            // Adds equivalence classes --- one equivalent class between a primary key
            // variable and a record field-access expression.
            EquivalenceClassUtils.addEquivalenceClassesForPrimaryIndexAccess(primaryIndexUnnestOp, scanVariables,
                    recordType, dataset, context);
        }

        if (tmpPrimaryIndexUnnestOp != null) {
            return tmpPrimaryIndexUnnestOp;
        } else {
            return primaryIndexUnnestOp;
        }
    }

    private int createKeyVarsAndExprs(int numKeys, LimitType[] keyLimits, ILogicalExpression[] searchKeyExprs,
            ArrayList<LogicalVariable> assignKeyVarList, ArrayList<Mutable<ILogicalExpression>> assignKeyExprList,
            ArrayList<LogicalVariable> keyVarList, IOptimizationContext context) {
        if (keyLimits[0] == null) {
            return 0;
        }
        for (int i = 0; i < numKeys; i++) {
            ILogicalExpression searchKeyExpr = searchKeyExprs[i];
            LogicalVariable keyVar = null;
            if (searchKeyExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                keyVar = context.newVar();
                assignKeyExprList.add(new MutableObject<ILogicalExpression>(searchKeyExpr));
                assignKeyVarList.add(keyVar);
            } else {
                keyVar = ((VariableReferenceExpression) searchKeyExpr).getVariableReference();
            }
            keyVarList.add(keyVar);
        }
        return numKeys;
    }

    private void getNewConditionExprs(Mutable<ILogicalExpression> conditionRef,
            Set<ILogicalExpression> replacedFuncExprs, List<Mutable<ILogicalExpression>> remainingFuncExprs) {
        remainingFuncExprs.clear();
        if (replacedFuncExprs.isEmpty()) {
            return;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) conditionRef.getValue();
        if (replacedFuncExprs.size() == 1) {
            Iterator<ILogicalExpression> it = replacedFuncExprs.iterator();
            if (!it.hasNext()) {
                return;
            }
            if (funcExpr == it.next()) {
                // There are no remaining function exprs.
                return;
            }
        }
        // The original select cond must be an AND. Check it just to be sure.
        if (funcExpr.getFunctionIdentifier() != AlgebricksBuiltinFunctions.AND) {
            throw new IllegalStateException();
        }
        // Clean the conjuncts.
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            ILogicalExpression argExpr = arg.getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            // If the function expression was not replaced by the new index
            // plan, then add it to the list of remaining function expressions.
            if (!replacedFuncExprs.contains(argExpr)) {
                remainingFuncExprs.add(arg);
            }
        }
    }

    private <T> int indexOf(T value, List<T> coll) {
        int i = 0;
        for (T member : coll) {
            if (member.equals(value)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    private LimitType getLimitType(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree probeSubTree) {
        ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(optFuncExpr.getFuncExpr()
                .getFunctionIdentifier());
        LimitType limit = null;
        switch (ck) {
            case EQ: {
                limit = LimitType.EQUAL;
                break;
            }
            case GE: {
                limit = probeIsOnLhs(optFuncExpr, probeSubTree) ? LimitType.HIGH_INCLUSIVE : LimitType.LOW_INCLUSIVE;
                break;
            }
            case GT: {
                limit = probeIsOnLhs(optFuncExpr, probeSubTree) ? LimitType.HIGH_EXCLUSIVE : LimitType.LOW_EXCLUSIVE;
                break;
            }
            case LE: {
                limit = probeIsOnLhs(optFuncExpr, probeSubTree) ? LimitType.LOW_INCLUSIVE : LimitType.HIGH_INCLUSIVE;
                break;
            }
            case LT: {
                limit = probeIsOnLhs(optFuncExpr, probeSubTree) ? LimitType.LOW_EXCLUSIVE : LimitType.HIGH_EXCLUSIVE;
                break;
            }
            case NEQ: {
                limit = null;
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
        return limit;
    }

    private boolean probeIsOnLhs(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree probeSubTree) {
        if (probeSubTree == null) {
            // We are optimizing a selection query. Search key is a constant. Return true if constant is on lhs.
            return optFuncExpr.getFuncExpr().getArguments().get(0) == optFuncExpr.getConstantVal(0);
        } else {
            // We are optimizing a join query. Determine whether the feeding variable is on the lhs.
            return (optFuncExpr.getOperatorSubTree(0) == null || optFuncExpr.getOperatorSubTree(0) == probeSubTree);
        }
    }

    private ILogicalExpression createSelectCondition(List<Mutable<ILogicalExpression>> predList) {
        if (predList.size() > 1) {
            IFunctionInfo finfo = FunctionUtils.getFunctionInfo(AlgebricksBuiltinFunctions.AND);
            return new ScalarFunctionCallExpression(finfo, predList);
        }
        return predList.get(0).getValue();
    }

    @Override
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        // If we are optimizing a join, check for the indexed nested-loop join hint.
        if (optFuncExpr.getNumLogicalVars() == 2) {
            if (!optFuncExpr.getFuncExpr().getAnnotations().containsKey(IndexedNLJoinExpressionAnnotation.INSTANCE)) {
                return false;
            }
        }
        // Are we skipping a secondary index-search by using "skip-index" hint?
        if (!index.isPrimaryIndex()
                && optFuncExpr.getFuncExpr().getAnnotations()
                        .containsKey(SkipSecondaryIndexSearchExpressionAnnotation.INSTANCE)) {
            return false;
        }
        // No additional analysis required for BTrees.
        return true;
    }
}