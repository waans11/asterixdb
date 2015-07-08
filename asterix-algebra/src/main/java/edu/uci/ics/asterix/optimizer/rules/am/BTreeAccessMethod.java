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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.optimizer.rules.util.EquivalenceClassUtils;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IndexedNLJoinExpressionAnnotation;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

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
    private static List<FunctionIdentifier> funcIdents = new ArrayList<FunctionIdentifier>();
    static {
        funcIdents.add(AlgebricksBuiltinFunctions.EQ);
        funcIdents.add(AlgebricksBuiltinFunctions.LE);
        funcIdents.add(AlgebricksBuiltinFunctions.GE);
        funcIdents.add(AlgebricksBuiltinFunctions.LT);
        funcIdents.add(AlgebricksBuiltinFunctions.GT);
    }

    public static BTreeAccessMethod INSTANCE = new BTreeAccessMethod();

    @Override
    public List<FunctionIdentifier> getOptimizableFunctions() {
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

        // index-only plan, a.k.a. TryLock() on PK optimization:
        // If the given secondary index can't generate false-positive results (a super-set of the true result set) and
        // the plan only deals with PK, and/or SK,
        // we can generate an optimized plan that does not require verification for PKs where
        // tryLock() is succeeded.
        // If tryLock() on PK from a secondary index search is succeeded,
        // SK, PK from a secondary index-search will be fed into Union Operators without looking the primary index.
        // If fails, a primary-index lookup will be placed and the results will be fed into Union Operators.
        // So, we need to push-down select and assign (unnest) to the after primary-index lookup.

        // Check whether assign (unnest) operator exists before the select operator
        Mutable<ILogicalOperator> assignBeforeSelectOpRef = (subTree.assignsAndUnnestsRefs.isEmpty()) ? null
                : subTree.assignsAndUnnestsRefs.get(0);
        ILogicalOperator assignBeforeSelectOp = null;
        if (assignBeforeSelectOpRef != null) {
            assignBeforeSelectOp = assignBeforeSelectOpRef.getValue();
        }

        //        List<LogicalVariable> aboveSelectVars = new ArrayList<LogicalVariable>();

        // logical variables that select operator is using
        List<LogicalVariable> usedVarsInSelect = new ArrayList<LogicalVariable>();

        // live variables that select operator can access
        List<LogicalVariable> liveVarsInSelect = new ArrayList<LogicalVariable>();
        //        List<LogicalVariable> assignBeforeSelectVars = new ArrayList<LogicalVariable>();

        // PK, record variable
        List<LogicalVariable> dataScanPKRecordVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> dataScanPKVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> dataScanRecordVars = new ArrayList<LogicalVariable>();

        // From now on, check whether the given plan is an index-only plan
        //        VariableUtilities.getUsedVariables((ILogicalOperator) aboveSelectRef.getValue(), aboveSelectVars);
        VariableUtilities.getUsedVariables((ILogicalOperator) selectRef.getValue(), usedVarsInSelect);
        VariableUtilities.getLiveVariables((ILogicalOperator) selectRef.getValue(), liveVarsInSelect);
        //        if (assignBeforeSelectOp != null) {
        //            VariableUtilities.getProducedVariables(assignBeforeSelectOp, assignBeforeSelectVars);
        //        }

        // Get PK, record variables
        dataScanPKRecordVars = subTree.getDataSourceVariables();
        subTree.getPrimaryKeyVars(dataScanPKVars);
        dataScanRecordVars.addAll(dataScanPKRecordVars);
        dataScanRecordVars.removeAll(dataScanPKVars);

        // At this stage, we know that this plan is utilizing an index, however we are not sure
        // that this plan is an index-only plan that only uses PK and/or a secondary key field.
        // Thus, we check whether select operator is only using variables from assign or data-source-scan
        // This is to ensure that an assign before select is not creating more variables other than a secondary key field.

        // Need to check whether variables from select operator only contain SK and/or PK condition
        List<IOptimizableFuncExpr> matchedFuncExprs = analysisCtx.matchedFuncExprs;

        // Fetch the field names of the primary index and chosen index
        Dataset dataset = subTree.dataset;
        List<List<String>> PKfieldNames = DatasetUtils.getPartitioningKeys(dataset);
        List<List<String>> chosenIndexFieldNames = chosenIndex.getKeyFieldNames();
        List<LogicalVariable> chosenIndexVars = new ArrayList<LogicalVariable>();

        boolean isIndexOnlyPlanPossible = false;

        // #1. Check whether variables in the SELECT operator only contains secondary key field and/or PK field condition
        int varFoundCount = 0;
        for (IOptimizableFuncExpr matchedFuncExpr : matchedFuncExprs) {
            for (LogicalVariable selectVar : usedVarsInSelect) {
                int varIndex = matchedFuncExpr.findLogicalVar(selectVar);
                if (varIndex != -1) {
                    List<String> fieldNameOfSelectVars = matchedFuncExpr.getFieldName(varIndex);
                    // Is this variable from PK?
                    int keyPos = PKfieldNames.indexOf(fieldNameOfSelectVars);
                    if (keyPos < 0) {
                        // Is this variable from chosen index (SK)?
                        keyPos = chosenIndexFieldNames.indexOf(fieldNameOfSelectVars);
                        if (keyPos < 0) {
                            isIndexOnlyPlanPossible = false;
                            break;
                        } else {
                            if (!chosenIndexVars.contains(selectVar)) {
                                chosenIndexVars.add(selectVar);
                                varFoundCount++;
                            }
                            isIndexOnlyPlanPossible = true;
                        }
                    } else {
                        if (!chosenIndexVars.contains(selectVar)) {
                            chosenIndexVars.add(selectVar);
                            varFoundCount++;
                        }
                        isIndexOnlyPlanPossible = true;
                    }
                } else {
                    continue;
                }
            }
            if (!isIndexOnlyPlanPossible) {
                break;
            }
        }
        // All variables in the SELECT condition should be found.
        if (varFoundCount < usedVarsInSelect.size()) {
            isIndexOnlyPlanPossible = false;
        }

        // #2. Check whether operators after the SELECT operator only use PK or secondary field variables.
        //     We exclude the variables produced after the SELECT operator.
        if (isIndexOnlyPlanPossible) {
            List<LogicalVariable> usedVarsAfterSelect = new ArrayList<LogicalVariable>();
            for (Mutable<ILogicalOperator> aboveSelectRef : aboveSelectRefs) {
                usedVarsAfterSelect.clear();
                VariableUtilities.getUsedVariables((ILogicalOperator) aboveSelectRef.getValue(), usedVarsAfterSelect);
                for (LogicalVariable usedVarAfterSelect : usedVarsAfterSelect) {
                    // If this operator is using the variables that are created before the SELECT operator
                    if (liveVarsInSelect.contains(usedVarAfterSelect)) {
                        if (dataScanPKRecordVars.contains(usedVarAfterSelect)) {
                            isIndexOnlyPlanPossible = true;
                            AbstractLogicalOperator aboveSelectRefOp = (AbstractLogicalOperator) aboveSelectRef
                                    .getValue();
                            // aggregate on count?
                            if (aboveSelectRefOp.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                                AggregateOperator aggOp = (AggregateOperator) aboveSelectRefOp;
                                List<Mutable<ILogicalExpression>> condExprs = aggOp.getExpressions();
                                for (int i = 0; i < condExprs.size(); i++) {
                                    ILogicalExpression condExpr = (ILogicalExpression) condExprs.get(i).getValue();
                                    if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                                        continue;
                                    } else {
                                        AbstractFunctionCallExpression condExprFnCall = (AbstractFunctionCallExpression) condExpr;
                                        if (condExprFnCall.getFunctionIdentifier() != AsterixBuiltinFunctions.COUNT) {
                                            continue;
                                        } else {
                                            // COUNT found. count on record ($$0) can be replaced as PK variable
                                            if (dataScanRecordVars.contains(usedVarAfterSelect)) {
                                                condExprFnCall.substituteVar(usedVarAfterSelect, dataScanPKVars.get(0));
                                            }
                                        }
                                    }
                                }
                            }
                        } else if (chosenIndexVars.contains(usedVarAfterSelect)) {
                            isIndexOnlyPlanPossible = true;
                        } else {
                            isIndexOnlyPlanPossible = false;
                            break;
                        }
                    } else {
                        // check is not necessary since this variable is generated after the SELECT operator
                        continue;
                    }
                }
                if (!isIndexOnlyPlanPossible) {
                    break;
                }
            }
        }

        if (isIndexOnlyPlanPossible && !chosenIndex.canProduceFalsePositive()) {
            analysisCtx.setIndexOnlyPlanEnabled(true);
        } else {
            analysisCtx.setIndexOnlyPlanEnabled(false);
        }

        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(aboveSelectRefs, selectRef, conditionRef,
                assignBeforeSelectOpRef, subTree, null, chosenIndex, analysisCtx, false, false, false, context);
        if (primaryIndexUnnestOp == null) {
            return false;
        }

        // Temporary: we are not doing post-processing check
        //        ((AbstractLogicalOperator) primaryIndexUnnestOp).setExecutionMode(ExecutionMode.PARTITIONED);
        //        selectRef.setValue(primaryIndexUnnestOp);

        // Generate new select using the new condition.
        if (conditionRef.getValue() != null) {
            //            select.getInputs().clear();
            if (assignBeforeSelectOp != null) {
                // If a tryLock() on PK optimization is possible, we don't need to use select operator.
                if (!chosenIndex.canProduceFalsePositive() && analysisCtx.isIndexOnlyPlanEnabled()) {
                    // Get dataSourceRef operator - unnest-map (PK, record)
                    // Right now, the order of opertors is: union -> select -> assign -> unnest-map
                    ILogicalOperator dataSourceRefOp = (ILogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                            .getValue(); // select
                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                    subTree.dataSourceRef.setValue(dataSourceRefOp);
                    selectRef.setValue(primaryIndexUnnestOp);
                } else {
                    select.getInputs().clear();
                    subTree.dataSourceRef.setValue(primaryIndexUnnestOp);
                    select.getInputs().add(new MutableObject<ILogicalOperator>(assignBeforeSelectOp));
                }

            } else {
                // If a tryLock() on PK is possible, we don't need to use select operator.
                if (!chosenIndex.canProduceFalsePositive() && analysisCtx.isIndexOnlyPlanEnabled()) {
                    select.getInputs().clear();
                    selectRef.setValue(primaryIndexUnnestOp);
                } else {
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
    public boolean applyJoinPlanTransformation(Mutable<ILogicalOperator> joinRef,
            OptimizableOperatorSubTree leftSubTree, OptimizableOperatorSubTree rightSubTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context, boolean isLeftOuterJoin,
            boolean hasGroupBy) throws AlgebricksException {
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinRef.getValue();
        Mutable<ILogicalExpression> conditionRef = joinOp.getCondition();
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

        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(null, joinRef, conditionRef, null,
                indexSubTree, probeSubTree, chosenIndex, analysisCtx, true, isLeftOuterJoin, true, context);
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
            SelectOperator topSelect = new SelectOperator(conditionRef, isLeftOuterJoin, newNullPlaceHolderVar);
            topSelect.getInputs().add(indexSubTree.rootRef);
            topSelect.setExecutionMode(ExecutionMode.LOCAL);
            context.computeAndSetTypeEnvironmentForOperator(topSelect);
            // Replace the original join with the new subtree rooted at the select op.
            joinRef.setValue(topSelect);
        } else {
            joinRef.setValue(indexSubTree.rootRef.getValue());
        }
        return true;
    }

    /**
     * Create a secondary index lookup -> sort -> primary index look up from a select plan
     */
    private ILogicalOperator createSecondaryToPrimaryPlan(List<Mutable<ILogicalOperator>> aboveTopOpRefs,
            Mutable<ILogicalOperator> opRef, Mutable<ILogicalExpression> conditionRef,
            Mutable<ILogicalOperator> assignBeforeTheOpRef, OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            boolean retainInput, boolean retainNull, boolean requiresBroadcast, IOptimizationContext context)
            throws AlgebricksException {
        Dataset dataset = indexSubTree.dataset;
        ARecordType recordType = indexSubTree.recordType;
        // we made sure indexSubTree has datasource scan
        AbstractDataSourceOperator dataSourceOp = (AbstractDataSourceOperator) indexSubTree.dataSourceRef.getValue();
        List<Pair<Integer, Integer>> exprAndVarList = analysisCtx.indexExprsAndVars.get(chosenIndex);
        List<IOptimizableFuncExpr> matchedFuncExprs = analysisCtx.matchedFuncExprs;
        int numSecondaryKeys = analysisCtx.indexNumMatchedKeys.get(chosenIndex);
        boolean isIndexOnlyPlanEnabled = !chosenIndex.canProduceFalsePositive() && analysisCtx.isIndexOnlyPlanEnabled();
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
                isIndexOnlyPlanEnabled);
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
                isIndexOnlyPlanEnabled);

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
            tmpPrimaryIndexUnnestOp = (AbstractLogicalOperator) AccessMethodUtils.createPrimaryIndexUnnestMap(
                    aboveTopOpRefs, opRef, conditionRef, assignBeforeTheOpRef, dataSourceOp, dataset, recordType,
                    secondaryIndexUnnestOp, context, true, retainInput, retainNull, false, chosenIndex, analysisCtx,
                    outputPrimaryKeysOnlyFromSIdxSearch);

            // Replace the datasource scan with the new plan rooted at
            // Get dataSourceRef operator - unnest-map (PK, record)
            if (!chosenIndex.canProduceFalsePositive() && isIndexOnlyPlanEnabled) {
                // Right now, the order of opertors is: union -> select -> assign -> unnest-map
                AbstractLogicalOperator dataSourceRefOp = (AbstractLogicalOperator) tmpPrimaryIndexUnnestOp.getInputs()
                        .get(0).getValue(); // select
                dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map

                //                indexSubTree.dataSourceRef.setValue(tmpPrimaryIndexUnnestOp);
                indexSubTree.dataSourceRef.setValue(dataSourceRefOp);
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

            //Temporary:
            //            primaryIndexPostProccessingIsNeeded = false;
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