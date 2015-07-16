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
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
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
 * Class for helping rewrite rules to choose and apply RTree indexes.
 */
public class RTreeAccessMethod implements IAccessMethod {

    private static List<FunctionIdentifier> funcIdents = new ArrayList<FunctionIdentifier>();
    static {
        funcIdents.add(AsterixBuiltinFunctions.SPATIAL_INTERSECT);
    }

    public static RTreeAccessMethod INSTANCE = new RTreeAccessMethod();

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

        ARecordType recordType = subTree.recordType;

        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(chosenIndex, analysisCtx);

        int optFieldIdx = AccessMethodUtils.chooseFirstOptFuncVar(chosenIndex, analysisCtx);
        Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(optFuncExpr.getFieldType(optFieldIdx),
                optFuncExpr.getFieldName(optFieldIdx), recordType);
        if (keyPairType == null) {
            return false;
        }

        // index-only plan possible?
        boolean isIndexOnlyPlanPossible = false;

        // secondary key field usage in the select condition
        boolean secondaryKeyFieldUsedInSelectCondition = false;

        // secondary key field usage after the select operator
        boolean secondaryKeyFieldUsedAfterSelectOp = false;

        // Preliminary condition for R-Tree:
        // If an index is not on POINT or RECTANGLE, the query result can generate false positives.
        // And the result from secondary index search is an MBR, we can't use an index-only plan.
        if (keyPairType.first != BuiltinType.APOINT && keyPairType.first != BuiltinType.ARECTANGLE) {
            isIndexOnlyPlanPossible = false;
        } else {
            // At this point, th
            isIndexOnlyPlanPossible = true;
        }

        // index-only plan, a.k.a. tryLock() on PK optimization:
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

        // logical variables that select operator is using
        List<LogicalVariable> usedVarsInSelect = new ArrayList<LogicalVariable>();

        // live variables that select operator can access
        List<LogicalVariable> liveVarsInSelect = new ArrayList<LogicalVariable>();

        // PK, record variable
        List<LogicalVariable> dataScanPKRecordVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> dataScanPKVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> dataScanRecordVars = new ArrayList<LogicalVariable>();

        // From now on, check whether the given plan is an index-only plan
        VariableUtilities.getUsedVariables((ILogicalOperator) selectRef.getValue(), usedVarsInSelect);
        VariableUtilities.getLiveVariables((ILogicalOperator) selectRef.getValue(), liveVarsInSelect);

        // Get PK, record variables
        dataScanPKRecordVars = subTree.getDataSourceVariables();
        subTree.getPrimaryKeyVars(dataScanPKVars);
        dataScanRecordVars.addAll(dataScanPKRecordVars);
        dataScanRecordVars.removeAll(dataScanPKVars);

        // At this stage, we know that this plan is utilizing an index, however we are not sure
        // that this plan is an index-only plan that only uses PK and/or a secondary key field.
        // Thus, we check whether select operator is only using variables from assign or data-source-scan
        // and the field-name of those variables are only PK or SK.

        // Need to check whether variables from select operator only contain SK and/or PK condition
        List<IOptimizableFuncExpr> matchedFuncExprs = analysisCtx.matchedFuncExprs;

        // Fetch the field names of the primary index and chosen index
        Dataset dataset = subTree.dataset;
        List<List<String>> PKfieldNames = DatasetUtils.getPartitioningKeys(dataset);
        List<List<String>> chosenIndexFieldNames = chosenIndex.getKeyFieldNames();
        List<LogicalVariable> chosenIndexVars = new ArrayList<LogicalVariable>();

        // #1. Check whether variables in the SELECT operator are from secondary key fields and/or PK fields
        int selectVarFoundCount = 0;
        if (isIndexOnlyPlanPossible) {
            for (IOptimizableFuncExpr matchedFuncExpr : matchedFuncExprs) {
                // for each select condition,
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
                                    selectVarFoundCount++;
                                }
                                isIndexOnlyPlanPossible = true;
                                secondaryKeyFieldUsedInSelectCondition = true;
                            }
                        } else {
                            if (!chosenIndexVars.contains(selectVar)) {
                                chosenIndexVars.add(selectVar);
                                selectVarFoundCount++;
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
            if (selectVarFoundCount < usedVarsInSelect.size()) {
                isIndexOnlyPlanPossible = false;
            }
        }

        // #2. Check whether operators after the SELECT operator only use PK or secondary field variables.
        //     We exclude the variables produced after the SELECT operator.
        boolean countIsUsedInThePlan = false;
        List<LogicalVariable> countUsedVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> producedVarsAfterSelect = new ArrayList<LogicalVariable>();

        VariableUtilities.getUsedVariables((ILogicalOperator) selectRef.getValue(), usedVarsInSelect);

        AbstractLogicalOperator aboveSelectRefOp = null;
        AggregateOperator aggOp = null;
        ILogicalExpression condExpr = null;
        List<Mutable<ILogicalExpression>> condExprs = null;
        AbstractFunctionCallExpression condExprFnCall = null;

        if (isIndexOnlyPlanPossible) {
            List<LogicalVariable> usedVarsAfterSelect = new ArrayList<LogicalVariable>();
            // for each operator above SELECT operator
            for (Mutable<ILogicalOperator> aboveSelectRef : aboveSelectRefs) {
                usedVarsAfterSelect.clear();
                producedVarsAfterSelect.clear();
                VariableUtilities.getUsedVariables((ILogicalOperator) aboveSelectRef.getValue(), usedVarsAfterSelect);
                VariableUtilities.getProducedVariables((ILogicalOperator) aboveSelectRef.getValue(),
                        producedVarsAfterSelect);
                // Check whether COUNT exists since we can substitute record variable into PK variable.
                aboveSelectRefOp = (AbstractLogicalOperator) aboveSelectRef.getValue();
                if (aboveSelectRefOp.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                    aggOp = (AggregateOperator) aboveSelectRefOp;
                    condExprs = aggOp.getExpressions();
                    for (int i = 0; i < condExprs.size(); i++) {
                        condExpr = (ILogicalExpression) condExprs.get(i).getValue();
                        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                            continue;
                        } else {
                            condExprFnCall = (AbstractFunctionCallExpression) condExpr;
                            if (condExprFnCall.getFunctionIdentifier() != AsterixBuiltinFunctions.COUNT) {
                                continue;
                            } else {
                                // COUNT found. count on record ($$0) can be replaced as PK variable
                                countIsUsedInThePlan = true;
                                VariableUtilities.getUsedVariables((ILogicalOperator) aboveSelectRef.getValue(),
                                        countUsedVars);
                                break;
                            }
                        }
                    }
                }

                // for each variable that is used in an operator above SELECT operator
                for (LogicalVariable usedVarAfterSelect : usedVarsAfterSelect) {
                    // If this operator is using the variables that are created before the SELECT operator
                    if (liveVarsInSelect.contains(usedVarAfterSelect)) {
                        // From PK?
                        if (dataScanPKVars.contains(usedVarAfterSelect)) {
                            isIndexOnlyPlanPossible = true;
                        } else if (chosenIndexVars.contains(usedVarAfterSelect)) {
                            // From SK?
                            isIndexOnlyPlanPossible = true;
                            secondaryKeyFieldUsedAfterSelectOp = true;
                        } else if (dataScanRecordVars.contains(usedVarAfterSelect)) {
                            // The only case that we allow when a record variable is used is when
                            // it is used with count either directly or indirectly via record-constructor
                            if (!countIsUsedInThePlan) {
                                // We don't need to care about this case since COUNT is not used.
                                isIndexOnlyPlanPossible = false;
                                break;
                            } else if (countUsedVars.contains(usedVarAfterSelect)
                                    || countUsedVars.containsAll(producedVarsAfterSelect)) {
                                VariableUtilities.substituteVariables(aboveSelectRefOp, usedVarAfterSelect,
                                        dataScanPKVars.get(0), context);
                                isIndexOnlyPlanPossible = true;
                            }
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

        // Check whether assign (unnest) operator exists before the select operator
        //        Mutable<ILogicalOperator> assignBeforeSelectOpRef = (subTree.assignsAndUnnestsRefs.isEmpty()) ? null
        //                : subTree.assignsAndUnnestsRefs.get(0);
        //        ILogicalOperator assignBeforeSelectOp = null;
        //        if (assignBeforeSelectOpRef != null) {
        //            assignBeforeSelectOp = assignBeforeSelectOpRef.getValue();
        //        }

        // At this point, we are sure that either an index-only plan is possible or not.
        // If the following two conditions are met, then we don't need to do post-processing.
        // That is, the given index will not generate false positive results.
        // If not, we need to put "select" condition to the path where tryLock on PK succeeds, too.
        // 1) Query shape should be rectangle and
        // 2) key field type of the index should be either point or rectangle.

        boolean verificationAfterSIdxSearchRequired = true;

        if (isIndexOnlyPlanPossible) {
            if (matchedFuncExprs.size() == 1) {
                condExpr = (ILogicalExpression) optFuncExpr.getFuncExpr();
                condExprFnCall = (AbstractFunctionCallExpression) condExpr;
                for (int i = 0; i < condExprFnCall.getArguments().size(); i++) {
                    Mutable<ILogicalExpression> t = condExprFnCall.getArguments().get(i);
                    if (t.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                        AsterixConstantValue tmpVal = (AsterixConstantValue) ((ConstantExpression) t.getValue())
                                .getValue();
                        if (tmpVal.getObject().getType() == BuiltinType.APOINT
                                || tmpVal.getObject().getType() == BuiltinType.ARECTANGLE) {
                            if (keyPairType.first == BuiltinType.APOINT || keyPairType.first == BuiltinType.ARECTANGLE) {
                                verificationAfterSIdxSearchRequired = false;
                            } else {
                                verificationAfterSIdxSearchRequired = true;
                                break;
                            }
                        } else {
                            verificationAfterSIdxSearchRequired = true;
                            break;
                        }
                    }
                }
            } else {
                verificationAfterSIdxSearchRequired = true;
            }
        }

        //        if (verificationAfterSIdxSearchRequired) {
        //            isIndexOnlyPlanPossible = false;
        //        }

        if (isIndexOnlyPlanPossible) {
            analysisCtx.setIndexOnlyPlanEnabled(true);
        } else {
            analysisCtx.setIndexOnlyPlanEnabled(false);
        }

        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(aboveSelectRefs, selectRef,
                assignBeforeSelectOpRef, subTree, null, chosenIndex, optFuncExpr, analysisCtx, false, false, false,
                context, isIndexOnlyPlanPossible, verificationAfterSIdxSearchRequired,
                secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp);
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
                false, false, false);
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
            boolean secondaryKeyFieldUsedInSelectCondition, boolean secondaryKeyFieldUsedAfterSelectOp)
            throws AlgebricksException {
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
                isIndexOnlyPlanEnabled);

        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            ExternalDataLookupOperator externalDataAccessOp = AccessMethodUtils.createExternalDataLookupUnnestMap(
                    dataSourceOp, dataset, recordType, secondaryIndexUnnestOp, context, chosenIndex, retainInput,
                    retainNull, outputPrimaryKeysOnlyFromSIdxSearch);
            return externalDataAccessOp;
        } else {
            //            UnnestMapOperator primaryIndexUnnestOp = AccessMethodUtils.createPrimaryIndexUnnestMap(dataSourceOp,
            //                    dataset, recordType, secondaryIndexUnnestOp, context, true, retainInput, false, false, chosenIndex);

            SelectOperator select = (SelectOperator) topRef.getValue();
            Mutable<ILogicalExpression> conditionRef = select.getCondition();

            ILogicalOperator primaryIndexUnnestOp = (AbstractLogicalOperator) AccessMethodUtils
                    .createPrimaryIndexUnnestMap(afterTopRefs, topRef, conditionRef, assignBeforeTopRef, dataSourceOp,
                            dataset, recordType, secondaryIndexUnnestOp, context, true, retainInput, false, false,
                            chosenIndex, analysisCtx, outputPrimaryKeysOnlyFromSIdxSearch,
                            verificationAfterSIdxSearchRequired, secondaryKeyFieldUsedInSelectCondition,
                            secondaryKeyFieldUsedAfterSelectOp);

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
