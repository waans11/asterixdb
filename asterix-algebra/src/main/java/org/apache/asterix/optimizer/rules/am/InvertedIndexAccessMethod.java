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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.algebra.base.LogicalOperatorDeepCopyVisitor;
import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.common.AqlExpressionTypeComputer;
import org.apache.asterix.formats.nontagged.AqlBinaryTokenizerFactoryProvider;
import org.apache.asterix.lang.aql.util.FunctionUtils;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACollection;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quintuple;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.ConjunctiveEditDistanceSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.ConjunctiveListEditDistanceSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.ConjunctiveSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.DisjunctiveSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.EditDistanceSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.JaccardSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.ListEditDistanceSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;

/**
 * Class for helping rewrite rules to choose and apply inverted indexes.
 */
public class InvertedIndexAccessMethod implements IAccessMethod {

    // Enum describing the search modifier type. Used for passing info to jobgen.
    public static enum SearchModifierType {
        CONJUNCTIVE,
        DISJUNCTIVE,
        JACCARD,
        EDIT_DISTANCE,
        CONJUNCTIVE_EDIT_DISTANCE,
        INVALID
    }

    // Second boolean value means that this function can produce false positive results if it is set to false.
    // That is, an index-search alone cannot replace SELECT condition and
    // SELECT condition needs to be applied after that index-search to get the correct results.
    private static List<Pair<FunctionIdentifier, Boolean>> funcIdents = new ArrayList<Pair<FunctionIdentifier, Boolean>>();
    static {
        // contains(substring) function
        funcIdents.add(new Pair<FunctionIdentifier, Boolean>(AsterixBuiltinFunctions.CONTAINS_SUBSTRING, false));
        // For matching similarity-check functions. For example, similarity-jaccard-check returns a list of two items,
        // and the select condition will get the first list-item and check whether it evaluates to true.
        funcIdents.add(new Pair<FunctionIdentifier, Boolean>(AsterixBuiltinFunctions.GET_ITEM, false));
        // full-text search function
        funcIdents.add(new Pair<FunctionIdentifier, Boolean>(AlgebricksBuiltinFunctions.CONTAINS, true));
    }

    // These function identifiers are matched in this AM's analyzeFuncExprArgs(),
    // and are not visible to the outside driver.
    // Second boolean value means that this function can produce false positive results if it is set to false.
    // That is, an index-search alone cannot replace SELECT condition and
    // SELECT condition needs to be applied after that index-search to get the correct results.
    private static List<Pair<FunctionIdentifier, Boolean>> secondLevelFuncIdents = new ArrayList<Pair<FunctionIdentifier, Boolean>>();
    static {
        secondLevelFuncIdents.add(new Pair<FunctionIdentifier, Boolean>(
                AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK, false));
        secondLevelFuncIdents.add(new Pair<FunctionIdentifier, Boolean>(AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK,
                false));
        secondLevelFuncIdents.add(new Pair<FunctionIdentifier, Boolean>(
                AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS_SUBSTRING, false));
    }

    public static InvertedIndexAccessMethod INSTANCE = new InvertedIndexAccessMethod();

    @Override
    public List<Pair<FunctionIdentifier, Boolean>> getOptimizableFunctions() {
        return funcIdents;
    }

    @Override
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {

        boolean matches;
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS_SUBSTRING) {
            matches = AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx, context,
                    typeEnvironment);
            if (!matches) {
                matches = AccessMethodUtils.analyzeFuncExprArgsForTwoVars(funcExpr, analysisCtx);
            }
            return matches;
        } else if (funcExpr.getFunctionIdentifier() == AlgebricksBuiltinFunctions.CONTAINS) {
            matches = AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx, context,
                    typeEnvironment);
            if (!matches) {
                matches = AccessMethodUtils.analyzeFuncExprArgsForTwoVars(funcExpr, analysisCtx);
            }
            return matches;
        }
        return analyzeGetItemFuncExpr(funcExpr, assignsAndUnnests, analysisCtx);
    }

    public boolean analyzeGetItemFuncExpr(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests, AccessMethodAnalysisContext analysisCtx)
            throws AlgebricksException {
        if (funcExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.GET_ITEM) {
            return false;
        }
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // The second arg is the item index to be accessed. It must be a constant.
        if (arg2.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        // The first arg must be a variable or a function expr.
        // If it is a variable we must track its origin in the assigns to get the original function expr.
        if (arg1.getExpressionTag() != LogicalExpressionTag.VARIABLE
                && arg1.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression matchedFuncExpr = null;
        // The get-item arg is function call, directly check if it's optimizable.
        if (arg1.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            matchedFuncExpr = (AbstractFunctionCallExpression) arg1;
        }
        // The get-item arg is a variable. Search the assigns and unnests for its origination function.
        int matchedAssignOrUnnestIndex = -1;
        if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            VariableReferenceExpression varRefExpr = (VariableReferenceExpression) arg1;
            // Try to find variable ref expr in all assigns.
            for (int i = 0; i < assignsAndUnnests.size(); i++) {
                AbstractLogicalOperator op = assignsAndUnnests.get(i);
                if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    AssignOperator assign = (AssignOperator) op;
                    List<LogicalVariable> assignVars = assign.getVariables();
                    List<Mutable<ILogicalExpression>> assignExprs = assign.getExpressions();
                    for (int j = 0; j < assignVars.size(); j++) {
                        LogicalVariable var = assignVars.get(j);
                        if (var != varRefExpr.getVariableReference()) {
                            continue;
                        }
                        // We've matched the variable in the first assign. Now analyze the originating function.
                        ILogicalExpression matchedExpr = assignExprs.get(j).getValue();
                        if (matchedExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                            return false;
                        }
                        matchedFuncExpr = (AbstractFunctionCallExpression) matchedExpr;
                        break;
                    }
                } else {
                    UnnestOperator unnest = (UnnestOperator) op;
                    LogicalVariable var = unnest.getVariable();
                    if (var == varRefExpr.getVariableReference()) {
                        ILogicalExpression matchedExpr = unnest.getExpressionRef().getValue();
                        if (matchedExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                            return false;
                        }
                        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) matchedExpr;
                        if (unnestFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.SCAN_COLLECTION) {
                            return false;
                        }
                        matchedFuncExpr = (AbstractFunctionCallExpression) unnestFuncExpr.getArguments().get(0)
                                .getValue();
                    }
                }
                // We've already found a match.
                if (matchedFuncExpr != null) {
                    matchedAssignOrUnnestIndex = i;
                    break;
                }
            }
        }

        boolean found = false;
        // Check that the matched function is optimizable by this access method.
        for (Iterator<Pair<FunctionIdentifier, Boolean>> iterator = secondLevelFuncIdents.iterator(); iterator
                .hasNext();) {
            FunctionIdentifier fID = iterator.next().first;

            if (fID.equals(matchedFuncExpr.getFunctionIdentifier())) {
                found = true;
                break;
            }
        }

        if (!found) {
            return false;
        }

        //        if (!secondLevelFuncIdents.contains(matchedFuncExpr.getFunctionIdentifier())) {
        //            return false;
        //        }
        boolean selectMatchFound = analyzeSelectSimilarityCheckFuncExprArgs(matchedFuncExpr, assignsAndUnnests,
                matchedAssignOrUnnestIndex, analysisCtx);
        boolean joinMatchFound = analyzeJoinSimilarityCheckFuncExprArgs(matchedFuncExpr, assignsAndUnnests,
                matchedAssignOrUnnestIndex, analysisCtx);
        if (selectMatchFound || joinMatchFound) {
            return true;
        }
        return false;
    }

    private boolean analyzeJoinSimilarityCheckFuncExprArgs(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests, int matchedAssignOrUnnestIndex,
            AccessMethodAnalysisContext analysisCtx) throws AlgebricksException {
        // There should be exactly three arguments.
        // The last function argument is assumed to be the similarity threshold.
        ILogicalExpression arg3 = funcExpr.getArguments().get(2).getValue();
        if (arg3.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // We expect arg1 and arg2 to be non-constants for a join.
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT
                || arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return false;
        }
        LogicalVariable fieldVarExpr1 = getNonConstArgFieldExprPair(arg1, funcExpr, assignsAndUnnests,
                matchedAssignOrUnnestIndex);
        if (fieldVarExpr1 == null) {
            return false;
        }
        LogicalVariable fieldVarExpr2 = getNonConstArgFieldExprPair(arg2, funcExpr, assignsAndUnnests,
                matchedAssignOrUnnestIndex);
        if (fieldVarExpr2 == null) {
            return false;
        }
        OptimizableFuncExpr newOptFuncExpr = new OptimizableFuncExpr(funcExpr, new LogicalVariable[] { fieldVarExpr1,
                fieldVarExpr2 }, new ILogicalExpression[] { arg3 },
                new IAType[] { (IAType) AqlExpressionTypeComputer.INSTANCE.getType(arg3, null, null) });
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.matchedFuncExprs) {
            //avoid additional optFuncExpressions in case of a join
            if (optFuncExpr.getFuncExpr().equals(funcExpr)) {
                return true;
            }
        }
        analysisCtx.matchedFuncExprs.add(newOptFuncExpr);
        return true;
    }

    private boolean analyzeSelectSimilarityCheckFuncExprArgs(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests, int matchedAssignOrUnnestIndex,
            AccessMethodAnalysisContext analysisCtx) throws AlgebricksException {
        // There should be exactly three arguments.
        // The last function argument is assumed to be the similarity threshold.
        ILogicalExpression arg3 = funcExpr.getArguments().get(2).getValue();
        if (arg3.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // Determine whether one arg is constant, and the other is non-constant.
        ILogicalExpression constArg = null;
        ILogicalExpression nonConstArg = null;
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT
                && arg2.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            // The arguments of edit-distance-contains() function are asymmetrical, we can only use index if it is on the first argument
            if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS_SUBSTRING) {
                return false;
            }
            constArg = arg1;
            nonConstArg = arg2;
        } else if (arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT
                && arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            constArg = arg2;
            nonConstArg = arg1;
        } else {
            return false;
        }
        LogicalVariable fieldVarExpr = getNonConstArgFieldExprPair(nonConstArg, funcExpr, assignsAndUnnests,
                matchedAssignOrUnnestIndex);
        if (fieldVarExpr == null) {
            return false;
        }

        OptimizableFuncExpr newOptFuncExpr = new OptimizableFuncExpr(funcExpr, new LogicalVariable[] { fieldVarExpr },
                new ILogicalExpression[] { constArg, arg3 }, new IAType[] {
                        (IAType) AqlExpressionTypeComputer.INSTANCE.getType(constArg, null, null),
                        (IAType) AqlExpressionTypeComputer.INSTANCE.getType(arg3, null, null) });
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.matchedFuncExprs) {
            //avoid additional optFuncExpressions in case of a join
            if (optFuncExpr.getFuncExpr().equals(funcExpr))
                return true;
        }
        analysisCtx.matchedFuncExprs.add(newOptFuncExpr);
        return true;
    }

    private LogicalVariable getNonConstArgFieldExprPair(ILogicalExpression nonConstArg,
            AbstractFunctionCallExpression funcExpr, List<AbstractLogicalOperator> assignsAndUnnests,
            int matchedAssignOrUnnestIndex) {
        LogicalVariable fieldVar = null;
        // Analyze nonConstArg depending on similarity function.
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK) {
            AbstractFunctionCallExpression nonConstFuncExpr = funcExpr;
            if (nonConstArg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                nonConstFuncExpr = (AbstractFunctionCallExpression) nonConstArg;
                // TODO: Currently, we're only looking for word and gram tokens (non hashed).
                if (nonConstFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.WORD_TOKENS
                        && nonConstFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.GRAM_TOKENS) {
                    return null;
                }
                // Find the variable that is being tokenized.
                nonConstArg = nonConstFuncExpr.getArguments().get(0).getValue();
            }
        }
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK
                || funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS_SUBSTRING) {
            while (nonConstArg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression nonConstFuncExpr = (AbstractFunctionCallExpression) nonConstArg;
                if (nonConstFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.WORD_TOKENS
                        && nonConstFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.SUBSTRING
                        && nonConstFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.SUBSTRING_BEFORE
                        && nonConstFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.SUBSTRING_AFTER) {
                    return null;
                }
                // Find the variable whose substring is used in the similarity function
                nonConstArg = nonConstFuncExpr.getArguments().get(0).getValue();
            }
        }
        if (nonConstArg.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            fieldVar = ((VariableReferenceExpression) nonConstArg).getVariableReference();
        }
        return fieldVar;
    }

    @Override
    public boolean matchAllIndexExprs() {
        return true;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        return false;
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
    private ILogicalOperator createSecondaryToPrimaryPlan(List<Mutable<ILogicalOperator>> afterTopOpRefs,
            Mutable<ILogicalOperator> topOpRef, Mutable<ILogicalExpression> conditionRef,
            List<Mutable<ILogicalOperator>> assignBeforeTopOpRefs, OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, IOptimizableFuncExpr optFuncExpr,
            boolean retainInput, boolean retainNull, boolean requiresBroadcast, IOptimizationContext context,
            AccessMethodAnalysisContext analysisCtx, boolean secondaryKeyFieldUsedInSelectCondition,
            boolean secondaryKeyFieldUsedAfterSelectOp, boolean noFalsePositiveResultsFromSIdxSearch)
            throws AlgebricksException {

        // Check whether assign (unnest) operator exists before the select operator
        Mutable<ILogicalOperator> assignBeforeSelectOpRef = (indexSubTree.assignsAndUnnestsRefs.isEmpty()) ? null
                : indexSubTree.assignsAndUnnestsRefs.get(0);
        ILogicalOperator assignBeforeSelectOp = null;
        if (assignBeforeSelectOpRef != null) {
            assignBeforeSelectOp = assignBeforeSelectOpRef.getValue();
        }

        Dataset dataset = indexSubTree.dataset;
        ARecordType recordType = indexSubTree.recordType;
        // we made sure indexSubTree has datasource scan
        DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) indexSubTree.dataSourceRef.getValue();

        // index-only plan enabled?
        boolean isIndexOnlyPlan = analysisCtx.getIsIndexOnlyPlan();

        // Set the LIMIT push-down if it is possible.
        long limitNumberOfResult = -1;

        if (noFalsePositiveResultsFromSIdxSearch && dataset.getDatasetType() == DatasetType.INTERNAL) {
            limitNumberOfResult = analysisCtx.getLimitNumberOfResult();
        }

        // We apply index-only plan or reducing the number of SELECT verification plan only for an internal dataset.
        boolean generateTrylockResultFromIndexSearch = false;
        if (dataset.getDatasetType() == DatasetType.INTERNAL
                && (isIndexOnlyPlan || noFalsePositiveResultsFromSIdxSearch)) {
            generateTrylockResultFromIndexSearch = true;
        }

        // For the case A and B, we need to generate the result of tryLock on a PK during the secondary index search.
        // The last parameter ensures that variable will be generated.
        InvertedIndexJobGenParams jobGenParams = new InvertedIndexJobGenParams(chosenIndex.getIndexName(),
                chosenIndex.getIndexType(), dataset.getDataverseName(), dataset.getDatasetName(), retainInput,
                retainNull, requiresBroadcast, generateTrylockResultFromIndexSearch, limitNumberOfResult);
        // Add function-specific args such as search modifier, and possibly a similarity threshold.
        addFunctionSpecificArgs(optFuncExpr, jobGenParams);
        // Add the type of search key from the optFuncExpr.
        addSearchKeyType(optFuncExpr, indexSubTree, context, jobGenParams);

        // Operator that feeds the secondary-index search.
        AbstractLogicalOperator inputOp = null;
        // Here we generate vars and funcs for assigning the secondary-index keys to be fed into the secondary-index search.
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        // probeSubTree is null if we are dealing with a selection query, and non-null for join queries.
        if (probeSubTree == null) {
            // List of expressions for the assign.
            ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
            // Add key vars and exprs to argument list.
            addKeyVarsAndExprs(optFuncExpr, keyVarList, keyExprList, context);
            // Assign operator that sets the secondary-index search-key fields.
            inputOp = new AssignOperator(keyVarList, keyExprList);
            // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
            inputOp.getInputs().add(dataSourceScan.getInputs().get(0));
            inputOp.setExecutionMode(dataSourceScan.getExecutionMode());
        } else {
            // We are optimizing a join. Add the input variable to the secondaryIndexFuncArgs.
            LogicalVariable inputSearchVariable = getInputSearchVar(optFuncExpr, indexSubTree);
            keyVarList.add(inputSearchVariable);
            inputOp = (AbstractLogicalOperator) probeSubTree.root;
        }
        jobGenParams.setKeyVarList(keyVarList);
        // By default, we don't generate SK output.
        boolean outputPrimaryKeysOnlyFromSIdxSearch = true;
        UnnestMapOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType,
                chosenIndex, inputOp, jobGenParams, context, outputPrimaryKeysOnlyFromSIdxSearch, retainInput);

        // If an index-only plan is not possible and there is no false positive results from this secondary index search,
        // then the tryLock on a PK during the secondary index search should be maintained after the primary index look-up
        // to reduce the number of SELECT operations.
        if (!isIndexOnlyPlan && noFalsePositiveResultsFromSIdxSearch) {
            retainInput = true;
        }

        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        ILogicalOperator primaryIndexUnnestOp = AccessMethodUtils.createPrimaryIndexUnnestMap(afterTopOpRefs, topOpRef,
                conditionRef, assignBeforeTopOpRefs, dataSourceScan, dataset, recordType, secondaryIndexUnnestOp,
                context, true, retainInput, retainNull, false, chosenIndex, analysisCtx,
                outputPrimaryKeysOnlyFromSIdxSearch, false, secondaryKeyFieldUsedInSelectCondition,
                secondaryKeyFieldUsedAfterSelectOp, indexSubTree, noFalsePositiveResultsFromSIdxSearch);

        // Replace the datasource scan with the new plan rooted at
        // Get dataSourceRef operator - unnest-map (PK, record)
        if (isIndexOnlyPlan) {
            // Right now, the order of opertors is: union -> select -> assign -> unnest-map
            AbstractLogicalOperator dataSourceRefOp = (AbstractLogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                    .getValue(); // select
            dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
            dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map

            //                indexSubTree.dataSourceRef.setValue(tmpPrimaryIndexUnnestOp);
            indexSubTree.dataSourceRef.setValue(dataSourceRefOp);
        } else if (noFalsePositiveResultsFromSIdxSearch) {
            // Right now, the order of opertors is: union -> select -> split -> assign -> unnest-map
            AbstractLogicalOperator dataSourceRefOp = (AbstractLogicalOperator) primaryIndexUnnestOp.getInputs().get(0)
                    .getValue(); // select
            dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // split
            dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
            dataSourceRefOp = (AbstractLogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
        } else {
            indexSubTree.dataSourceRef.setValue(primaryIndexUnnestOp);
        }

        return primaryIndexUnnestOp;
    }

    /**
     * Returns the variable which acts as the input search key to a secondary
     * index that optimizes optFuncExpr by replacing rewriting indexSubTree
     * (which is the original subtree that will be replaced by the index plan).
     */
    private LogicalVariable getInputSearchVar(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree indexSubTree) {
        if (optFuncExpr.getOperatorSubTree(0) == indexSubTree) {
            // If the index is on a dataset in subtree 0, then subtree 1 will feed.
            return optFuncExpr.getLogicalVar(1);
        } else {
            // If the index is on a dataset in subtree 1, then subtree 0 will feed.
            return optFuncExpr.getLogicalVar(0);
        }
    }

    @Override
    public boolean applySelectPlanTransformation(List<Mutable<ILogicalOperator>> aboveSelectRefs,
            Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context) throws AlgebricksException {

        // index-only plan, a.k.a. TryLock() on PK optimization:
        // If the given secondary index can't generate false-positive results (a super-set of the true result set) and
        // the plan only deals with PK, and/or SK,
        // we can generate an optimized plan that does not require verification for PKs where
        // tryLock() is succeeded.
        // If tryLock() on PK from a secondary index search is succeeded,
        // SK, PK from a secondary index-search will be fed into Union Operators without looking the primary index.
        // If fails, a primary-index lookup will be placed and the results will be fed into Union Operators.
        // So, we need to push-down select and assign (unnest) to the after primary-index lookup.

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
                break;
            }
        }

        if (!functionFound) {
            for (int i = 0; i < secondLevelFuncIdents.size(); i++) {
                if (argFuncIdent == secondLevelFuncIdents.get(i).first) {
                    functionFound = true;
                    noFalsePositiveResultsFromSIdxSearch = secondLevelFuncIdents.get(i).second;
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

                for (int i = 0; i < secondLevelFuncIdents.size(); i++) {
                    if (argFuncIdent == secondLevelFuncIdents.get(i).first) {
                        noFalsePositiveResultsFromSIdxSearch = secondLevelFuncIdents.get(i).second;
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
                isIndexOnlyPlanPossible, secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp,
                verificationAfterSIdxSearchRequired, noFalsePositiveResultsFromSIdxSearch);

        Dataset dataset = subTree.dataset;

        // If a function generates false positive results, then an index-only plan is not possible.
        if (!noFalsePositiveResultsFromSIdxSearch) {
            isIndexOnlyPlanPossible = false;
        } else {
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
            }
        }

        // Since an inverted-index search can't generate the original secondary key field value,
        // an index-only plan is not possible if the original secondary key field value is used after SELECT operator.

        if (isIndexOnlyPlanPossible && !secondaryKeyFieldUsedAfterSelectOp) {
            analysisCtx.setIsIndexOnlyPlan(true);
        } else {
            analysisCtx.setIsIndexOnlyPlan(false);
        }

        SelectOperator selectOp = (SelectOperator) selectRef.getValue();

        IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(chosenIndex, analysisCtx);
        ILogicalOperator indexPlanRootOp = createSecondaryToPrimaryPlan(aboveSelectRefs, selectRef,
                selectOp.getCondition(), subTree.assignsAndUnnestsRefs, subTree, null, chosenIndex, optFuncExpr, false,
                false, false, context, analysisCtx, secondaryKeyFieldUsedInSelectCondition,
                secondaryKeyFieldUsedAfterSelectOp, noFalsePositiveResultsFromSIdxSearch);
        if (indexPlanRootOp == null) {
            return false;
        }

        // Generate new select using the new condition.
        if (conditionRef.getValue() != null) {
            //            select.getInputs().clear();
            if (assignBeforeSelectOp != null) {
                // If a tryLock() on PK optimization is possible, we don't need to use select operator.
                if (analysisCtx.getIsIndexOnlyPlan() && noFalsePositiveResultsFromSIdxSearch
                        && dataset.getDatasetType() == DatasetType.INTERNAL) {
                    // Get dataSourceRef operator - unnest-map (PK, record)
                    // Right now, the order of opertors is: union -> select -> assign -> unnest-map
                    ILogicalOperator dataSourceRefOp = (ILogicalOperator) indexPlanRootOp.getInputs().get(0).getValue(); // select
                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                    dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                    subTree.dataSourceRef.setValue(dataSourceRefOp);
                    selectRef.setValue(indexPlanRootOp);
                } else if (noFalsePositiveResultsFromSIdxSearch && dataset.getDatasetType() == DatasetType.INTERNAL) {
                    selectRef.setValue(indexPlanRootOp);
                } else {
                    subTree.dataSourceRef.setValue(indexPlanRootOp);
                    select.getInputs().clear();
                    select.getInputs().add(new MutableObject<ILogicalOperator>(assignBeforeSelectOp));
                }
            } else {
                // If a tryLock() on PK is possible, we don't need to use select operator.
                if (dataset.getDatasetType() == DatasetType.INTERNAL
                        && (analysisCtx.getIsIndexOnlyPlan() || noFalsePositiveResultsFromSIdxSearch)) {
                    selectRef.setValue(indexPlanRootOp);
                } else {
                    subTree.dataSourceRef.setValue(indexPlanRootOp);
                }
            }
        } else {
            if (assignBeforeSelectOp != null) {
                subTree.dataSourceRef.setValue(indexPlanRootOp);
                select.getInputs().clear();
                select.getInputs().add(new MutableObject<ILogicalOperator>(assignBeforeSelectOp));
                //                selectRef.setValue(assignBeforeSelectOp);
            } else {
                subTree.dataSourceRef.setValue(indexPlanRootOp);
                //                selectRef.setValue(indexPlanRootOp);
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

        IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(chosenIndex, analysisCtx);
        // The arguments of edit-distance-contains() and contains() function are asymmetrical, we can only use index
        // if the dataset of index subtree and the dataset of first argument's subtree is the same
        if ((optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS || optFuncExpr
                .getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS_SUBSTRING)
                && optFuncExpr.getOperatorSubTree(0).dataset != null
                && !optFuncExpr.getOperatorSubTree(0).dataset.getDatasetName().equals(
                        indexSubTree.dataset.getDatasetName())) {
            return false;
        }

        //if LOJ, reset null place holder variable
        LogicalVariable newNullPlaceHolderVar = null;
        if (isLeftOuterJoin && hasGroupBy) {
            //get a new null place holder variable that is the first field variable of the primary key
            //from the indexSubTree's datasourceScanOp
            newNullPlaceHolderVar = indexSubTree.getDataSourceVariables().get(0);

            //reset the null place holder variable
            AccessMethodUtils.resetLOJNullPlaceholderVariableInGroupByOp(analysisCtx, newNullPlaceHolderVar, context);
        }

        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) joinRef.getValue();

        // Check whether assign (unnest) operator exists before the join operator
        Mutable<ILogicalOperator> assignBeforeJoinOpRef = (indexSubTree.assignsAndUnnestsRefs.isEmpty()) ? null
                : indexSubTree.assignsAndUnnestsRefs.get(0);
        ILogicalOperator assignBeforeJoinOp = null;
        if (assignBeforeJoinOpRef != null) {
            assignBeforeJoinOp = assignBeforeJoinOpRef.getValue();
        }

        // index-only plan possible?
        boolean isIndexOnlyPlan = false;

        // secondary key field usage in the join condition
        boolean secondaryKeyFieldUsedInJoinCondition = false;

        // secondary key field usage after the join operator
        boolean secondaryKeyFieldUsedAfterJoinOp = false;

        // whether a verification is required after the secondary index search
        boolean verificationAfterSIdxSearchRequired = false;

        // Can the chosen method generate any false positive results?
        // Currently, for the B+ Tree index, there cannot be any false positive results except the composite index case.
        boolean noFalsePositiveResultsFromSIdxSearch = false;
        // Check whether the given function-call can generate false positive results.
        FunctionIdentifier argFuncIdent = funcExpr.getFunctionIdentifier();
        boolean functionFound = false;
        for (int i = 0; i < funcIdents.size(); i++) {
            if (argFuncIdent == funcIdents.get(i).first) {
                functionFound = true;
                noFalsePositiveResultsFromSIdxSearch = funcIdents.get(i).second;
                break;
            }
        }

        if (!functionFound) {
            for (int i = 0; i < secondLevelFuncIdents.size(); i++) {
                if (argFuncIdent == secondLevelFuncIdents.get(i).first) {
                    functionFound = true;
                    noFalsePositiveResultsFromSIdxSearch = secondLevelFuncIdents.get(i).second;
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

                for (int i = 0; i < secondLevelFuncIdents.size(); i++) {
                    if (argFuncIdent == secondLevelFuncIdents.get(i).first) {
                        noFalsePositiveResultsFromSIdxSearch = secondLevelFuncIdents.get(i).second;
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
                isIndexOnlyPlan, secondaryKeyFieldUsedInJoinCondition, secondaryKeyFieldUsedAfterJoinOp,
                verificationAfterSIdxSearchRequired, noFalsePositiveResultsFromSIdxSearch);

        // If there can be any false positive results, an index-only plan is not possible
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            boolean indexOnlyPlanCheck = AccessMethodUtils.indexOnlyPlanCheck(aboveJoinRefs, joinRef, indexSubTree,
                    probeSubTree, chosenIndex, analysisCtx, context, indexOnlyPlanInfo);

            if (!indexOnlyPlanCheck) {
                return false;
            } else {
                isIndexOnlyPlan = indexOnlyPlanInfo.first;
                secondaryKeyFieldUsedInJoinCondition = indexOnlyPlanInfo.second;
                secondaryKeyFieldUsedAfterJoinOp = indexOnlyPlanInfo.third;
                verificationAfterSIdxSearchRequired = indexOnlyPlanInfo.fourth;
                noFalsePositiveResultsFromSIdxSearch = indexOnlyPlanInfo.fifth;
            }
        } else {
            // We don't consider an index on an external dataset to be an index-only plan.
            isIndexOnlyPlan = false;
        }

        if (isIndexOnlyPlan) {
            analysisCtx.setIsIndexOnlyPlan(true);
        } else {
            analysisCtx.setIsIndexOnlyPlan(false);
        }

        // Clone the original join condition because we may have to modify it (and we also need the original).
        ILogicalExpression joinCond = null;

        // Prepare a panic-case in a join:
        // Remember the original probe subtree, and its primary-key variables,
        // so we can later retrieve the missing attributes via an equi join.
        List<LogicalVariable> originalSubTreePKs = new ArrayList<LogicalVariable>();
        // Remember the primary-keys of the new probe subtree for the top-level equi join.
        List<LogicalVariable> surrogateSubTreePKs = new ArrayList<LogicalVariable>();

        // Remember original live variables from the index sub tree.
        List<LogicalVariable> indexSubTreeLiveVars = new ArrayList<LogicalVariable>();

        Mutable<ILogicalOperator> panicJoinRef = null;
        Map<LogicalVariable, LogicalVariable> panicVarMap = null;
        Mutable<ILogicalOperator> originalProbeSubTreeRootRef = null;
        Mutable<ILogicalOperator> newProbeRootRef = null;

        if (!isIndexOnlyPlan && !noFalsePositiveResultsFromSIdxSearch) {
            // Copy probe subtree, replacing their variables with new ones. We will use the original variables
            // to stitch together a top-level equi join.
            originalProbeSubTreeRootRef = copyAndReinitProbeSubTree(probeSubTree, join.getCondition().getValue(),
                    optFuncExpr, originalSubTreePKs, surrogateSubTreePKs, context);

            VariableUtilities.getLiveVariables(indexSubTree.root, indexSubTreeLiveVars);

            joinCond = join.getCondition().getValue().cloneExpression();

            // Create "panic" (non indexed) nested-loop join path if necessary.
            if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK
                    || optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS_SUBSTRING) {
                panicJoinRef = new MutableObject<ILogicalOperator>(joinRef.getValue());
                panicVarMap = new HashMap<LogicalVariable, LogicalVariable>();
                newProbeRootRef = createPanicNestedLoopJoinPlan(panicJoinRef, indexSubTree, probeSubTree, optFuncExpr,
                        chosenIndex, panicVarMap, context);
                probeSubTree.rootRef.setValue(newProbeRootRef.getValue());
                probeSubTree.root = newProbeRootRef.getValue();
            }
        } else {
            joinCond = join.getCondition().getValue().cloneExpression();
        }

        // Create regular indexed-nested loop join path.
        ILogicalOperator indexPlanRootOp = createSecondaryToPrimaryPlan(aboveJoinRefs, joinRef,
                new MutableObject<ILogicalExpression>(joinCond), indexSubTree.assignsAndUnnestsRefs, indexSubTree,
                probeSubTree, chosenIndex, optFuncExpr, true, isLeftOuterJoin, true, context, analysisCtx,
                secondaryKeyFieldUsedInJoinCondition, secondaryKeyFieldUsedAfterJoinOp,
                noFalsePositiveResultsFromSIdxSearch);
        indexSubTree.dataSourceRef.setValue(indexPlanRootOp);

        if (!isIndexOnlyPlan && !noFalsePositiveResultsFromSIdxSearch) {
            // Change join into a select with the same condition.
            SelectOperator topSelect = new SelectOperator(new MutableObject<ILogicalExpression>(joinCond),
                    isLeftOuterJoin, newNullPlaceHolderVar);
            topSelect.getInputs().add(indexSubTree.rootRef);
            topSelect.setExecutionMode(ExecutionMode.LOCAL);
            context.computeAndSetTypeEnvironmentForOperator(topSelect);
            ILogicalOperator topOp = topSelect;

            // Hook up the indexed-nested loop join path with the "panic" (non indexed) nested-loop join path by putting a union all on top.
            if (panicJoinRef != null) {
                LogicalVariable inputSearchVar = getInputSearchVar(optFuncExpr, indexSubTree);
                indexSubTreeLiveVars.addAll(originalSubTreePKs);
                indexSubTreeLiveVars.add(inputSearchVar);
                List<LogicalVariable> panicPlanLiveVars = new ArrayList<LogicalVariable>();
                VariableUtilities.getLiveVariables(panicJoinRef.getValue(), panicPlanLiveVars);
                // Create variable mapping for union all operator.
                List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap = new ArrayList<Triple<LogicalVariable, LogicalVariable, LogicalVariable>>();
                for (int i = 0; i < indexSubTreeLiveVars.size(); i++) {
                    LogicalVariable indexSubTreeVar = indexSubTreeLiveVars.get(i);
                    LogicalVariable panicPlanVar = panicVarMap.get(indexSubTreeVar);
                    if (panicPlanVar == null) {
                        panicPlanVar = indexSubTreeVar;
                    }
                    varMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(indexSubTreeVar,
                            panicPlanVar, indexSubTreeVar));
                }
                UnionAllOperator unionAllOp = new UnionAllOperator(varMap);
                unionAllOp.getInputs().add(new MutableObject<ILogicalOperator>(topOp));
                unionAllOp.getInputs().add(panicJoinRef);
                unionAllOp.setExecutionMode(ExecutionMode.PARTITIONED);
                context.computeAndSetTypeEnvironmentForOperator(unionAllOp);
                topOp = unionAllOp;
            }

            // Place a top-level equi-join on top to retrieve the missing variables from the original probe subtree.
            // The inner (build) branch of the join is the subtree with the data scan, since the result of the similarity join could potentially be big.
            // This choice may not always be the most efficient, but it seems more robust than the alternative.
            Mutable<ILogicalExpression> eqJoinConditionRef = createPrimaryKeysEqJoinCondition(originalSubTreePKs,
                    surrogateSubTreePKs);
            InnerJoinOperator topEqJoin = new InnerJoinOperator(eqJoinConditionRef, originalProbeSubTreeRootRef,
                    new MutableObject<ILogicalOperator>(topOp));
            topEqJoin.setExecutionMode(ExecutionMode.PARTITIONED);
            joinRef.setValue(topEqJoin);
            context.computeAndSetTypeEnvironmentForOperator(topEqJoin);

            return true;

        } else {
            // For index-only plan:
            if (conditionRef.getValue() != null) {
                if (assignBeforeJoinOp != null) {
                    // If a tryLock() on PK optimization is possible,
                    // the whole plan is changed. replace the current path with the new plan.
                    if (analysisCtx.getIsIndexOnlyPlan()) {
                        // Get the revised dataSourceRef operator - unnest-map (PK, record)
                        // Right now, the order of operators is: union <- select <- assign <- unnest-map (primary index look-up)
                        ILogicalOperator dataSourceRefOp = (ILogicalOperator) indexPlanRootOp.getInputs().get(0)
                                .getValue(); // select
                        dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // assign
                        dataSourceRefOp = (ILogicalOperator) dataSourceRefOp.getInputs().get(0).getValue(); // unnest-map
                        indexSubTree.dataSourceRef.setValue(dataSourceRefOp);
                        // Replace the current operator with the newly created operator
                        joinRef.setValue(indexPlanRootOp);
                    } else if (noFalsePositiveResultsFromSIdxSearch && dataset.getDatasetType() == DatasetType.INTERNAL) {
                        // If there are no false positives, still there can be
                        // Right now, the order of operators is: union <- select <- split <- assign <- unnest-map (primary index look-up) <- [A]
                        //                                       [A] <- stream_project <- stable_sort <- unnest-map (secondary index look-up) <- ...
                        //             or
                        //                                       select <- assign <- unnest-map ...

                        // Case 1: we have UNION
                        if (indexPlanRootOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                            ILogicalOperator dataSourceRefOp = (ILogicalOperator) indexPlanRootOp.getInputs().get(0)
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
                            ILogicalOperator dataSourceRefOp = (ILogicalOperator) indexPlanRootOp.getInputs().get(0)
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
                        joinRef.setValue(indexPlanRootOp);
                    } else {
                        // Index-only optimization and reducing the number of SELECT optimization are not possible.
                        // Right now, the order of operators is: select <- assign <- unnest-map (primary index look-up)
                        joinOp.getInputs().clear();
                        indexSubTree.dataSourceRef.setValue(indexPlanRootOp);
                        joinOp.getInputs().add(new MutableObject<ILogicalOperator>(assignBeforeJoinOp));
                    }
                }

                if (!isIndexOnlyPlan && !noFalsePositiveResultsFromSIdxSearch) {
                    SelectOperator topSelect = new SelectOperator(conditionRef, isLeftOuterJoin, newNullPlaceHolderVar);
                    topSelect.getInputs().add(indexSubTree.rootRef);
                    topSelect.setExecutionMode(ExecutionMode.LOCAL);
                    context.computeAndSetTypeEnvironmentForOperator(topSelect);
                    joinRef.setValue(topSelect);
                }
                //            // Replace the original join with the new subtree rooted at the select op.
            } else {
                if (indexPlanRootOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                    joinRef.setValue(indexPlanRootOp);
                } else {
                    joinRef.setValue(indexSubTree.rootRef.getValue());
                }
            }
        }

        return true;
    }

    /**
     * Copies the probeSubTree (using new variables), and reinitializes the probeSubTree to it.
     * Accordingly replaces the variables in the given joinCond, and the optFuncExpr.
     * Returns a reference to the original plan root.
     */
    private Mutable<ILogicalOperator> copyAndReinitProbeSubTree(OptimizableOperatorSubTree probeSubTree,
            ILogicalExpression joinCond, IOptimizableFuncExpr optFuncExpr, List<LogicalVariable> originalSubTreePKs,
            List<LogicalVariable> surrogateSubTreePKs, IOptimizationContext context) throws AlgebricksException {

        probeSubTree.getPrimaryKeyVars(originalSubTreePKs);

        // Create two copies of the original probe subtree.
        // The first copy, which becomes the new probe subtree, will retain the primary-key and secondary-search key variables,
        // but have all other variables replaced with new ones.
        // The second copy, which will become an input to the top-level equi-join to resolve the surrogates,
        // will have all primary-key and secondary-search keys replaced, but retains all other original variables.

        // Variable replacement map for the first copy.
        Map<LogicalVariable, LogicalVariable> newProbeSubTreeVarMap = new HashMap<LogicalVariable, LogicalVariable>();
        // Variable replacement map for the second copy.
        Map<LogicalVariable, LogicalVariable> joinInputSubTreeVarMap = new HashMap<LogicalVariable, LogicalVariable>();
        // Init with all live vars.
        List<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(probeSubTree.root, liveVars);
        for (LogicalVariable var : liveVars) {
            joinInputSubTreeVarMap.put(var, var);
        }
        // Fill variable replacement maps.
        for (int i = 0; i < optFuncExpr.getNumLogicalVars(); i++) {
            joinInputSubTreeVarMap.put(optFuncExpr.getLogicalVar(i), context.newVar());
            newProbeSubTreeVarMap.put(optFuncExpr.getLogicalVar(i), optFuncExpr.getLogicalVar(i));
        }
        for (int i = 0; i < originalSubTreePKs.size(); i++) {
            LogicalVariable newPKVar = context.newVar();
            surrogateSubTreePKs.add(newPKVar);
            joinInputSubTreeVarMap.put(originalSubTreePKs.get(i), newPKVar);
            newProbeSubTreeVarMap.put(originalSubTreePKs.get(i), originalSubTreePKs.get(i));
        }

        // Create first copy.
        Counter firstCounter = new Counter(context.getVarCounter());
        LogicalOperatorDeepCopyVisitor firstDeepCopyVisitor = new LogicalOperatorDeepCopyVisitor(firstCounter,
                newProbeSubTreeVarMap);
        ILogicalOperator newProbeSubTree = firstDeepCopyVisitor.deepCopy(probeSubTree.root, null);
        inferTypes(newProbeSubTree, context);
        Mutable<ILogicalOperator> newProbeSubTreeRootRef = new MutableObject<ILogicalOperator>(newProbeSubTree);
        context.setVarCounter(firstCounter.get());
        // Create second copy.
        Counter secondCounter = new Counter(context.getVarCounter());
        LogicalOperatorDeepCopyVisitor secondDeepCopyVisitor = new LogicalOperatorDeepCopyVisitor(secondCounter,
                joinInputSubTreeVarMap);
        ILogicalOperator joinInputSubTree = secondDeepCopyVisitor.deepCopy(probeSubTree.root, null);
        inferTypes(joinInputSubTree, context);
        probeSubTree.rootRef.setValue(joinInputSubTree);
        context.setVarCounter(secondCounter.get());

        // Remember the original probe subtree reference so we can return it.
        Mutable<ILogicalOperator> originalProbeSubTreeRootRef = probeSubTree.rootRef;

        // Replace the original probe subtree with its copy.
        Dataset origDataset = probeSubTree.dataset;
        ARecordType origRecordType = probeSubTree.recordType;
        probeSubTree.initFromSubTree(newProbeSubTreeRootRef);
        probeSubTree.dataset = origDataset;
        probeSubTree.recordType = origRecordType;

        // Replace the variables in the join condition based on the mapping of variables
        // in the new probe subtree.
        Map<LogicalVariable, LogicalVariable> varMapping = firstDeepCopyVisitor.getVariableMapping();
        for (Map.Entry<LogicalVariable, LogicalVariable> varMapEntry : varMapping.entrySet()) {
            if (varMapEntry.getKey() != varMapEntry.getValue()) {
                joinCond.substituteVar(varMapEntry.getKey(), varMapEntry.getValue());
            }
        }
        return originalProbeSubTreeRootRef;
    }

    private Mutable<ILogicalExpression> createPrimaryKeysEqJoinCondition(List<LogicalVariable> originalSubTreePKs,
            List<LogicalVariable> surrogateSubTreePKs) {
        List<Mutable<ILogicalExpression>> eqExprs = new ArrayList<Mutable<ILogicalExpression>>();
        int numPKVars = originalSubTreePKs.size();
        for (int i = 0; i < numPKVars; i++) {
            List<Mutable<ILogicalExpression>> args = new ArrayList<Mutable<ILogicalExpression>>();
            args.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(surrogateSubTreePKs.get(i))));
            args.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(originalSubTreePKs.get(i))));
            ILogicalExpression eqFunc = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AlgebricksBuiltinFunctions.EQ), args);
            eqExprs.add(new MutableObject<ILogicalExpression>(eqFunc));
        }
        if (eqExprs.size() == 1) {
            return eqExprs.get(0);
        } else {
            ILogicalExpression andFunc = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AlgebricksBuiltinFunctions.AND), eqExprs);
            return new MutableObject<ILogicalExpression>(andFunc);
        }
    }

    private Mutable<ILogicalOperator> createPanicNestedLoopJoinPlan(Mutable<ILogicalOperator> joinRef,
            OptimizableOperatorSubTree indexSubTree, OptimizableOperatorSubTree probeSubTree,
            IOptimizableFuncExpr optFuncExpr, Index chosenIndex, Map<LogicalVariable, LogicalVariable> panicVarMap,
            IOptimizationContext context) throws AlgebricksException {
        LogicalVariable inputSearchVar = getInputSearchVar(optFuncExpr, indexSubTree);

        // We split the plan into two "branches", and add selections on each side.
        AbstractLogicalOperator replicateOp = new ReplicateOperator(2);
        replicateOp.getInputs().add(new MutableObject<ILogicalOperator>(probeSubTree.root));
        replicateOp.setExecutionMode(ExecutionMode.PARTITIONED);
        context.computeAndSetTypeEnvironmentForOperator(replicateOp);

        // Create select ops for removing tuples that are filterable and not filterable, respectively.
        IVariableTypeEnvironment probeTypeEnv = context.getOutputTypeEnvironment(probeSubTree.root);
        IAType inputSearchVarType;
        if (chosenIndex.isEnforcingKeyFileds())
            inputSearchVarType = optFuncExpr.getFieldType(optFuncExpr.findLogicalVar(inputSearchVar));
        else
            inputSearchVarType = (IAType) probeTypeEnv.getVarType(inputSearchVar);
        Mutable<ILogicalOperator> isFilterableSelectOpRef = new MutableObject<ILogicalOperator>();
        Mutable<ILogicalOperator> isNotFilterableSelectOpRef = new MutableObject<ILogicalOperator>();
        createIsFilterableSelectOps(replicateOp, inputSearchVar, inputSearchVarType, optFuncExpr, chosenIndex, context,
                isFilterableSelectOpRef, isNotFilterableSelectOpRef);

        List<LogicalVariable> originalLiveVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(indexSubTree.root, originalLiveVars);

        // Copy the scan subtree in indexSubTree.
        Counter counter = new Counter(context.getVarCounter());
        LogicalOperatorDeepCopyVisitor deepCopyVisitor = new LogicalOperatorDeepCopyVisitor(counter);
        ILogicalOperator scanSubTree = deepCopyVisitor.deepCopy(indexSubTree.root, null);

        context.setVarCounter(counter.get());
        Map<LogicalVariable, LogicalVariable> copyVarMap = deepCopyVisitor.getVariableMapping();
        panicVarMap.putAll(copyVarMap);

        List<LogicalVariable> copyLiveVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(scanSubTree, copyLiveVars);

        // Replace the inputs of the given join op, and replace variables in its
        // condition since we deep-copied one of the scanner subtrees which
        // changed variables.
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinRef.getValue();
        for (Map.Entry<LogicalVariable, LogicalVariable> entry : copyVarMap.entrySet()) {
            joinOp.getCondition().getValue().substituteVar(entry.getKey(), entry.getValue());
        }
        joinOp.getInputs().clear();
        joinOp.getInputs().add(new MutableObject<ILogicalOperator>(scanSubTree));
        // Make sure that the build input (which may be materialized causing blocking) comes from
        // the split+select, otherwise the plan will have a deadlock.
        joinOp.getInputs().add(isNotFilterableSelectOpRef);
        context.computeAndSetTypeEnvironmentForOperator(joinOp);

        // Return the new root of the probeSubTree.
        return isFilterableSelectOpRef;
    }

    private void createIsFilterableSelectOps(ILogicalOperator inputOp, LogicalVariable inputSearchVar,
            IAType inputSearchVarType, IOptimizableFuncExpr optFuncExpr, Index chosenIndex,
            IOptimizationContext context, Mutable<ILogicalOperator> isFilterableSelectOpRef,
            Mutable<ILogicalOperator> isNotFilterableSelectOpRef) throws AlgebricksException {
        // Create select operator for removing tuples that are not filterable.
        // First determine the proper filter function and args based on the type of the input search var.
        ILogicalExpression isFilterableExpr = null;
        switch (inputSearchVarType.getTypeTag()) {
            case STRING: {
                List<Mutable<ILogicalExpression>> isFilterableArgs = new ArrayList<Mutable<ILogicalExpression>>(4);
                isFilterableArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                        inputSearchVar)));
                // Since we are optimizing a join, the similarity threshold should be the only constant in the optimizable function expression.
                isFilterableArgs.add(new MutableObject<ILogicalExpression>(optFuncExpr.getConstantAtRuntimeExpr(0)));
                isFilterableArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils
                        .createInt32Constant(chosenIndex.getGramLength())));
                boolean usePrePost = optFuncExpr.containsPartialField() ? false : true;
                isFilterableArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils
                        .createBooleanConstant(usePrePost)));
                isFilterableExpr = new ScalarFunctionCallExpression(
                        FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.EDIT_DISTANCE_STRING_IS_FILTERABLE),
                        isFilterableArgs);
                break;
            }
            case UNORDEREDLIST:
            case ORDEREDLIST: {
                List<Mutable<ILogicalExpression>> isFilterableArgs = new ArrayList<Mutable<ILogicalExpression>>(2);
                isFilterableArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                        inputSearchVar)));
                // Since we are optimizing a join, the similarity threshold should be the only constant in the optimizable function expression.
                isFilterableArgs.add(new MutableObject<ILogicalExpression>(optFuncExpr.getConstantAtRuntimeExpr(0)));
                isFilterableExpr = new ScalarFunctionCallExpression(
                        FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.EDIT_DISTANCE_LIST_IS_FILTERABLE),
                        isFilterableArgs);
                break;
            }
            default: {
                throw new AlgebricksException("Only strings, ordered and unordered list types supported.");
            }
        }

        SelectOperator isFilterableSelectOp = new SelectOperator(
                new MutableObject<ILogicalExpression>(isFilterableExpr), false, null);
        isFilterableSelectOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        isFilterableSelectOp.setExecutionMode(ExecutionMode.LOCAL);
        context.computeAndSetTypeEnvironmentForOperator(isFilterableSelectOp);

        // Select operator for removing tuples that are filterable.
        List<Mutable<ILogicalExpression>> isNotFilterableArgs = new ArrayList<Mutable<ILogicalExpression>>();
        isNotFilterableArgs.add(new MutableObject<ILogicalExpression>(isFilterableExpr));
        ILogicalExpression isNotFilterableExpr = new ScalarFunctionCallExpression(
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.NOT), isNotFilterableArgs);
        SelectOperator isNotFilterableSelectOp = new SelectOperator(new MutableObject<ILogicalExpression>(
                isNotFilterableExpr), false, null);
        isNotFilterableSelectOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        isNotFilterableSelectOp.setExecutionMode(ExecutionMode.LOCAL);
        context.computeAndSetTypeEnvironmentForOperator(isNotFilterableSelectOp);

        isFilterableSelectOpRef.setValue(isFilterableSelectOp);
        isNotFilterableSelectOpRef.setValue(isNotFilterableSelectOp);
    }

    private void addSearchKeyType(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree indexSubTree,
            IOptimizationContext context, InvertedIndexJobGenParams jobGenParams) throws AlgebricksException {
        // If we have two variables in the optFunxExpr, then we are optimizing a join.
        IAType type = null;
        ATypeTag typeTag = null;
        if (optFuncExpr.getNumLogicalVars() == 2) {
            // Find the type of the variable that is going to feed into the index search.
            if (optFuncExpr.getOperatorSubTree(0) == indexSubTree) {
                // If the index is on a dataset in subtree 0, then subtree 1 will feed.
                type = optFuncExpr.getFieldType(1);
            } else {
                // If the index is on a dataset in subtree 1, then subtree 0 will feed.
                type = optFuncExpr.getFieldType(0);
            }
            typeTag = type.getTypeTag();
        } else {
            // We are optimizing a selection query. Add the type of the search key constant.
            type = optFuncExpr.getConstantType(0);
            typeTag = type.getTypeTag();
            if (typeTag != ATypeTag.ORDEREDLIST && typeTag != ATypeTag.STRING && typeTag != ATypeTag.UNORDEREDLIST) {
                throw new AlgebricksException("Only ordered lists, string, and unordered lists types supported.");
            }
        }
        jobGenParams.setSearchKeyType(typeTag);
    }

    private void addFunctionSpecificArgs(IOptimizableFuncExpr optFuncExpr, InvertedIndexJobGenParams jobGenParams) {
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS_SUBSTRING) {
            jobGenParams.setSearchModifierType(SearchModifierType.CONJUNCTIVE);
            jobGenParams.setSimilarityThreshold(new AsterixConstantValue(ANull.NULL));
        }
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AlgebricksBuiltinFunctions.CONTAINS) {
            jobGenParams.setSearchModifierType(SearchModifierType.DISJUNCTIVE);
            //            jobGenParams.setSearchModifierType(SearchModifierType.CONJUNCTIVE);
            jobGenParams.setSimilarityThreshold(new AsterixConstantValue(ANull.NULL));
        }
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK) {
            jobGenParams.setSearchModifierType(SearchModifierType.JACCARD);
            // Add the similarity threshold which, by convention, is the last constant value.
            jobGenParams.setSimilarityThreshold(((ConstantExpression) optFuncExpr.getConstantAtRuntimeExpr(optFuncExpr
                    .getNumConstantAtRuntimeExpr() - 1)).getValue());
        }
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK
                || optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS_SUBSTRING) {
            if (optFuncExpr.containsPartialField()) {
                jobGenParams.setSearchModifierType(SearchModifierType.CONJUNCTIVE_EDIT_DISTANCE);
            } else {
                jobGenParams.setSearchModifierType(SearchModifierType.EDIT_DISTANCE);
            }
            // Add the similarity threshold which, by convention, is the last constant value.
            jobGenParams.setSimilarityThreshold(((ConstantExpression) optFuncExpr.getConstantAtRuntimeExpr(optFuncExpr
                    .getNumConstantAtRuntimeExpr() - 1)).getValue());
        }
    }

    private void addKeyVarsAndExprs(IOptimizableFuncExpr optFuncExpr, ArrayList<LogicalVariable> keyVarList,
            ArrayList<Mutable<ILogicalExpression>> keyExprList, IOptimizationContext context)
            throws AlgebricksException {
        // For now we are assuming a single secondary index key.
        // Add a variable and its expr to the lists which will be passed into an assign op.
        LogicalVariable keyVar = context.newVar();
        keyVarList.add(keyVar);
        keyExprList.add(new MutableObject<ILogicalExpression>(optFuncExpr.getConstantAtRuntimeExpr(0)));
        return;
    }

    @Override
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) throws AlgebricksException {
        if (optFuncExpr.getFuncExpr().getAnnotations()
                .containsKey(SkipSecondaryIndexSearchExpressionAnnotation.INSTANCE)) {
            return false;
        }

        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK
                || optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS_SUBSTRING) {
            return isEditDistanceFuncOptimizable(index, optFuncExpr);
        }

        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK) {
            return isJaccardFuncOptimizable(index, optFuncExpr);
        }

        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS_SUBSTRING) {
            return isContainsSubstringFuncOptimizable(index, optFuncExpr);
        }

        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AlgebricksBuiltinFunctions.CONTAINS) {
            return isContainsFuncOptimizable(index, optFuncExpr);
        }

        return false;
    }

    private boolean isEditDistanceFuncOptimizable(Index index, IOptimizableFuncExpr optFuncExpr)
            throws AlgebricksException {
        if (optFuncExpr.getNumConstantAtRuntimeExpr() == 1) {
            return isEditDistanceFuncJoinOptimizable(index, optFuncExpr);
        } else {
            return isEditDistanceFuncSelectOptimizable(index, optFuncExpr);
        }
    }

    private boolean isEditDistanceFuncJoinOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        if (index.isEnforcingKeyFileds())
            return isEditDistanceFuncCompatible(index.getKeyFieldTypes().get(0).getTypeTag(), index.getIndexType());
        else
            return isEditDistanceFuncCompatible(optFuncExpr.getFieldType(0).getTypeTag(), index.getIndexType());
    }

    private boolean isEditDistanceFuncCompatible(ATypeTag typeTag, IndexType indexType) {
        // We can only optimize edit distance on strings using an ngram index.
        if (typeTag == ATypeTag.STRING
                && (indexType == IndexType.SINGLE_PARTITION_NGRAM_INVIX || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX)) {
            return true;
        }
        // We can only optimize edit distance on lists using a word index.
        if ((typeTag == ATypeTag.ORDEREDLIST)
                && (indexType == IndexType.SINGLE_PARTITION_WORD_INVIX || indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX)) {
            return true;
        }
        return false;
    }

    private boolean isEditDistanceFuncSelectOptimizable(Index index, IOptimizableFuncExpr optFuncExpr)
            throws AlgebricksException {

        // Check for panic in selection query.
        // TODO: Panic also depends on prePost which is currently hardcoded to be true.
        AsterixConstantValue listOrStrConstVal = (AsterixConstantValue) ((ConstantExpression) optFuncExpr
                .getConstantAtRuntimeExpr(0)).getValue();
        IAObject listOrStrObj = listOrStrConstVal.getObject();
        ATypeTag typeTag = listOrStrObj.getType().getTypeTag();

        if (!isEditDistanceFuncCompatible(typeTag, index.getIndexType())) {
            return false;
        }

        AsterixConstantValue intConstVal = (AsterixConstantValue) ((ConstantExpression) optFuncExpr
                .getConstantAtRuntimeExpr(1)).getValue();
        IAObject intObj = intConstVal.getObject();

        AInt32 edThresh = null;
        // Apply type casting based on numeric types of the input to INT32 type.
        try {
            edThresh = (AInt32) ATypeHierarchy.convertNumericTypeObject(intObj, ATypeTag.INT32);
        } catch (AsterixException e) {
            throw new AlgebricksException(e);
        }
        int mergeThreshold = 0;

        if (typeTag == ATypeTag.STRING) {
            AString astr = (AString) listOrStrObj;
            // Compute merge threshold depending on the query grams contain pre- and postfixing
            if (optFuncExpr.containsPartialField()) {
                mergeThreshold = (astr.getStringValue().length() - index.getGramLength() + 1)
                        - edThresh.getIntegerValue() * index.getGramLength();
            } else {
                mergeThreshold = (astr.getStringValue().length() + index.getGramLength() - 1)
                        - edThresh.getIntegerValue() * index.getGramLength();
            }
        }

        if ((typeTag == ATypeTag.ORDEREDLIST)
                && (index.getIndexType() == IndexType.SINGLE_PARTITION_WORD_INVIX || index.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX)) {
            IACollection alist = (IACollection) listOrStrObj;
            // Compute merge threshold.
            mergeThreshold = alist.size() - edThresh.getIntegerValue();
        }
        if (mergeThreshold <= 0) {
            // We cannot use index to optimize expr.
            return false;
        }
        return true;
    }

    private boolean isJaccardFuncOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        //TODO we need to split join and select cases in order to check join case more thoroughly.

        int variableCount = optFuncExpr.getNumLogicalVars();

        //check whether gram-tokens function is optimizable
        ScalarFunctionCallExpression funcExpr = null;
        for (int i = 0; i < variableCount; i++) {
            funcExpr = findTokensFunc(AsterixBuiltinFunctions.GRAM_TOKENS, optFuncExpr, i);
            if (funcExpr != null) {
                return isJaccardFuncCompatible(funcExpr, optFuncExpr.getFieldType(i).getTypeTag(), index.getIndexType());
            }
        }

        //check whether word-tokens function is optimizable
        for (int i = 0; i < variableCount; i++) {
            funcExpr = findTokensFunc(AsterixBuiltinFunctions.WORD_TOKENS, optFuncExpr, i);
            if (funcExpr != null) {
                return isJaccardFuncCompatible(funcExpr, optFuncExpr.getFieldType(i).getTypeTag(), index.getIndexType());
            }
        }

        //check whether a search variable is optimizable
        OptimizableOperatorSubTree subTree = null;
        LogicalVariable targetVar = null;
        for (int i = 0; i < variableCount; i++) {
            subTree = optFuncExpr.getOperatorSubTree(i);
            if (subTree == null)
                continue;
            targetVar = optFuncExpr.getLogicalVar(i);
            if (targetVar == null)
                continue;
            return isJaccardFuncCompatible(optFuncExpr.getFuncExpr().getArguments().get(i).getValue(), optFuncExpr
                    .getFieldType(i).getTypeTag(), index.getIndexType());
        }

        return false;
    }

    private ScalarFunctionCallExpression findTokensFunc(FunctionIdentifier funcId, IOptimizableFuncExpr optFuncExpr,
            int subTreeIndex) {
        //find either a gram-tokens or a word-tokens function that exists in optFuncExpr.subTrees' assignsAndUnnests
        OptimizableOperatorSubTree subTree = null;
        LogicalVariable targetVar = null;

        subTree = optFuncExpr.getOperatorSubTree(subTreeIndex);
        if (subTree == null) {
            return null;
        }

        targetVar = optFuncExpr.getLogicalVar(subTreeIndex);
        if (targetVar == null) {
            return null;
        }

        for (AbstractLogicalOperator op : subTree.assignsAndUnnests) {
            if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN)
                continue;
            List<Mutable<ILogicalExpression>> exprList = ((AssignOperator) op).getExpressions();
            for (Mutable<ILogicalExpression> expr : exprList) {
                if (expr.getValue().getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL)
                    continue;
                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr.getValue();
                if (funcExpr.getFunctionIdentifier() != funcId)
                    continue;
                ILogicalExpression varExpr = funcExpr.getArguments().get(0).getValue();
                if (varExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE)
                    continue;
                if (((VariableReferenceExpression) varExpr).getVariableReference() == targetVar)
                    continue;
                return (ScalarFunctionCallExpression) funcExpr;
            }
        }
        return null;
    }

    private boolean isJaccardFuncCompatible(ILogicalExpression nonConstArg, ATypeTag typeTag, IndexType indexType) {
        if (nonConstArg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression nonConstfuncExpr = (AbstractFunctionCallExpression) nonConstArg;
            // We can use this index if the tokenization function matches the index type.
            if (nonConstfuncExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.WORD_TOKENS
                    && (indexType == IndexType.SINGLE_PARTITION_WORD_INVIX || indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX)) {
                return true;
            }
            if (nonConstfuncExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.GRAM_TOKENS
                    && (indexType == IndexType.SINGLE_PARTITION_NGRAM_INVIX || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX)) {
                return true;
            }
        }

        if (nonConstArg.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            if ((typeTag == ATypeTag.ORDEREDLIST || typeTag == ATypeTag.UNORDEREDLIST)
                    && (indexType == IndexType.SINGLE_PARTITION_WORD_INVIX || indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX)) {
                return true;
            }
            // We assume that the given list variable doesn't have ngram list in it since it is unrealistic.
        }
        return false;
    }

    private boolean isContainsSubstringFuncOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        if (optFuncExpr.getNumLogicalVars() == 2) {
            return isContainsSubstringFuncJoinOptimizable(index, optFuncExpr);
        } else {
            return isContainsSubstringFuncSelectOptimizable(index, optFuncExpr);
        }
    }

    // Does full-text search can utilize the given index?
    private boolean isContainsFuncOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        if (optFuncExpr.getNumLogicalVars() == 2) {
            return isContainsFuncJoinOptimizable(index, optFuncExpr);
        } else {
            return isContainsFuncSelectOptimizable(index, optFuncExpr);
        }
    }

    private boolean isContainsSubstringFuncSelectOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        AsterixConstantValue strConstVal = (AsterixConstantValue) ((ConstantExpression) optFuncExpr
                .getConstantAtRuntimeExpr(0)).getValue();
        IAObject strObj = strConstVal.getObject();
        ATypeTag typeTag = strObj.getType().getTypeTag();

        if (!isContainsSubstringFuncCompatible(typeTag, index.getIndexType())) {
            return false;
        }

        // Check that the constant search string has at least gramLength characters.
        if (strObj.getType().getTypeTag() == ATypeTag.STRING) {
            AString astr = (AString) strObj;
            if (astr.getStringValue().length() >= index.getGramLength()) {
                return true;
            }
        }
        return false;
    }

    // For full-text search
    private boolean isContainsFuncSelectOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        AsterixConstantValue strConstVal = (AsterixConstantValue) ((ConstantExpression) optFuncExpr
                .getConstantAtRuntimeExpr(0)).getValue();
        IAObject strObj = strConstVal.getObject();
        ATypeTag typeTag = strObj.getType().getTypeTag();

        if (!isContainsFuncCompatible(typeTag, index.getIndexType())) {
            return false;
        }

        // Check that the constant search string has at least gramLength characters.
        if (strObj.getType().getTypeTag() == ATypeTag.STRING) {
            AString astr = (AString) strObj;
            if (astr.getStringValue().length() >= index.getGramLength()) {
                return true;
            }
        }
        return false;
    }

    private boolean isContainsSubstringFuncJoinOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        if (index.isEnforcingKeyFileds())
            return isContainsSubstringFuncCompatible(index.getKeyFieldTypes().get(0).getTypeTag(), index.getIndexType());
        else
            return isContainsSubstringFuncCompatible(optFuncExpr.getFieldType(0).getTypeTag(), index.getIndexType());
    }

    private boolean isContainsFuncJoinOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        if (index.isEnforcingKeyFileds())
            return isContainsFuncCompatible(index.getKeyFieldTypes().get(0).getTypeTag(), index.getIndexType());
        else
            return isContainsFuncCompatible(optFuncExpr.getFieldType(0).getTypeTag(), index.getIndexType());
    }

    private boolean isContainsSubstringFuncCompatible(ATypeTag typeTag, IndexType indexType) {
        //We can only optimize contains-substring with ngram indexes.
        if ((typeTag == ATypeTag.STRING)
                && (indexType == IndexType.SINGLE_PARTITION_NGRAM_INVIX || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX)) {
            return true;
        }
        return false;
    }

    private boolean isContainsFuncCompatible(ATypeTag typeTag, IndexType indexType) {
        //We can only optimize contains with full-text indexes.
        if ((typeTag == ATypeTag.STRING) && (indexType == IndexType.SINGLE_PARTITION_WORD_INVIX)) {
            return true;
        }
        return false;
    }

    public static IBinaryTokenizerFactory getBinaryTokenizerFactory(SearchModifierType searchModifierType,
            ATypeTag searchKeyType, Index index) throws AlgebricksException {
        switch (index.getIndexType()) {
            case SINGLE_PARTITION_WORD_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX: {
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getWordTokenizerFactory(searchKeyType, false, false);
            }
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX: {
                // Make sure not to use pre- and postfixing for conjunctive searches.
                boolean prePost = (searchModifierType == SearchModifierType.CONJUNCTIVE || searchModifierType == SearchModifierType.CONJUNCTIVE_EDIT_DISTANCE) ? false
                        : true;
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getNGramTokenizerFactory(searchKeyType,
                        index.getGramLength(), prePost, false);
            }
            default: {
                throw new AlgebricksException("Tokenizer not applicable to index kind '" + index.getIndexType() + "'.");
            }
        }
    }

    public static IInvertedIndexSearchModifierFactory getSearchModifierFactory(SearchModifierType searchModifierType,
            IAObject simThresh, Index index) throws AlgebricksException {
        switch (searchModifierType) {
            case CONJUNCTIVE: {
                return new ConjunctiveSearchModifierFactory();
            }
            case DISJUNCTIVE: {
                return new DisjunctiveSearchModifierFactory();
            }
            case JACCARD: {
                float jaccThresh = ((AFloat) simThresh).getFloatValue();
                return new JaccardSearchModifierFactory(jaccThresh);
            }
            case EDIT_DISTANCE:
            case CONJUNCTIVE_EDIT_DISTANCE: {
                int edThresh = 0;
                try {
                    edThresh = ((AInt32) ATypeHierarchy.convertNumericTypeObject(simThresh, ATypeTag.INT32))
                            .getIntegerValue();
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }

                switch (index.getIndexType()) {
                    case SINGLE_PARTITION_NGRAM_INVIX:
                    case LENGTH_PARTITIONED_NGRAM_INVIX: {
                        // Edit distance on strings, filtered with overlapping grams.
                        if (searchModifierType == SearchModifierType.EDIT_DISTANCE) {
                            return new EditDistanceSearchModifierFactory(index.getGramLength(), edThresh);
                        } else {
                            return new ConjunctiveEditDistanceSearchModifierFactory(index.getGramLength(), edThresh);
                        }
                    }
                    case SINGLE_PARTITION_WORD_INVIX:
                    case LENGTH_PARTITIONED_WORD_INVIX: {
                        // Edit distance on two lists. The list-elements are non-overlapping.
                        if (searchModifierType == SearchModifierType.EDIT_DISTANCE) {
                            return new ListEditDistanceSearchModifierFactory(edThresh);
                        } else {
                            return new ConjunctiveListEditDistanceSearchModifierFactory(edThresh);
                        }
                    }
                    default: {
                        throw new AlgebricksException("Incompatible search modifier '" + searchModifierType
                                + "' for index type '" + index.getIndexType() + "'");
                    }
                }
            }
            default: {
                throw new AlgebricksException("Unknown search modifier type '" + searchModifierType + "'.");
            }
        }
    }

    private void inferTypes(ILogicalOperator op, IOptimizationContext context) throws AlgebricksException {
        for (Mutable<ILogicalOperator> childOpRef : op.getInputs()) {
            inferTypes(childOpRef.getValue(), context);
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
    }
}
