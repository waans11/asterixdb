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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * This rule optimizes a join with secondary indexes into an indexed nested-loop join.
 * Matches the following operator pattern:
 * (join) <-- (select)? <-- (assign | unnest)+ <-- (datasource scan)
 * <-- (select)? <-- (assign | unnest)+ <-- (datasource scan | unnest-map)
 * The order of the join inputs matters (left becomes the outer relation and right becomes the inner relation).
 * This rule tries to utilize an index on the inner relation.
 * If that's not possible, it stops transforming the given join into an index-nested-loop join.
 * Replaces the above pattern with the following simplified plan:
 * (select) <-- (assign) <-- (btree search) <-- (sort) <-- (unnest(index search)) <-- (assign) <-- (datasource scan | unnest-map)
 * The sort is optional, and some access methods may choose not to sort.
 * Note that for some index-based optimizations we do not remove the triggering
 * condition from the join, since the secondary index may only act as a filter, and the
 * final verification must still be done with the original join condition.
 * The basic outline of this rule is:
 * 1. Match operator pattern.
 * 2. Analyze join condition to see if there are optimizable functions (delegated to IAccessMethods).
 * 3. Check metadata to see if there are applicable indexes.
 * 4. Choose an index to apply (for now only a single index will be chosen).
 * 5. Rewrite plan using index (delegated to IAccessMethods).
 * For left-outer-join, additional patterns are checked and additional treatment is needed as follows:
 * 1. First it checks if there is a groupByOp above the join: (groupby) <-- (leftouterjoin)
 * 2. Inherently, only the right-subtree of the lojOp can be used as indexSubtree.
 * So, the right-subtree must have at least one applicable index on join field(s)
 * 3. If there is a groupByOp, the null placeholder variable introduced in groupByOp should be taken care of correctly.
 * Here, the primary key variable from datasourceScanOp replaces the introduced null placeholder variable.
 * If the primary key is composite key, then the first variable of the primary key variables becomes the
 * null place holder variable. This null placeholder variable works for all three types of indexes.
 */
public class IntroduceJoinAccessMethodRule extends AbstractIntroduceAccessMethodRule {

    protected Mutable<ILogicalOperator> joinRef = null;
    protected AbstractBinaryJoinOperator join = null;
    protected AbstractFunctionCallExpression joinCond = null;
    protected final OptimizableOperatorSubTree leftSubTree = new OptimizableOperatorSubTree();
    protected final OptimizableOperatorSubTree rightSubTree = new OptimizableOperatorSubTree();
    protected IVariableTypeEnvironment typeEnvironment = null;
    protected boolean isLeftOuterJoin = false;
    protected boolean hasGroupBy = true;

    // Register access methods.
    protected static Map<FunctionIdentifier, List<IAccessMethod>> accessMethods = new HashMap<FunctionIdentifier, List<IAccessMethod>>();

    static {
        registerAccessMethod(BTreeAccessMethod.INSTANCE, accessMethods);
        registerAccessMethod(RTreeAccessMethod.INSTANCE, accessMethods);
        registerAccessMethod(InvertedIndexAccessMethod.INSTANCE, accessMethods);
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        clear();
        setMetadataDeclarations(context);

        // Match operator pattern and initialize optimizable sub trees.
        if (!matchesOperatorPattern(opRef, context)) {
            return false;
        }
        // Analyze condition on those optimizable subtrees that have a datasource scan.
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new HashMap<IAccessMethod, AccessMethodAnalysisContext>();
        boolean matchInLeftSubTree = false;
        boolean matchInRightSubTree = false;
        if (leftSubTree.hasDataSource()) {
            matchInLeftSubTree = analyzeCondition(joinCond, leftSubTree.getAssignsAndUnnests(), analyzedAMs, context,
                    typeEnvironment);
        }
        if (rightSubTree.hasDataSource()) {
            matchInRightSubTree = analyzeCondition(joinCond, rightSubTree.getAssignsAndUnnests(), analyzedAMs, context,
                    typeEnvironment);
        }
        if (!matchInLeftSubTree && !matchInRightSubTree) {
            return false;
        }

        // Set dataset and type metadata.
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        boolean checkLeftSubTreeMetadata = false;
        boolean checkRightSubTreeMetadata = false;
        if (matchInLeftSubTree) {
            checkLeftSubTreeMetadata = leftSubTree.setDatasetAndTypeMetadata(metadataProvider);
        }
        if (matchInRightSubTree) {
            checkRightSubTreeMetadata = rightSubTree.setDatasetAndTypeMetadata(metadataProvider);
        }
        if (!checkLeftSubTreeMetadata && !checkRightSubTreeMetadata) {
            return false;
        }
        if (checkLeftSubTreeMetadata) {
            fillSubTreeIndexExprs(leftSubTree, analyzedAMs, context);
        }
        if (checkRightSubTreeMetadata) {
            fillSubTreeIndexExprs(rightSubTree, analyzedAMs, context);
        }
        pruneIndexCandidates(analyzedAMs, context, typeEnvironment);

        // We only consider indexes from the inner branch (right subTree).
        // If no index is available, then we stop this optimization.
        removeIndexCandidatesFromOuterBranch(analyzedAMs);

        // Choose an index from the inner branch that will be used.
        Pair<IAccessMethod, Index> chosenIndex = chooseBestIndex(analyzedAMs);
        if (chosenIndex == null) {
            context.addToDontApplySet(this, join);
            return false;
        }

        // Apply plan transformation using chosen index.
        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(chosenIndex.first);

        //For LOJ with GroupBy, prepare objects to reset LOJ nullPlaceHolderVariable in GroupByOp
        if (isLeftOuterJoin && hasGroupBy) {
            analysisCtx.setLOJGroupbyOpRef(opRef);
            ScalarFunctionCallExpression isNullFuncExpr = AccessMethodUtils
                    .findLOJIsMissingFuncInGroupBy((GroupByOperator) opRef.getValue());
            analysisCtx.setLOJIsNullFuncInGroupBy(isNullFuncExpr);
        }

        // At this point, we are sure that only an index from the inner branch is going to be used.
        // So, the left subtree is the outer branch and the right subtree is the inner branch.
        boolean res = chosenIndex.first.applyJoinPlanTransformation(joinRef, leftSubTree, rightSubTree,
                chosenIndex.second, analysisCtx, context, isLeftOuterJoin, hasGroupBy);
        if (res) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }
        context.addToDontApplySet(this, join);
        return res;
    }

    /**
     * Removes indexes from the optimizer's consideration for this rule.
     */
    protected void removeIndexCandidatesFromOuterBranch(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        String innerDataset = null;
        if (rightSubTree.getDataset() != null) {
            innerDataset = rightSubTree.getDataset().getDatasetName();
        }

        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> entry = amIt.next();
            AccessMethodAnalysisContext amCtx = entry.getValue();

            // Fetch index, expression, and variables.
            Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt = amCtx.indexExprsAndVars.entrySet()
                    .iterator();

            while (indexIt.hasNext()) {
                Map.Entry<Index, List<Pair<Integer, Integer>>> indexExprAndVarEntry = indexIt.next();
                Iterator<Pair<Integer, Integer>> exprsAndVarIter = indexExprAndVarEntry.getValue().iterator();
                boolean indexFromInnerBranch = false;

                while (exprsAndVarIter.hasNext()) {
                    Pair<Integer, Integer> exprAndVarIdx = exprsAndVarIter.next();
                    IOptimizableFuncExpr optFuncExpr = amCtx.matchedFuncExprs.get(exprAndVarIdx.first);

                    // Does this index come from the inner branch?
                    // We check the dataset name and the subtree to make sure the index is applicable.
                    if (indexExprAndVarEntry.getKey().getDatasetName().equals(innerDataset)) {
                        if (optFuncExpr.getOperatorSubTree(exprAndVarIdx.second).equals(rightSubTree)) {
                            indexFromInnerBranch = true;
                        }
                    }
                }

                // If the index does not come from the inner branch,
                // We do not consider this index.
                if (!indexFromInnerBranch) {
                    indexIt.remove();
                }
            }
        }
    }

    protected boolean matchesOperatorPattern(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // First check that the operator is a join and its condition is a function call.
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }

        boolean isInnerJoin = isInnerJoin(op1);
        isLeftOuterJoin = isLeftOuterJoin(op1);

        if (!isInnerJoin && !isLeftOuterJoin) {
            return false;
        }

        // Set and analyze select.
        if (isInnerJoin) {
            joinRef = opRef;
            join = (InnerJoinOperator) op1;
        } else {
            joinRef = op1.getInputs().get(0);
            join = (LeftOuterJoinOperator) joinRef.getValue();
        }

        typeEnvironment = context.getOutputTypeEnvironment(join);
        // Check that the select's condition is a function call.
        ILogicalExpression condExpr = join.getCondition().getValue();
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        joinCond = (AbstractFunctionCallExpression) condExpr;
        boolean leftSubTreeInitialized = leftSubTree.initFromSubTree(join.getInputs().get(0));
        boolean rightSubTreeInitialized = rightSubTree.initFromSubTree(join.getInputs().get(1));
        if (!leftSubTreeInitialized || !rightSubTreeInitialized) {
            return false;
        }

        // One of the subtrees must have a datasource scan.
        if (leftSubTree.hasDataSourceScan() || rightSubTree.hasDataSourceScan()) {
            return true;
        }
        return false;
    }

    private boolean isLeftOuterJoin(AbstractLogicalOperator op1) {
        if (op1.getInputs().size() != 1) {
            return false;
        }
        if (((AbstractLogicalOperator) op1.getInputs().get(0).getValue())
                .getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        if (op1.getOperatorTag() == LogicalOperatorTag.GROUP) {
            return true;
        }
        hasGroupBy = false;
        return true;
    }

    private boolean isInnerJoin(AbstractLogicalOperator op1) {
        return op1.getOperatorTag() == LogicalOperatorTag.INNERJOIN;
    }

    @Override
    public Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods() {
        return accessMethods;
    }

    private void clear() {
        joinRef = null;
        join = null;
        joinCond = null;
        isLeftOuterJoin = false;
    }
}
