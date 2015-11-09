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
import java.util.List;
import java.util.Map;

import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.canDecreaseCardinalityCode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.canPreserveOrderCode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * This rule optimizes a join with secondary indexes into an indexed nested-loop join.
 * Matches the following operator pattern:
 * (join) <-- (limit)* <-- (order)* <-- (select)? <-- (assign | unnest)+ <-- (datasource scan)
 * <-- (limit)* <-- (order)* <-- (select)? <-- (assign | unnest)+ <-- (datasource scan | unnest-map)
 * The order of the join inputs matters (left-outer relation, right-inner relation).
 * This rule tries to utilize an index on the inner relation first.
 * If that's not possible, it tries to use an index on the outer relation.
 * Replaces the above pattern with the following simplified plan:
 * (limit)* <-- (order)* <-- (select) <-- (assign) <-- (btree search) <-- (sort) <-- (unnest(index search)) <-- (assign) <-- (datasource scan | unnest-map)
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
 * Also, if there is a LIMIT and if an index-search in an index-nested-loop join can generate trustworthy tuples,
 * we can parameterize the LIMIT information (pass LIMIT to index-search) if:
 * 1) The given join plan should be transformed into an index-nested-loop join plan.
 * 2) A secondary index search or a primary index search of INNER branch can cover the given condition in the given join operator.
 * = no more verification is required after an index search assuming that we can get a trustworthy tuple from that index search.
 * 3) For Left-outer join, the LIMIT information is applied in the OUTER branch.
 * For inner join, the LIMIT information is applied in the INNER branch.
 * 4) If an order by is used, it should be ascending order.
 * 4-1) Also, the attribute order in the given "order by" should be consistent with the attribute order in the first index search
 * or data scan of OUTER branch.
 * However, the OUTER branch should generate tuples in the sorted order (index-only plan and reducing the number of verification
 * plan are not applicable because of UNION operator in those plans in outer branch.)
 */
public class IntroduceJoinAccessMethodRule extends AbstractIntroduceAccessMethodRule {

    protected Mutable<ILogicalOperator> joinRef = null;
    protected AbstractBinaryJoinOperator joinOp = null;
    protected AbstractFunctionCallExpression joinCond = null;
    protected final OptimizableOperatorSubTree leftSubTree = new OptimizableOperatorSubTree();
    protected final OptimizableOperatorSubTree rightSubTree = new OptimizableOperatorSubTree();
    protected IVariableTypeEnvironment typeEnvironment = null;
    protected boolean isLeftOuterJoin = false;
    protected boolean hasGroupBy = true;
    protected IOptimizationContext context = null;
    protected List<Mutable<ILogicalOperator>> afterJoinRefs = null;

    // used for pushing-down LIMIT
    protected long limitNumberOfResult = -1;
    protected boolean canPushDownLimit = false;
    List<Pair<IOrder, Mutable<ILogicalExpression>>> orderByExpressions = null;

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
        this.context = context;

        // Check whether this operator is the root, which is DISTRIBUTE_RESULT
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        // Begin from the root operator - DISTRIBUTE_RESULT or SINK
        if (op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
            if (op.getOperatorTag() != LogicalOperatorTag.SINK) {
                return false;
            }
        }

        afterJoinRefs = new ArrayList<Mutable<ILogicalOperator>>();
        boolean planTransformed = false;

        // Recursively check the plan whether the desired pattern exists in it. If so, try to optimize the plan.
        planTransformed = checkAndApplyTheRule(opRef);

        if (joinOp != null) {
            context.addToDontApplySet(this, joinOp);
        }

        if (!planTransformed) {
            return false;
        } else {
            StringBuilder sb = new StringBuilder();
            LogicalOperatorPrettyPrintVisitor pvisitor = context.getPrettyPrintVisitor();
            PlanPrettyPrinter.printOperator((AbstractLogicalOperator) opRef.getValue(), sb, pvisitor, 0);
            System.out.println("\n" + sb.toString());
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }

        return planTransformed;
    }

    // Check whether (Groupby)? <-- Leftouterjoin
    private boolean isLeftOuterJoin(AbstractLogicalOperator op1) {
        if (op1.getInputs().size() != 1) {
            return false;
        }
        if (((AbstractLogicalOperator) op1.getInputs().get(0).getValue()).getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
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
        joinOp = null;
        joinCond = null;
        isLeftOuterJoin = false;
        context = null;
        afterJoinRefs = null;
    }

    // Recursively traverse the given plan and check whether JOIN operator exists.
    // If one is found, maintain the path from the root to JOIN operator if it is not already optimized.
    protected boolean checkAndApplyTheRule(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        boolean joinFoundAndOptimizationApplied = false;

        // Check the operator pattern to see whether it is JOIN or not.
        boolean isThisOpInnerJoin = isInnerJoin(op);
        isLeftOuterJoin = isLeftOuterJoin(op);
        boolean isThisOpLeftOuterJoin = isLeftOuterJoin;
        boolean isParentOpGroupBy = hasGroupBy;
        boolean canPushDownLimitForThisOp = canPushDownLimit;
        long limitNumberOfResultForThisOp = limitNumberOfResult;
        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderByExpressionsForThisOp = orderByExpressions;

        Mutable<ILogicalOperator> joinRefFromThisOp = null;
        AbstractBinaryJoinOperator joinOpFromThisOp = null;

        if (isThisOpInnerJoin) {
            // Set join op.
            joinRef = opRef;
            joinOp = (InnerJoinOperator) op;
            joinRefFromThisOp = opRef;
            joinOpFromThisOp = (InnerJoinOperator) op;
        } else if (isThisOpLeftOuterJoin) {
            // Set left-outer-join op.
            joinRef = op.getInputs().get(0);
            joinOp = (LeftOuterJoinOperator) joinRef.getValue();
            joinRefFromThisOp = op.getInputs().get(0);
            joinOpFromThisOp = (LeftOuterJoinOperator) joinRefFromThisOp.getValue();

            // Since the child of the current operator is left-outer-join,
            // we need to add the current operator to the operators list that contains
            // all operators after join operator.
            afterJoinRefs.add(opRef);
        } else {
            // Other ops: before traversing the descendants,
            // we keep certain information such as LIMIT, order by.
            afterJoinRefs.add(opRef);

            if (op.getOperatorTag() == LogicalOperatorTag.LIMIT) {
                canPushDownLimit = true;

                // Reset order-by expression since the previous one (if any)
                // can't be combined with this new LIMIT
                orderByExpressions = null;

                // Keep the limit number of Result
                LimitOperator limitOp = (LimitOperator) op;
                if (limitOp.getMaxObjects().getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    // Currently, we support LIMIT with a constant value.
                    limitNumberOfResult = AccessMethodUtils.getInt64Constant(limitOp.getMaxObjects());
                } else {
                    canPushDownLimit = false;
                    limitNumberOfResult = -1;
                }
            } else if (canPushDownLimit && op.getOperatorTag() == LogicalOperatorTag.ORDER) {
                // Save the given order by expression
                OrderOperator orderOp = (OrderOperator) op;
                orderByExpressions = orderOp.getOrderExpressions();
            } else if (op.canDecreaseCardinality() != canDecreaseCardinalityCode.FALSE
                    || (orderByExpressions != null && op.canPreserveOrder() != canPreserveOrderCode.TRUE)) {
                // If the given operator can decrease the input cardinality or
                // cannot preserve the input order when there is an order by,
                // we can't pass the LIMIT information to the index-search. We need to find another LIMIT.
                canPushDownLimit = false;
                limitNumberOfResult = -1;
            }
        }

        // Recursively check the plan and try to optimize it.
        // We first check the children of the given operator to make sure an earlier join in the path is optimized first.
        for (int i = 0; i < op.getInputs().size(); i++) {
            joinFoundAndOptimizationApplied = checkAndApplyTheRule(op.getInputs().get(i));
            if (joinFoundAndOptimizationApplied) {
                return true;
            }
        }

        // For JOIN case, try to transform the given plan.
        if (isThisOpInnerJoin || isThisOpLeftOuterJoin) {

            // Restore the information from this operator since it might be set to null
            // if there are other join operators in the earlier path.
            joinRef = joinRefFromThisOp;
            joinOp = joinOpFromThisOp;
            isLeftOuterJoin = isThisOpLeftOuterJoin;
            hasGroupBy = isParentOpGroupBy;
            canPushDownLimit = canPushDownLimitForThisOp;
            limitNumberOfResult = limitNumberOfResultForThisOp;
            orderByExpressions = orderByExpressionsForThisOp;

            // Already checked? If not, this operator may be optimized.
            if (!context.checkIfInDontApplySet(this, joinOp)) {
                Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new HashMap<IAccessMethod, AccessMethodAnalysisContext>();

                // Check the condition of JOIN operator is a function call and initialize operator members.
                if (checkJoinOperatorCondition()) {

                    // Analyze condition on those optimizable subtrees that has a datasource scan.
                    boolean matchInLeftSubTree = false;
                    boolean matchInRightSubTree = false;
                    if (leftSubTree.hasDataSource()) {
                        matchInLeftSubTree = analyzeCondition(joinCond, leftSubTree.assignsAndUnnests, analyzedAMs,
                                context, typeEnvironment);
                    }
                    if (rightSubTree.hasDataSource()) {
                        matchInRightSubTree = analyzeCondition(joinCond, rightSubTree.assignsAndUnnests, analyzedAMs,
                                context, typeEnvironment);
                    }

                    if (matchInLeftSubTree || matchInRightSubTree) {
                        // Set dataset and type metadata.
                        AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
                        boolean checkLeftSubTreeMetadata = false;
                        boolean checkRightSubTreeMetadata = false;
                        if (matchInLeftSubTree) {
                            checkLeftSubTreeMetadata = leftSubTree.setDatasetAndTypeMetadata(metadataProvider);
                        }
                        if (matchInRightSubTree) {
                            checkRightSubTreeMetadata = rightSubTree.setDatasetAndTypeMetadata(metadataProvider);
                        }

                        if (checkLeftSubTreeMetadata || checkRightSubTreeMetadata) {
                            // Map variables to the applicable indexes.
                            if (checkLeftSubTreeMetadata) {
                                fillSubTreeIndexExprs(leftSubTree, analyzedAMs, context);
                            }
                            if (checkRightSubTreeMetadata) {
                                fillSubTreeIndexExprs(rightSubTree, analyzedAMs, context);
                            }

                            // Prune the access methods if there is no applicable index for them.
                            pruneIndexCandidates(analyzedAMs, context, typeEnvironment);

                            // Prioritize the order of index that will be applied. If the right subtree (inner branch) has indexes,
                            // those indexes will be used.
                            String innerDataset = null;
                            if (rightSubTree.dataset != null) {
                                innerDataset = rightSubTree.dataset.getDatasetName();
                            }
                            removeIndexCandidatesFromOuterRelation(analyzedAMs, innerDataset);

                            // For the case of left-outer-join, we have to use indexes from the inner branch.
                            // For the inner-join, we try to use the indexes from the inner branch first.
                            // If no index is available, then we use the indexes from the outer branch.
                            Pair<IAccessMethod, Index> chosenIndex = chooseIndex(analyzedAMs);
                            if (chosenIndex != null) {

                                // Find the field name of each variable in the sub-tree - required when checking index-only plan.
                                if (checkLeftSubTreeMetadata) {
                                    fillFieldNamesInTheSubTree(leftSubTree);
                                }
                                if (checkRightSubTreeMetadata) {
                                    fillFieldNamesInTheSubTree(rightSubTree);
                                }

                                // Apply plan transformation using chosen index.
                                AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(chosenIndex.first);

                                //For LOJ with GroupBy, prepare objects to reset LOJ nullPlaceHolderVariable in GroupByOp
                                if (isThisOpLeftOuterJoin && isParentOpGroupBy) {
                                    analysisCtx.setLOJGroupbyOpRef(opRef);
                                    ScalarFunctionCallExpression isNullFuncExpr = AccessMethodUtils
                                            .findLOJIsNullFuncInGroupBy((GroupByOperator) opRef.getValue());
                                    analysisCtx.setLOJIsNullFuncInGroupBy(isNullFuncExpr);
                                }

                                // Determine if the index is applicable on the left or right side (if both, we prefer the right (inner) side).
                                Dataset indexDataset = analysisCtx.indexDatasetMap.get(chosenIndex.second);
                                OptimizableOperatorSubTree indexSubTree = null;
                                OptimizableOperatorSubTree probeSubTree = null;

                                boolean isRightTreeIndexSubTree = AccessMethodUtils.isRightTreeIndexSubTree(
                                        indexDataset, isThisOpLeftOuterJoin, leftSubTree, rightSubTree);
                                if (isRightTreeIndexSubTree) {
                                    indexSubTree = rightSubTree;
                                    probeSubTree = leftSubTree;
                                } else {
                                    indexSubTree = leftSubTree;
                                    probeSubTree = rightSubTree;
                                }

                                // If the chosen index is the primary index - add variable:name to subTree.fieldNames
                                ArrayList<LogicalVariable> pkVars = new ArrayList<LogicalVariable>();
                                if (chosenIndex.second.isPrimaryIndex()) {
                                    indexSubTree.getPrimaryKeyVars(pkVars);
                                    List<List<String>> chosenIndexFieldNames = chosenIndex.second.getKeyFieldNames();
                                    for (int i = 0; i < pkVars.size(); i++) {
                                        indexSubTree.fieldNames.put(pkVars.get(i), chosenIndexFieldNames.get(i));
                                    }
                                }

                                // There is a LIMIT operator in the plan and we can push down this to the secondary index search.
                                if (canPushDownLimit && limitNumberOfResult > -1) {

                                    // Set the first index-search (data-scan) of the given sub trees
                                    leftSubTree.setFirstIndexSearchOrDataScan();
                                    rightSubTree.setFirstIndexSearchOrDataScan();

                                    // If this is LEFT-OUTER-JOIN,
                                    // we can pass LIMIT information to the first index-search of outer branch under:
                                    // 1) outer branch has an UNNEST-MAP (index-search) or a data-scan.
                                    // 2) This index-search can generate the final results (trustworthy results in the outer branch).
                                    // 3) the order of attribute from its index-search (data-scan) is the same as the attributes
                                    //    in the order by expressions
                                    // If it is the case, the index-search of the OUTER branch can generate just enough number of results
                                    // indicated by LIMIT operator in the plan.

                                    // INNER-JOIN case: we pass limit to inner branch since we can't count the final results
                                    // from the outer branch. Thus, lIMIT will be applied on the secondary index-search
                                    // or primary-index search of the inner branch. And the index should cover all join conditions,
                                    // meaning that it needs to generate trustworthy results.

                                    if (orderByExpressions != null) {
                                        // Check the attributes order between the index-search and order by expressions.
                                        canPushDownLimit = isAttrSequenceOfOrderByAndUnnestMapSame(probeSubTree,
                                                orderByExpressions, !isLeftOuterJoin);
                                    }

                                    // Now, we pass LIMIT information to the index-search.
                                    // For OUTER JOIN, limit will be applied on outer branch.
                                    // For INNER JOIN, limit will be applied on inner branch.
                                    if (canPushDownLimit) {
                                        if (isLeftOuterJoin) {
                                            canPushDownLimit = checkAndPassLimitToIndexSearch(probeSubTree,
                                                    limitNumberOfResult, false);
                                        } else {
                                            canPushDownLimit = checkAndPassLimitToIndexSearch(indexSubTree,
                                                    limitNumberOfResult, true);
                                        }
                                    }

                                    if (!canPushDownLimit) {
                                        limitNumberOfResult = -1;
                                    } else {
                                        analysisCtx.setLimitNumberOfResult(limitNumberOfResult);
                                        analysisCtx.setOrderByExpressions(orderByExpressions);
                                    }
                                }

                                boolean res = chosenIndex.first.applyJoinPlanTransformation(afterJoinRefs, joinRef,
                                        leftSubTree, rightSubTree, chosenIndex.second, analysisCtx, context,
                                        isLeftOuterJoin, hasGroupBy);

                                // If the plan transformation is successful, we don't need to traverse the plan any more,
                                // since if there are more JOIN operators, the next trigger on this plan will find them.
                                if (res) {
                                    return res;
                                }
                            } else {
                                context.addToDontApplySet(this, joinOp);
                            }

                        }

                    }
                }

            }

            joinRef = null;
            joinOp = null;
            afterJoinRefs.add(opRef);
        }

        // Clean the path above JOIN operator by removing the current operator
        afterJoinRefs.remove(opRef);

        return false;
    }

    /**
     * After the pattern is matched, check the condition and initialize the data sources from the both sub trees.
     */
    protected boolean checkJoinOperatorCondition() {

        typeEnvironment = context.getOutputTypeEnvironment(joinOp);

        // Check that the join's condition is a function call.
        ILogicalExpression condExpr = joinOp.getCondition().getValue();
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        joinCond = (AbstractFunctionCallExpression) condExpr;
        leftSubTree.initFromSubTree(joinOp.getInputs().get(0));
        rightSubTree.initFromSubTree(joinOp.getInputs().get(1));
        // One of the subtrees must have a datasource scan.
        if (leftSubTree.hasDataSourceScan() || rightSubTree.hasDataSourceScan()) {
            return true;
        }
        return false;
    }

}
