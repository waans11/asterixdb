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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * This rule optimizes a join with secondary indexes into an indexed nested-loop join.
 * Matches the following operator pattern:
 * (join) <-- (select)? <-- (assign | unnest)+ <-- (datasource scan)
 * ...... <-- (select)? <-- (assign | unnest)+ <-- (datasource scan | unnest-map)
 * The order of the join inputs does not matter.
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
            matchInLeftSubTree = analyzeCondition(joinCond, leftSubTree.assignsAndUnnests, analyzedAMs);
        }
        if (rightSubTree.hasDataSource()) {
            matchInRightSubTree = analyzeCondition(joinCond, rightSubTree.assignsAndUnnests, analyzedAMs);
        }
        if (!matchInLeftSubTree && !matchInRightSubTree) {
            return false;
        }

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
        if (!checkLeftSubTreeMetadata && !checkRightSubTreeMetadata) {
            return false;
        }
        if (checkLeftSubTreeMetadata) {
            fillSubTreeIndexExprs(leftSubTree, analyzedAMs, context);
        }
        if (checkRightSubTreeMetadata) {
            fillSubTreeIndexExprs(rightSubTree, analyzedAMs, context);
        }
        pruneIndexCandidates(analyzedAMs);

        //Remove possibly chosen indexes from left Tree
        if (isLeftOuterJoin) {
            Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
            // Check applicability of indexes by access method type.
            while (amIt.hasNext()) {
                Map.Entry<IAccessMethod, AccessMethodAnalysisContext> entry = amIt.next();
                AccessMethodAnalysisContext amCtx = entry.getValue();
                Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt = amCtx.indexExprsAndVars.entrySet()
                        .iterator();
                while (indexIt.hasNext()) {
                    Map.Entry<Index, List<Pair<Integer, Integer>>> indexEntry = indexIt.next();

                    Index chosenIndex = indexEntry.getKey();
                    if (!chosenIndex.getDatasetName().equals(rightSubTree.dataset.getDatasetName())) {
                        indexIt.remove();
                    }
                }
            }
        }

        // Choose index to be applied.
        Pair<IAccessMethod, Index> chosenIndex = chooseIndex(analyzedAMs);
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
                    .findLOJIsNullFuncInGroupBy((GroupByOperator) opRef.getValue());
            analysisCtx.setLOJIsNullFuncInGroupBy(isNullFuncExpr);
        }
        boolean res = chosenIndex.first.applyJoinPlanTransformation(joinRef, leftSubTree, rightSubTree,
                chosenIndex.second, analysisCtx, context, isLeftOuterJoin, hasGroupBy);
        if (res) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }
        context.addToDontApplySet(this, join);
        return res;
    }

    protected boolean matchesOperatorPattern(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
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

        // Check that the select's condition is a function call.
        ILogicalExpression condExpr = join.getCondition().getValue();
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        joinCond = (AbstractFunctionCallExpression) condExpr;
        leftSubTree.initFromSubTree(join.getInputs().get(0));
        rightSubTree.initFromSubTree(join.getInputs().get(1));
        // One of the subtrees must have a datasource scan.
        if (leftSubTree.hasDataSourceScan() || rightSubTree.hasDataSourceScan()) {
            return true;
        }
        return false;
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
        join = null;
        joinCond = null;
        isLeftOuterJoin = false;
    }
}
