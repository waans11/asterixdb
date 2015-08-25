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
import java.util.HashMap;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * This rule optimizes simple selections with secondary or primary indexes. The use of an
 * index is expressed as an unnest-map over an index-search function which will be
 * replaced with the appropriate embodiment during codegen.
 * Matches the following operator patterns:
 * Standard secondary index pattern:
 * There must be at least one assign, but there may be more, e.g., when matching similarity-jaccard-check().
 * (select) <-- (assign | unnest)+ <-- (datasource scan)
 * Primary index lookup pattern:
 * Since no assign is necessary to get the primary key fields (they are already stored fields in the BTree tuples).
 * (select) <-- (datasource scan)
 * Replaces the above patterns with this plan if it is an index-only plan (only using PK and/or secondary key field):
 * OLD:(select) <-- (assign) <-- (btree search) <-- (sort) <-- (unnest-map(index search)) <-- (assign)
 * NEW: (union) <-- (select) <-- (assign)+ <-- (b-tree search) <-- (sort) <-- (split) <-- (unnest-map(index search)) <-- (assign)
 * (union) <-- <-- (split)
 * In an index-only plan, sort is not required.
 * If an index-only plan is not possible, the original plan will be transformed into this:
 * OLD:(select) <-- (assign | unnest)+ <-- (datasource scan)
 * NEW:(select) <-- (assign) <-- (btree search) <-- (sort) <-- (unnest-map(index search)) <-- (assign)
 * In this case, the sort is optional, and some access methods implementations may choose not to sort.
 * Note that for some index-based optimizations we do not remove the triggering
 * condition from the select, since the index may only acts as a filter, and the
 * final verification must still be done with the original select condition (where an Index.canProduceFalsePositive = true).
 * The basic outline of this rule is:
 * 1. Match operator pattern.
 * 2. Analyze select condition to see if there are optimizable functions (delegated to IAccessMethods).
 * 3. Check metadata to see if there are applicable indexes.
 * 4. Choose an index to apply (for now only a single index will be chosen).
 * 5. Rewrite plan using index (delegated to IAccessMethods).
 */
public class IntroduceSelectAccessMethodRule extends AbstractIntroduceAccessMethodRule {

    // Operators representing the patterns to be matched:
    // These ops are set in matchesPattern()
    protected List<Mutable<ILogicalOperator>> afterSelectRefs = null;
    protected Mutable<ILogicalOperator> selectRef = null;
    protected SelectOperator selectOp = null;
    protected AbstractFunctionCallExpression selectCond = null;
    protected final OptimizableOperatorSubTree subTree = new OptimizableOperatorSubTree();
    protected IOptimizationContext context = null;

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

        afterSelectRefs = new ArrayList<Mutable<ILogicalOperator>>();
        boolean planTransformed = false;

        // Recursively check the plan whether the desired pattern exists in it. If so, try to optimize the plan.
        planTransformed = checkAndApplyTheRule(opRef);

        if (!planTransformed) {
            return false;
        } else {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
            context.addToDontApplySet(this, selectRef.getValue());
        }

        return planTransformed;
    }

    protected boolean checkSelectOperatorCondition(Mutable<ILogicalOperator> opRef) {
        // Set and analyze select.
        // Check that the SELECT condition is a function call.
        ILogicalExpression condExpr = selectOp.getCondition().getValue();
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        selectCond = (AbstractFunctionCallExpression) condExpr;

        // Match and put assign, un-nest, datasource information
        boolean res = subTree.initFromSubTree(selectOp.getInputs().get(0));
        return res && subTree.hasDataSourceScan();
    }

    // Recursively traverse the given plan and check whether SELECT operator exists.
    // If one is found, maintain the path from the root to SELECT operator if it is not already optimized.
    protected boolean checkAndApplyTheRule(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        boolean selectFoundAndOptimizationApplied = false;

        // Found SELECT operator
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            selectRef = opRef;
            selectOp = (SelectOperator) op;

            // Already checked? If not, this operator can be optimized.
            if (!context.checkIfInDontApplySet(this, selectOp)) {
                Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new HashMap<IAccessMethod, AccessMethodAnalysisContext>();

                // Check the condition of SELECT operator is a function call and initialize operator members.
                if (checkSelectOperatorCondition(opRef)) {

                    // Analyze the condition of SELECT operator.
                    if (analyzeCondition(selectCond, subTree.assignsAndUnnests, analyzedAMs)) {

                        // Set dataset and type metadata.
                        if (subTree.setDatasetAndTypeMetadata((AqlMetadataProvider) context.getMetadataProvider())) {

                            // Map variables to the applicable indexes.
                            fillSubTreeIndexExprs(subTree, analyzedAMs, context);

                            // Prune the access methods if there is no applicable index for them.
                            pruneIndexCandidates(analyzedAMs);

                            // Choose index to be applied.
                            Pair<IAccessMethod, Index> chosenIndex = chooseIndex(analyzedAMs);

                            // We can't apply any index for this SELECT operator
                            if (chosenIndex == null) {
                                context.addToDontApplySet(this, selectRef.getValue());
                            } else {

                                // Get the access method context for the chosen index.
                                AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(chosenIndex.first);

                                // Find the field name of each variable in the sub-tree - required when checking index-only plan.
                                fillFieldNamesInTheSubTree(subTree);

                                // Try to apply plan transformation using chosen index.
                                boolean res = chosenIndex.first.applySelectPlanTransformation(afterSelectRefs,
                                        selectRef, subTree, chosenIndex.second, analysisCtx, context);

                                // If the plan transformation is successful, we don't need to traverse the plan any more,
                                // since if there are more SELECT operators, the next trigger on this plan will find them.
                                if (res) {
                                    return res;
                                }
                            }
                        }
                    }
                }
            }
            selectRef = null;
            selectOp = null;
            afterSelectRefs.add(opRef);
        } else {
            afterSelectRefs.add(opRef);
        }

        // Recursively check the plan and try to optimize it.
        for (int i = 0; i < op.getInputs().size(); i++) {
            selectFoundAndOptimizationApplied = checkAndApplyTheRule(op.getInputs().get(i));
            if (selectFoundAndOptimizationApplied) {
                return true;
            }
        }

        // Clean the path above SELECT operator by removing the current operator
        afterSelectRefs.remove(opRef);

        return false;
    }

    @Override
    public Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods() {
        return accessMethods;
    }

    private void clear() {
        afterSelectRefs = null;
        selectRef = null;
        selectOp = null;
        selectCond = null;
        context = null;
    }
}
