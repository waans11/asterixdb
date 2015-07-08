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

import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
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
 *
 * Replaces the above patterns with this plan if it is an index-only plan (only using PK and/or secondary key field):
 * OLD:(select) <-- (assign) <-- (btree search) <-- (sort) <-- (unnest-map(index search)) <-- (assign)
 * NEW: (union) <-- (select) <-- (assign)+ <-- (b-tree search) <-- (sort) <-- (split) <-- (unnest-map(index search)) <-- (assign)
 *      (union) <--                                                       <-- (split)
 * In an index-only plan, sort is not required.
 *
 * If an index-only plan is not possible, the original plan will be transformed into this:
 * OLD:(select) <-- (assign) <-- (btree search) <-- (sort) <-- (unnest-map(index search)) <-- (assign)
 * NEW:(select) <-- (assign) <-- (btree search) <-- (sort) <-- (unnest-map(index search)) <-- (assign)
 * In this case, the sort is optional, and some access methods implementations may choose not to sort.
 *
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

        // Match operator pattern and initialize operator members.
        if (!matchesOperatorPattern(opRef, context)) {
            return false;
        }

        // Analyze select condition.
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new HashMap<IAccessMethod, AccessMethodAnalysisContext>();
        if (!analyzeCondition(selectCond, subTree.assignsAndUnnests, analyzedAMs)) {
            return false;
        }

        // Set dataset and recordType metadata.
        if (!subTree.setDatasetAndTypeMetadata((AqlMetadataProvider) context.getMetadataProvider())) {
            return false;
        }

        fillSubTreeIndexExprs(subTree, analyzedAMs, context);
        pruneIndexCandidates(analyzedAMs);

        // Choose index to be applied.
        Pair<IAccessMethod, Index> chosenIndex = chooseIndex(analyzedAMs);
        if (chosenIndex == null) {
            context.addToDontApplySet(this, opRef.getValue());
            context.addToDontApplySet(this, selectRef.getValue());
            return false;
        }

        // Apply plan transformation using chosen index.
        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(chosenIndex.first);
        boolean res = chosenIndex.first.applySelectPlanTransformation(afterSelectRefs, selectRef, subTree, chosenIndex.second,
                analysisCtx, context);
        if (res) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }
        context.addToDontApplySet(this, opRef.getValue());
        context.addToDontApplySet(this, selectRef.getValue());

        return res;
    }

    /**
     * matches operator's pattern: distribute_result -> (any operators)+  -> select -> assign | unnest? -> datasource_scan
     * Checking begins from the root operator in order to keep logical variables that are used above select operators.
     * This way, we can identify whether the given plan is an index-only plan.
     */
    protected boolean matchesOperatorPattern(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
    	// Check whether this operator is the root, which is distriute_result
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        if (op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
        	return false;
        }

        Mutable<ILogicalOperator> descendantRef = op.getInputs().get(0);
        AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) descendantRef.getValue();

        afterSelectRefs = new ArrayList<Mutable<ILogicalOperator>>();
        boolean selectFound = false;

        // Add the current op - root
		afterSelectRefs.add(opRef);

        // Store all operators after SELECT operator.
        // This information will be used to determine whether the given plan is an index-only plan.

    	// First, check that whether a select operator exists and its condition is a function call.
        while (descendantOp != null) {
        	if (descendantOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
        		selectRef = descendantRef;
        		selectOp = (SelectOperator) descendantOp;
        		selectFound = true;
        		break;
        	} else {
        		afterSelectRefs.add(descendantRef);
                if (descendantOp.getInputs().size() < 1) {
                	// reach the bottom level
                    break;
                }
                descendantRef = descendantOp.getInputs().get(0);
        		descendantOp = (AbstractLogicalOperator) descendantRef.getValue();
        	}
        }

        if (!selectFound) {
        	return false;
        }

        if (context.checkIfInDontApplySet(this, opRef.getValue()) || context.checkIfInDontApplySet(this, selectOp)) {
        	return false;
        }

        // Set and analyze select.
        // Check that the select's condition is a function call.
        ILogicalExpression condExpr = selectOp.getCondition().getValue();
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        selectCond = (AbstractFunctionCallExpression) condExpr;

        // Match and put assign, un-nest, datasource information
        boolean res = subTree.initFromSubTree(selectOp.getInputs().get(0));
        return res && subTree.hasDataSourceScan();
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
    }
}
