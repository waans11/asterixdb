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
package org.apache.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;

/**
 * Context for analyzing the applicability of a single access method.
 */
public class AccessMethodAnalysisContext {

    public List<IOptimizableFuncExpr> matchedFuncExprs = new ArrayList<IOptimizableFuncExpr>();

    // Contains candidate indexes and a list of (integer,integer) tuples that index into matchedFuncExprs and matched variable inside this expr.
    // We are mapping from candidate indexes to a list of function expressions
    // that match one of the index's expressions.
    public Map<Index, List<Pair<Integer, Integer>>> indexExprsAndVars = new TreeMap<Index, List<Pair<Integer, Integer>>>();

    // Maps from index to the dataset it is indexing.
    public Map<Index, Dataset> indexDatasetMap = new TreeMap<Index, Dataset>();

    // Maps from an index to the number of matched fields in the query plan (for performing prefix search)
    public Map<Index, Integer> indexNumMatchedKeys = new TreeMap<Index, Integer>();

    // variables for resetting null placeholder for left-outer-join
    private Mutable<ILogicalOperator> lojGroupbyOpRef = null;
    private ScalarFunctionCallExpression lojIsNullFuncInGroupBy = null;

    // For a secondary index, if we use only PK and secondary key field in a plan, it is an index-only plan.
    private boolean indexOnlySelectPlanEnabled = false;

    // For this access method, we push down the LIMIT from an ancestor operator (-1: no limit)
    // That is, an index-search just generates this number of results.
    private long limitNumberOfResult = -1;

    List<Pair<IOrder, Mutable<ILogicalExpression>>> orderByExpressions = null;

    public void addIndexExpr(Dataset dataset, Index index, Integer exprIndex, Integer varIndex) {
        List<Pair<Integer, Integer>> exprs = indexExprsAndVars.get(index);
        if (exprs == null) {
            exprs = new ArrayList<Pair<Integer, Integer>>();
            indexExprsAndVars.put(index, exprs);
        }
        exprs.add(new Pair<Integer, Integer>(exprIndex, varIndex));
        indexDatasetMap.put(index, dataset);
    }

    public List<Pair<Integer, Integer>> getIndexExprs(Index index) {
        return indexExprsAndVars.get(index);
    }

    public void setLOJGroupbyOpRef(Mutable<ILogicalOperator> opRef) {
        lojGroupbyOpRef = opRef;
    }

    public Mutable<ILogicalOperator> getLOJGroupbyOpRef() {
        return lojGroupbyOpRef;
    }

    public void setLOJIsNullFuncInGroupBy(ScalarFunctionCallExpression isNullFunc) {
        lojIsNullFuncInGroupBy = isNullFunc;
    }

    public ScalarFunctionCallExpression getLOJIsNullFuncInGroupBy() {
        return lojIsNullFuncInGroupBy;
    }

    public void setIndexOnlyPlanEnabled(boolean enabled) {
        this.indexOnlySelectPlanEnabled = enabled;
    }

    public boolean isIndexOnlyPlanEnabled() {
        return this.indexOnlySelectPlanEnabled;
    }

    public void setLimitNumberOfResult(long limitNumberOfResult) {
        this.limitNumberOfResult = limitNumberOfResult;
    }

    public long getLimitNumberOfResult() {
        return this.limitNumberOfResult;
    }

    public void setOrderByExpressions(List<Pair<IOrder, Mutable<ILogicalExpression>>> orderByExpressions) {
        this.orderByExpressions = orderByExpressions;
    }

    public List<Pair<IOrder, Mutable<ILogicalExpression>>> getOrderByExpressions() {
        return this.orderByExpressions;
    }

}
