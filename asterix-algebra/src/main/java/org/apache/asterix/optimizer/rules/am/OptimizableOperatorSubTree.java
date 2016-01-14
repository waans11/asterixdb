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

import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;

/**
 * Operator subtree that matches the following patterns, and provides convenient access to its nodes:
 * (limit)* <-- (orderby)* <-- (select)? <-- (assign | unnest)* <-- (datasource scan | unnest-map)*
 */
public class OptimizableOperatorSubTree {

    public static enum DataSourceType {
        DATASOURCE_SCAN,
        EXTERNAL_SCAN,
        PRIMARY_INDEX_LOOKUP,
        COLLECTION_SCAN,
        INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP,
        REDUCING_NUMBER_OF_SELECT_PLAN_PRIMARY_INDEX_LOOKUP,
        NO_DATASOURCE
    }

    public ILogicalOperator root = null;
    public Mutable<ILogicalOperator> rootRef = null;
    public final List<Mutable<ILogicalOperator>> assignsAndUnnestsRefs = new ArrayList<Mutable<ILogicalOperator>>();
    public final List<AbstractLogicalOperator> assignsAndUnnests = new ArrayList<AbstractLogicalOperator>();
    public Mutable<ILogicalOperator> dataSourceRef = null;
    public DataSourceType dataSourceType = DataSourceType.NO_DATASOURCE;
    // Dataset and type metadata. Set in setDatasetAndTypeMetadata().
    public Dataset dataset = null;
    public ARecordType recordType = null;
    // Contains the field names for all assign operations in this sub-tree
    public HashMap<LogicalVariable, List<String>> fieldNames = new HashMap<LogicalVariable, List<String>>();

    // Additional datasources can exist if IntroduceJoinAccessMethodRule has been applied.
    // (E.g. There are index-nested-loop-joins in the plan.)
    public List<Mutable<ILogicalOperator>> ixJoinOuterAdditionalDataSourceRefs = null;
    public List<DataSourceType> ixJoinOuterAdditionalDataSourceTypes = null;
    public List<Dataset> ixJoinOuterAdditionalDatasets = null;
    public List<ARecordType> ixJoinOuterAdditionalRecordTypes = null;

    // Order by expression in this subtree
    public List<Pair<IOrder, Mutable<ILogicalExpression>>> subTreeOrderByExpr = null;

    // Limit information in this subtree
    public Mutable<ILogicalOperator> limitRef = null;

    // First trustworthy index-search or data-scan in this subtree
    public Mutable<ILogicalOperator> firstTrustworthyIdxSearchRef = null;

    // Contains SELECT operators
    public final List<Mutable<ILogicalOperator>> selectRefs = new ArrayList<Mutable<ILogicalOperator>>();

    /**
     * Initialize assign, unnest and datasource information
     *
     * @throws AlgebricksException
     */
    public boolean initFromSubTree(Mutable<ILogicalOperator> subTreeOpRef, IOptimizationContext context)
            throws AlgebricksException {
        reset();
        rootRef = subTreeOpRef;
        root = subTreeOpRef.getValue();
        // Examine the op's children to match the expected patterns.
        AbstractLogicalOperator subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
        Mutable<ILogicalOperator> dataSourceOpRef = subTreeOpRef;

        AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
        do {
            // Keep limit information if one is present
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.LIMIT) {
                limitRef = subTreeOpRef;
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
                //                subTreeOp = (AbstractLogicalOperator) subTreeOp.getInputs().get(0).getValue();
            }
            // Match (assign | unnest)+.
            while (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN
                    || subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                assignsAndUnnestsRefs.add(subTreeOpRef);
                assignsAndUnnests.add(subTreeOp);

                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            };
            // Keep Order by information if one is present
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.ORDER) {
                OrderOperator orderOp = (OrderOperator) subTreeOp;
                subTreeOrderByExpr = orderOp.getOrderExpressions();
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
                //                subTreeOp = (AbstractLogicalOperator) subTreeOp.getInputs().get(0).getValue();
            }
            // If this is an index-only plan or reducing the number of select plan, then we have union operator.
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                // If this plan has a UNIONALL operator and turns out to be a non-index only plan,
                // then we try to initialize data source and return to the caller.
                if (!initFromIndexOnlyOrReducingTheNumberOfTheSelectPlan(subTreeOpRef, context)) {
                    return initializeDataSource(subTreeOpRef);
                } else {
                    // Done initializing the data source.
                    return true;
                }
            }
            // Skip select operator.
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                selectRefs.add(subTreeOpRef);
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            }
            // Check primary-index pattern.
            if (subTreeOp.getOperatorTag() != LogicalOperatorTag.ASSIGN
                    && subTreeOp.getOperatorTag() != LogicalOperatorTag.UNNEST) {
                // Pattern may still match if we are looking for primary index matches as well.
                return initializeDataSource(subTreeOpRef);
            }
            // Match (assign | unnest)+.
            while (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN
                    || subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                assignsAndUnnestsRefs.add(subTreeOpRef);
                assignsAndUnnests.add(subTreeOp);

                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            };

            // Gather the field (variable, name) information for (unnest-map)+
            // We do not increase the given subTreeOpRef at this point to properly setup the data-source.
            Mutable<ILogicalOperator> additionalSubTreeOpRef = subTreeOpRef;
            UnnestMapOperator unnestMapOp = null;
            LeftOuterUnnestMapOperator leftOuterUnnestMapOp = null;
            List<LogicalVariable> unnestMapOpVars = null;
            do {
                if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                    unnestMapOp = (UnnestMapOperator) subTreeOp;
                    unnestMapOpVars = unnestMapOp.getVariables();

                    AccessMethodJobGenParams jobGenParams = AbstractIntroduceAccessMethodRule
                            .getAccessMethodJobGenParamsFromUnnestMap(unnestMapOp);

                    setFieldNameForUnnestMap(jobGenParams, unnestMapOpVars, metadataProvider);
                } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP) {
                    leftOuterUnnestMapOp = (LeftOuterUnnestMapOperator) subTreeOp;
                    unnestMapOpVars = leftOuterUnnestMapOp.getVariables();

                    AccessMethodJobGenParams jobGenParams = AbstractIntroduceAccessMethodRule
                            .getAccessMethodJobGenParamsFromUnnestMap(unnestMapOp);

                    setFieldNameForUnnestMap(jobGenParams, unnestMapOpVars, metadataProvider);
                }
                additionalSubTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) additionalSubTreeOpRef.getValue();

                if (subTreeOp.getInputs().size() < 1) {
                    break;
                }
            } while (subTreeOp.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE);

        } while (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT);

        // Match data source (datasource scan or primary index search).
        return initializeDataSource(subTreeOpRef);
    }

    /**
     * Set the field names for variables from the given index-search.
     */
    private void setFieldNameForUnnestMap(AccessMethodJobGenParams jobGenParams,
            List<LogicalVariable> unnestMapVariables, AqlMetadataProvider metadataProvider) throws AlgebricksException {
        if (jobGenParams != null) {
            // Fetch the associated index
            Index idxUsedInUnnestMap = metadataProvider.getIndex(jobGenParams.getDataverseName(),
                    jobGenParams.getDatasetName(), jobGenParams.getIndexName());

            if (idxUsedInUnnestMap != null) {
                List<List<String>> idxUsedInUnnestMapFieldNames = idxUsedInUnnestMap.getKeyFieldNames();

                switch (idxUsedInUnnestMap.getIndexType()) {
                    case BTREE:
                        for (int i = 0; i < idxUsedInUnnestMapFieldNames.size(); i++) {
                            fieldNames.put(unnestMapVariables.get(i), idxUsedInUnnestMapFieldNames.get(i));
                        }
                        break;
                    case RTREE:
                    case SINGLE_PARTITION_NGRAM_INVIX:
                    case SINGLE_PARTITION_WORD_INVIX:
                    case LENGTH_PARTITIONED_NGRAM_INVIX:
                    case LENGTH_PARTITIONED_WORD_INVIX:
                    default:
                        break;
                }
            }
        }
    }

    /**
     * Initialize assign, unnest and datasource information for the index-only
     * or reducing the number of select plan subtree.
     *
     * @throws AlgebricksException
     */
    private boolean initFromIndexOnlyOrReducingTheNumberOfTheSelectPlan(Mutable<ILogicalOperator> subTreeOpRef,
            IOptimizationContext context) throws AlgebricksException {

        AbstractLogicalOperator subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
        boolean isIndexOnlyPlan = false;
        boolean isReducingTheNumberOfSelectPlan = false;

        // Top level operator should be UNIONALL operator.
        if (subTreeOp.getOperatorTag() != LogicalOperatorTag.UNIONALL) {
            return false;
        }

        UnionAllOperator unionAllOp = (UnionAllOperator) subTreeOp;
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> unionVarMap = unionAllOp.getVariableMappings();

        // The complete path
        //
        // Index-only plan path:
        // Left -
        // ... <- UNION <- PROJECT <- SELECT <- ASSIGN? <- UNNEST_MAP(PIdx) <- SPLIT <- UNNEST_MAP (SIdx) <- ASSIGN? <- ...
        // Right -
        //              <- PROJECT <- SELECT? <- ASSIGN?                    <-
        //
        // Reducing the number of SELECT plan path:
        // Left -
        // ... <- UNION <- SELECT <- SPLIT <- ASSIGN <- UNNEST_MAP(PIdx) <- ORDER <- UNNEST_MAP(SIdx) <- ASSIGN? <- ...
        // Right -
        //              <-        <-

        // We now traverse the left path first (tryLock on PK fail path).
        // Index-only plan: PROJECT
        // Reducing the number of SELECT plan: SELECT
        subTreeOpRef = subTreeOp.getInputs().get(0);
        subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
        if (subTreeOp.getOperatorTag() == LogicalOperatorTag.PROJECT) {
            // Index-only plan
            isIndexOnlyPlan = true;
        } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
            // Reducing the number of SELECT plan
            selectRefs.add(subTreeOpRef);
            isReducingTheNumberOfSelectPlan = true;
        }

        // The left path
        // Index-only plan: SELECT
        // Reducing the number of SELECT plan: SPLIT - left path is done
        subTreeOpRef = subTreeOp.getInputs().get(0);
        subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();

        if (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
            selectRefs.add(subTreeOpRef);
        } else if (subTreeOp.getOperatorTag() != LogicalOperatorTag.SPLIT) {
            return false;
        }

        // The left path
        // Index-only plan: ASSIGN?
        // Reducing the number of SELECT plan: left path is already done
        subTreeOpRef = subTreeOp.getInputs().get(0);
        subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();

        if (isIndexOnlyPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                assignsAndUnnestsRefs.add(subTreeOpRef);
                assignsAndUnnests.add(subTreeOp);
            }
        }

        // The left path
        // Index-only plan: UNNEST-MAP (PIdx)
        // Reducing the number of SELECT plan: left path is already done
        subTreeOpRef = subTreeOp.getInputs().get(0);
        subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();

        if (isIndexOnlyPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                //                assignsAndUnnestsRefs.add(subTreeOpRef);
                //                assignsAndUnnests.add(subTreeOp);
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            } else {
                return false;
            }
        }

        // The left path
        // Index-only plan: SPLIT - left path is done
        // Reducing the number of SELECT plan: left path is already done
        if (isIndexOnlyPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.SPLIT) {
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            } else {
                return false;
            }
        }

        // Now, check the right path of the UNION
        // Index-only plan right path:
        //   UNION <- PROJECT <- SELECT? <- ASSIGN? <- SPLIT <- ...
        // Reducing the number of SELECT plan right path:
        //   UNION <- SPLIT <- ...
        subTreeOpRef = unionAllOp.getInputs().get(1);
        subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();

        // The right path
        // Index-only plan: PROJECT
        // Reducing the number of SELECT plan: SPLIT - right path is done - do nothing
        if (isIndexOnlyPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.PROJECT) {
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            } else {
                return false;
            }
        } else if (isReducingTheNumberOfSelectPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.SPLIT) {
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            } else {
                return false;
            }
        }

        // The right path
        // Index-only plan: SELECT?
        // Reducing the number of SELECT plan: already done
        if (isIndexOnlyPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                // This is actually an R-Tree index search case.
                selectRefs.add(subTreeOpRef);
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            }
        }

        // Index-only plan: ASSIGN?
        // Reducing the number of SELECT plan: already done
        if (isIndexOnlyPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                //                AssignOperator assignOp = (AssignOperator) OperatorManipulationUtil.deepCopy(subTreeOp);
                //                // Substitute the original assign variables to the ones that will be used after the UNION.
                //                for (int i = 0; i < unionVarMap.size(); i++) {
                //                    VariableUtilities.substituteVariables(assignOp, unionVarMap.get(i).second,
                //                            unionVarMap.get(i).third, context);
                //                }
                //                assignOp.getInputs().addAll(subTreeOp.getInputs());
                //                subTreeOpRef.setValue(assignOp);
                //                assignsAndUnnestsRefs.add(subTreeOpRef);
                //                assignsAndUnnests.add(assignOp);
                assignsAndUnnestsRefs.add(subTreeOpRef);
                assignsAndUnnests.add(subTreeOp);
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            }
        }

        // Index-only plan: SPLIT - right path is done
        // Reducing the number of SELECT plan: already done
        if (isIndexOnlyPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.SPLIT) {
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            } else {
                return false;
            }
        }

        // Now, we traversed both the left and the right path.
        // We are going to traverse the common path.
        // Index-only plan common path:
        //  ... <- SPLIT <- UNNEST_MAP (SIdx) <- ASSIGN? <- ...
        // Reducing the number of the SELECT plan common path:
        //  ... <- SPLIT <- ASSIGN <- UNNEST_MAP(PIdx) <- ORDER <- UNNEST_MAP(SIdx) <- ASSIGN? <- ...
        //
        // Index-only plan: UNNEST-MAP (SIdx)
        // Reducing the number of the SELECT plan: ASSIGN
        if (isIndexOnlyPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                //                UnnestMapOperator unnestMapOp = (UnnestMapOperator) OperatorManipulationUtil.deepCopy(subTreeOp);
                //                // Substitute the original assign variables to the ones that will be used after the UNION.
                //                for (int i = 0; i < unionVarMap.size(); i++) {
                //                    VariableUtilities.substituteVariables(unnestMapOp, unionVarMap.get(i).second,
                //                            unionVarMap.get(i).third, context);
                //                }
                //                unnestMapOp.getInputs().addAll(subTreeOp.getInputs());
                //                subTreeOpRef.setValue(unnestMapOp);
                //                assignsAndUnnestsRefs.add(subTreeOpRef);
                //                assignsAndUnnests.add(unnestMapOp);
                //                assignsAndUnnestsRefs.add(subTreeOpRef);
                //                assignsAndUnnests.add(subTreeOp);

                // This secondary index search is the data source
                boolean initDataSource = initializeDataSource(subTreeOpRef);

                if (!initDataSource) {
                    return false;
                }

                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            } else {
                return false;
            }
        } else if (isReducingTheNumberOfSelectPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                assignsAndUnnestsRefs.add(subTreeOpRef);
                assignsAndUnnests.add(subTreeOp);
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            } else {
                return false;
            }
        }

        // Index-only plan: ASSIGN?
        // Reducing the number of the SELECT plan: UNNEST_MAP (PIdx)
        if (isIndexOnlyPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                assignsAndUnnestsRefs.add(subTreeOpRef);
                assignsAndUnnests.add(subTreeOp);
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            }
        } else if (isReducingTheNumberOfSelectPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                //                assignsAndUnnestsRefs.add(subTreeOpRef);
                //                assignsAndUnnests.add(subTreeOp);

                // This primary index-search is the data source
                boolean initDataSource = initializeDataSource(subTreeOpRef);

                if (!initDataSource) {
                    return false;
                }

                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            } else {
                return false;
            }
        }

        // Index-only plan: EMPTYTUPLESOURCE
        // Reducing the number of the SELECT plan: ORDER?
        if (isIndexOnlyPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                // Done traversing the plan
                return true;
            } else {
                return false;
            }
        } else if (isReducingTheNumberOfSelectPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.ORDER) {
                // Even if there is an ORDER, this will be not kept by UNION operator.
                // Thus, we don't keep any information for this operator.
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            }
        }

        // Index-only plan: the common path is done
        // Reducing the number of the SELECT plan: UNNEST-MAP (SIdx)
        if (isReducingTheNumberOfSelectPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                //                assignsAndUnnestsRefs.add(subTreeOpRef);
                //                assignsAndUnnests.add(subTreeOp);
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            } else {
                return false;
            }
        }

        // Index-only plan: the common path is done
        // Reducing the number of the SELECT plan: ASSIGN?
        if (isReducingTheNumberOfSelectPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                assignsAndUnnestsRefs.add(subTreeOpRef);
                assignsAndUnnests.add(subTreeOp);
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            }
        }

        // Index-only plan: the common path is done
        // Reducing the number of the SELECT plan: EMPTYTUPLESOURCE
        if (isReducingTheNumberOfSelectPlan) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                // Done traversing the plan
                return true;
            } else {
                return false;
            }
        }

        // Control-flow should not reach here.
        return false;
    }

    /**
     * Initialize datasource information
     */
    private boolean initializeDataSource(Mutable<ILogicalOperator> subTreeOpRef) {
        AbstractLogicalOperator subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();

        if (subTreeOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            dataSourceType = DataSourceType.DATASOURCE_SCAN;
            dataSourceRef = subTreeOpRef;
            return true;
        } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EXTERNAL_LOOKUP) {
            dataSourceType = DataSourceType.EXTERNAL_SCAN;
            dataSourceRef = subTreeOpRef;
            return true;
        } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            dataSourceType = DataSourceType.COLLECTION_SCAN;
            dataSourceRef = subTreeOpRef;
            return true;
        } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            // There can be multiple unnest-map or datasource-scan operators
            // if index-nested-loop-join has been applied by IntroduceJoinAccessMethodRule.
            // So, we need to traverse the whole path from the subTreeOp.
            boolean dataSourceFound = false;
            while (true) {
                if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                    UnnestMapOperator unnestMapOp = (UnnestMapOperator) subTreeOp;
                    ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();

                    if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                        if (f.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
                            AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                            jobGenParams.readFromFuncArgs(f.getArguments());
                            if (jobGenParams.isPrimaryIndex()) {
                                if (dataSourceRef == null) {
                                    dataSourceRef = subTreeOpRef;
                                    dataSourceType = DataSourceType.PRIMARY_INDEX_LOOKUP;
                                } else {
                                    // One datasource already exists. This is an additional datasource.
                                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                                    ixJoinOuterAdditionalDataSourceTypes.add(DataSourceType.PRIMARY_INDEX_LOOKUP);
                                    ixJoinOuterAdditionalDataSourceRefs.add(subTreeOpRef);
                                }
                                dataSourceFound = true;
                            } else {
                                // Secondary index search in an index-only plan case
                                if (dataSourceRef == null) {
                                    dataSourceRef = subTreeOpRef;
                                    dataSourceType = DataSourceType.INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP;
                                } else {
                                    // One datasource already exists. This is an additional datasource.
                                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                                    ixJoinOuterAdditionalDataSourceTypes
                                            .add(DataSourceType.INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP);
                                    ixJoinOuterAdditionalDataSourceRefs.add(subTreeOpRef);
                                }
                                dataSourceFound = true;
                            }
                        }
                    }
                } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                    ixJoinOuterAdditionalDataSourceTypes.add(DataSourceType.DATASOURCE_SCAN);
                    ixJoinOuterAdditionalDataSourceRefs.add(subTreeOpRef);
                    dataSourceFound = true;
                } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EXTERNAL_LOOKUP) {
                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                    ixJoinOuterAdditionalDataSourceTypes.add(DataSourceType.EXTERNAL_SCAN);
                    ixJoinOuterAdditionalDataSourceRefs.add(subTreeOpRef);
                    dataSourceFound = true;
                } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                    ixJoinOuterAdditionalDataSourceTypes.add(DataSourceType.COLLECTION_SCAN);
                    ixJoinOuterAdditionalDataSourceRefs.add(subTreeOpRef);
                }

                // Traverse the subtree while there are operators in the path.
                if (subTreeOp.hasInputs()) {
                    subTreeOpRef = subTreeOp.getInputs().get(0);
                    subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
                } else {
                    break;
                }
            }

            if (dataSourceFound) {
                return true;
            }
        }

        return false;
    }

    /**
     * Find the dataset corresponding to the datasource scan in the metadata.
     * Also sets recordType to be the type of that dataset.
     */
    public boolean setDatasetAndTypeMetadata(AqlMetadataProvider metadataProvider) throws AlgebricksException {
        String dataverseName = null;
        String datasetName = null;

        Dataset ds = null;
        ARecordType rType = null;

        List<Mutable<ILogicalOperator>> sourceOpRefs = new ArrayList<Mutable<ILogicalOperator>>();
        List<DataSourceType> dsTypes = new ArrayList<DataSourceType>();

        sourceOpRefs.add(dataSourceRef);
        dsTypes.add(dataSourceType);

        // If there are multiple datasources in the subtree, we need to find the dataset for these.
        if (ixJoinOuterAdditionalDataSourceRefs != null) {
            for (int i = 0; i < ixJoinOuterAdditionalDataSourceRefs.size(); i++) {
                sourceOpRefs.add(ixJoinOuterAdditionalDataSourceRefs.get(i));
                dsTypes.add(ixJoinOuterAdditionalDataSourceTypes.get(i));
            }
        }

        for (int i = 0; i < sourceOpRefs.size(); i++) {
            switch (dsTypes.get(i)) {
                case DATASOURCE_SCAN:
                    DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) sourceOpRefs.get(i).getValue();
                    Pair<String, String> datasetInfo = AnalysisUtil.getDatasetInfo(dataSourceScan);
                    dataverseName = datasetInfo.first;
                    datasetName = datasetInfo.second;
                    break;
                case PRIMARY_INDEX_LOOKUP:
                case INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP:
                case REDUCING_NUMBER_OF_SELECT_PLAN_PRIMARY_INDEX_LOOKUP:
                    AbstractUnnestOperator unnestMapOp = (AbstractUnnestOperator) sourceOpRefs.get(i).getValue();
                    ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
                    AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                    AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                    jobGenParams.readFromFuncArgs(f.getArguments());
                    datasetName = jobGenParams.getDatasetName();
                    dataverseName = jobGenParams.getDataverseName();
                    break;
                case EXTERNAL_SCAN:
                    ExternalDataLookupOperator externalScan = (ExternalDataLookupOperator) sourceOpRefs.get(i)
                            .getValue();
                    datasetInfo = AnalysisUtil.getDatasetInfo(externalScan);
                    dataverseName = datasetInfo.first;
                    datasetName = datasetInfo.second;
                    break;
                case COLLECTION_SCAN:
                    if (i != 0) {
                        ixJoinOuterAdditionalDatasets.add(null);
                        ixJoinOuterAdditionalRecordTypes.add(null);
                    }
                    continue;
                case NO_DATASOURCE:
                default:
                    return false;
            }
            if (dataverseName == null || datasetName == null) {
                return false;
            }
            // Find the dataset corresponding to the datasource in the metadata.
            ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException("No metadata for dataset " + datasetName);
            }
            // Get the record type for that dataset.
            IAType itemType = metadataProvider.findType(dataverseName, ds.getItemTypeName());
            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                if (i == 0) {
                    return false;
                } else {
                    ixJoinOuterAdditionalDatasets.add(null);
                    ixJoinOuterAdditionalRecordTypes.add(null);
                }
            }
            rType = (ARecordType) itemType;

            // First index is always the primary datasource in this subtree.
            if (i == 0) {
                dataset = ds;
                recordType = rType;
            } else {
                ixJoinOuterAdditionalDatasets.add(ds);
                ixJoinOuterAdditionalRecordTypes.add(rType);
            }

            dataverseName = null;
            datasetName = null;
            ds = null;
            rType = null;
        }

        return true;
    }

    public boolean hasDataSource() {
        return dataSourceType != DataSourceType.NO_DATASOURCE;
    }

    public boolean hasIxJoinOuterAdditionalDataSource() {
        boolean dataSourceFound = false;
        if (ixJoinOuterAdditionalDataSourceTypes != null) {
            for (int i = 0; i < ixJoinOuterAdditionalDataSourceTypes.size(); i++) {
                if (ixJoinOuterAdditionalDataSourceTypes.get(i) != DataSourceType.NO_DATASOURCE) {
                    dataSourceFound = true;
                    break;
                }
            }
        }
        return dataSourceFound;
    }

    public boolean hasDataSourceScan() {
        return dataSourceType == DataSourceType.DATASOURCE_SCAN;
    }

    public boolean hasIxJoinOuterAdditionalDataSourceScan() {
        if (ixJoinOuterAdditionalDataSourceTypes != null) {
            for (int i = 0; i < ixJoinOuterAdditionalDataSourceTypes.size(); i++) {
                if (ixJoinOuterAdditionalDataSourceTypes.get(i) == DataSourceType.DATASOURCE_SCAN) {
                    return true;
                }
            }
        }
        return false;
    }

    public void reset() {
        root = null;
        rootRef = null;
        assignsAndUnnestsRefs.clear();
        assignsAndUnnests.clear();
        dataSourceRef = null;
        dataSourceType = DataSourceType.NO_DATASOURCE;
        ixJoinOuterAdditionalDataSourceRefs = null;
        ixJoinOuterAdditionalDataSourceTypes = null;
        dataset = null;
        ixJoinOuterAdditionalDatasets = null;
        recordType = null;
        ixJoinOuterAdditionalRecordTypes = null;
        firstTrustworthyIdxSearchRef = null;
        selectRefs.clear();
    }

    public void getPrimaryKeyVars(Mutable<ILogicalOperator> dataSourceRefToFetch, List<LogicalVariable> target)
            throws AlgebricksException {
        if (dataSourceRefToFetch == null) {
            dataSourceRefToFetch = dataSourceRef;
        }
        switch (dataSourceType) {
            case DATASOURCE_SCAN:
                DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) dataSourceRefToFetch.getValue();
                int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
                for (int i = 0; i < numPrimaryKeys; i++) {
                    target.add(dataSourceScan.getVariables().get(i));
                }
                break;
            case PRIMARY_INDEX_LOOKUP:
            case REDUCING_NUMBER_OF_SELECT_PLAN_PRIMARY_INDEX_LOOKUP:
                UnnestMapOperator unnestMapOp = (UnnestMapOperator) dataSourceRefToFetch.getValue();
                List<LogicalVariable> primaryKeys = null;
                primaryKeys = AccessMethodUtils.getPrimaryKeyVarsFromPrimaryUnnestMap(dataset, unnestMapOp);
                target.addAll(primaryKeys);
                break;
            case INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP:
                UnnestMapOperator idxOnlyPlanUnnestMapOp = (UnnestMapOperator) dataSourceRefToFetch.getValue();
                List<LogicalVariable> idxOnlyPlanKeyVars = idxOnlyPlanUnnestMapOp.getVariables();
                int idxOnlyPlanNumPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
                // The order of variables: SK, PK, the result of tryLock on PK.
                // The last variable keeps the result of tryLock on PK.
                // Thus, we deduct -1.
                int start = idxOnlyPlanKeyVars.size() - 1 - idxOnlyPlanNumPrimaryKeys;
                int end = start + idxOnlyPlanNumPrimaryKeys;

                for (int i = start; i < end; i++) {
                    target.add(idxOnlyPlanKeyVars.get(i));
                }
                break;
            case NO_DATASOURCE:
            default:
                throw new AlgebricksException("The subtree does not have any data source.");
        }
    }

    public List<LogicalVariable> getDataSourceVariables() throws AlgebricksException {
        switch (dataSourceType) {
            case DATASOURCE_SCAN:
            case EXTERNAL_SCAN:
            case PRIMARY_INDEX_LOOKUP:
            case REDUCING_NUMBER_OF_SELECT_PLAN_PRIMARY_INDEX_LOOKUP:
                AbstractScanOperator scanOp = (AbstractScanOperator) dataSourceRef.getValue();
                return scanOp.getVariables();
            case INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP:
                // This data source doesn't have record variables.
                List<LogicalVariable> pkVars = new ArrayList<LogicalVariable>();
                getPrimaryKeyVars(null, pkVars);
                return pkVars;
            case COLLECTION_SCAN:
                return new ArrayList<LogicalVariable>();
            case NO_DATASOURCE:
            default:
                throw new AlgebricksException("The subtree does not have any data source.");
        }
    }

    public List<LogicalVariable> getIxJoinOuterAdditionalDataSourceVariables(int idx) throws AlgebricksException {
        if (ixJoinOuterAdditionalDataSourceRefs != null && ixJoinOuterAdditionalDataSourceRefs.size() > idx) {
            switch (ixJoinOuterAdditionalDataSourceTypes.get(idx)) {
                case DATASOURCE_SCAN:
                case EXTERNAL_SCAN:
                case PRIMARY_INDEX_LOOKUP:
                case REDUCING_NUMBER_OF_SELECT_PLAN_PRIMARY_INDEX_LOOKUP:
                    AbstractScanOperator scanOp = (AbstractScanOperator) ixJoinOuterAdditionalDataSourceRefs.get(idx)
                            .getValue();
                    return scanOp.getVariables();
                case INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP:
                    List<LogicalVariable> pkVars = new ArrayList<LogicalVariable>();
                    getPrimaryKeyVars(ixJoinOuterAdditionalDataSourceRefs.get(idx), pkVars);
                    return pkVars;
                case COLLECTION_SCAN:
                    return new ArrayList<LogicalVariable>();
                case NO_DATASOURCE:
                default:
                    throw new AlgebricksException("The subtree does not have any additional data sources.");
            }
        } else {
            return null;
        }
    }

    public void initializeIxJoinOuterAddtionalDataSourcesIfEmpty() {
        if (ixJoinOuterAdditionalDataSourceRefs == null) {
            ixJoinOuterAdditionalDataSourceRefs = new ArrayList<Mutable<ILogicalOperator>>();
            ixJoinOuterAdditionalDataSourceTypes = new ArrayList<DataSourceType>();
            ixJoinOuterAdditionalDatasets = new ArrayList<Dataset>();
            ixJoinOuterAdditionalRecordTypes = new ArrayList<ARecordType>();
        }
    }

    /**
     * Find and set the first trustworthy index-search or data-scan of this subTree
     */
    public boolean setFirstIndexSearchOrDataScan() {
        // This method assumes that data-source for the subtree is already set.
        if (dataSourceType == null) {
            return false;
        } else {
            switch (dataSourceType) {
                case COLLECTION_SCAN:
                case NO_DATASOURCE:
                    return false;
                default:
                    firstTrustworthyIdxSearchRef = dataSourceRef;
                    return true;
            }
        }

        /*
        // Checks whether there is an unnest-map from the beginning.
        boolean unnestMapFound = false;
        if (assignsAndUnnestsRefs.size() > 0) {
            int i = assignsAndUnnestsRefs.size() - 1;
            while (true) {
                AbstractLogicalOperator lop = (AbstractLogicalOperator) assignsAndUnnestsRefs.get(i).getValue();
                if (lop.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                    // We find an unnest-map. It should be an index-search.
                    unnestMapFound = true;
                    UnnestMapOperator unnestMap = (UnnestMapOperator) lop;
                    ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();
                    if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                        continue;
                    }

                    AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
                    FunctionIdentifier funcIdent = unnestFuncExpr.getFunctionIdentifier();
                    if (!funcIdent.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
                        continue;
                    }
                    firstTrustworthyIdxSearchRef = assignsAndUnnestsRefs.get(i);
                    return true;
                }

                i--;
                if (i < 0) {
                    break;
                }
            }
        }

        // If there is no UNNEST-MAP, checks whether this subtree has data-source scan.
        if (!unnestMapFound && hasDataSourceScan()) {
            firstTrustworthyIdxSearchRef = dataSourceRef;
            return true;
        }

        return false;
        */
    }

}
