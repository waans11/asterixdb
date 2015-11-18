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
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
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

    // First index-search or data-scan in this subtree
    public Mutable<ILogicalOperator> firstIdxSearchRef = null;

    // Contains SELECT operators
    public final List<Mutable<ILogicalOperator>> selectRefs = new ArrayList<Mutable<ILogicalOperator>>();

    /**
     * Initialize assign, unnest and datasource information
     */
    public boolean initFromSubTree(Mutable<ILogicalOperator> subTreeOpRef) {
        reset();
        rootRef = subTreeOpRef;
        root = subTreeOpRef.getValue();
        boolean isIndexOnlyOrReducingTheNumberOfSelectPlan = false;
        // Examine the op's children to match the expected patterns.
        AbstractLogicalOperator subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
        do {
            // Keep limit information if one is present
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.LIMIT) {
                limitRef = subTreeOpRef;
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            }
            // Keep Order by information if one is present
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.ORDER) {
                OrderOperator orderOp = (OrderOperator) subTreeOp;
                subTreeOrderByExpr = orderOp.getOrderExpressions();
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            }

            // If this is an index-only plan or reducing the number of select plan, then we have union operator.
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                isIndexOnlyOrReducingTheNumberOfSelectPlan = true;
                // If this plan has a union operator and turns out to be non-index only plan,
                // then we try to initialize data source and return to the caller.
                if (!initFromIndexOnlyOrReducingTheNumberOfSelectPlan(subTreeOpRef)) {
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
        } while (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT);

        // Match data source (datasource scan or primary index search).
        return initializeDataSource(subTreeOpRef);
    }

    /**
     * Initialize assign, unnest and datasource information for the index-only
     * or reducing the number of select plan.
     */
    private boolean initFromIndexOnlyOrReducingTheNumberOfSelectPlan(Mutable<ILogicalOperator> subTreeOpRef) {

        AbstractLogicalOperator subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
        boolean isIndexOnlyPlan = false;
        boolean isReducingTheNumberOfSelectPlan = false;

        // Top level operator should be UNIONALL operator.
        if (subTreeOp.getOperatorTag() != LogicalOperatorTag.UNIONALL) {
            return false;
        }

        UnionAllOperator unionOp = (UnionAllOperator) subTreeOp;

        // Traverse the left-path first (tryLock fail path).
        // The first operator should be SELECT operator.
        subTreeOpRef = subTreeOp.getInputs().get(0);
        subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();

        if (subTreeOp.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }

        // The second operator can be SPLIT or ASSIGN
        subTreeOpRef = subTreeOp.getInputs().get(0);
        subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();

        return true;
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
        firstIdxSearchRef = null;
        selectRefs.clear();
    }

    public void getPrimaryKeyVars(List<LogicalVariable> target) throws AlgebricksException {
        switch (dataSourceType) {
            case DATASOURCE_SCAN:
                DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) dataSourceRef.getValue();
                int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
                for (int i = 0; i < numPrimaryKeys; i++) {
                    target.add(dataSourceScan.getVariables().get(i));
                }
                break;
            case PRIMARY_INDEX_LOOKUP:
                UnnestMapOperator unnestMapOp = (UnnestMapOperator) dataSourceRef.getValue();
                List<LogicalVariable> primaryKeys = null;
                primaryKeys = AccessMethodUtils.getPrimaryKeyVarsFromPrimaryUnnestMap(dataset, unnestMapOp);
                target.addAll(primaryKeys);
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
                AbstractScanOperator scanOp = (AbstractScanOperator) dataSourceRef.getValue();
                return scanOp.getVariables();
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
                    AbstractScanOperator scanOp = (AbstractScanOperator) ixJoinOuterAdditionalDataSourceRefs.get(idx)
                            .getValue();
                    return scanOp.getVariables();
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
     * Find and set the first index-search or data-scan of this subTree
     */
    public boolean setFirstIndexSearchOrDataScan() {
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

                    firstIdxSearchRef = assignsAndUnnestsRefs.get(i);

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
            firstIdxSearchRef = dataSourceRef;
            return true;
        }

        return false;
    }
}
