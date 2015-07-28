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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.algebra.operators.physical.ExternalDataLookupPOperator;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.external.IndexingConstants;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.hierachy.ATypeHierarchy;
import edu.uci.ics.asterix.om.types.hierachy.ATypeHierarchy.mathFunctionType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Quadruple;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * Static helper functions for rewriting plans using indexes.
 */
public class AccessMethodUtils {

    public static void appendPrimaryIndexTypes(Dataset dataset, IAType itemType, List<Object> target)
            throws IOException, AlgebricksException {
        ARecordType recordType = (ARecordType) itemType;
        List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        for (List<String> partitioningKey : partitioningKeys) {
            target.add(recordType.getSubFieldType(partitioningKey));
        }
        target.add(itemType);
    }

    public static ConstantExpression createStringConstant(String str) {
        return new ConstantExpression(new AsterixConstantValue(new AString(str)));
    }

    public static ConstantExpression createInt32Constant(int i) {
        return new ConstantExpression(new AsterixConstantValue(new AInt32(i)));
    }

    public static ConstantExpression createBooleanConstant(boolean b) {
        if (b) {
            return new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE));
        } else {
            return new ConstantExpression(new AsterixConstantValue(ABoolean.FALSE));
        }
    }

    public static String getStringConstant(Mutable<ILogicalExpression> expr) {
        IAObject obj = ((AsterixConstantValue) ((ConstantExpression) expr.getValue()).getValue()).getObject();
        return ((AString) obj).getStringValue();
    }

    public static int getInt32Constant(Mutable<ILogicalExpression> expr) {
        IAObject obj = ((AsterixConstantValue) ((ConstantExpression) expr.getValue()).getValue()).getObject();
        return ((AInt32) obj).getIntegerValue();
    }

    public static boolean getBooleanConstant(Mutable<ILogicalExpression> expr) {
        IAObject obj = ((AsterixConstantValue) ((ConstantExpression) expr.getValue()).getValue()).getObject();
        return ((ABoolean) obj).getBoolean();
    }

    // Analyzes the given function expression
    // One arg: constant, the other arg: variable
    public static boolean analyzeFuncExprArgsForOneConstAndVar(AbstractFunctionCallExpression funcExpr,
            AccessMethodAnalysisContext analysisCtx) {
        IAlgebricksConstantValue constFilterVal = null;
        LogicalVariable fieldVar = null;
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // One of the arguments must be a constant, and the other argument must be a variable.
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT
                && arg2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            // The arguments of contains-substring() function or contains text() are asymmetrical.
            // We can only use index if the variable is on the first argument
            if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS_SUBSTRING
                    || funcExpr.getFunctionIdentifier() == AlgebricksBuiltinFunctions.CONTAINS) {
                return false;
            }
            ConstantExpression constExpr = (ConstantExpression) arg1;
            constFilterVal = constExpr.getValue();
            VariableReferenceExpression varExpr = (VariableReferenceExpression) arg2;
            fieldVar = varExpr.getVariableReference();
        } else if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE
                && arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression constExpr = (ConstantExpression) arg2;
            constFilterVal = constExpr.getValue();
            VariableReferenceExpression varExpr = (VariableReferenceExpression) arg1;
            fieldVar = varExpr.getVariableReference();
        } else {
            return false;
        }
        OptimizableFuncExpr newOptFuncExpr = new OptimizableFuncExpr(funcExpr, fieldVar, constFilterVal);
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.matchedFuncExprs) {
            //avoid additional optFuncExpressions in case of a join
            if (optFuncExpr.getFuncExpr().equals(funcExpr))
                return true;
        }
        analysisCtx.matchedFuncExprs.add(newOptFuncExpr);
        return true;
    }

    public static boolean analyzeFuncExprArgsForTwoVars(AbstractFunctionCallExpression funcExpr,
            AccessMethodAnalysisContext analysisCtx) {
        LogicalVariable fieldVar1 = null;
        LogicalVariable fieldVar2 = null;
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE
                && arg2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            fieldVar1 = ((VariableReferenceExpression) arg1).getVariableReference();
            fieldVar2 = ((VariableReferenceExpression) arg2).getVariableReference();
        } else {
            return false;
        }
        OptimizableFuncExpr newOptFuncExpr = new OptimizableFuncExpr(funcExpr, new LogicalVariable[] { fieldVar1,
                fieldVar2 }, null);
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.matchedFuncExprs) {
            //avoid additional optFuncExpressions in case of a join
            if (optFuncExpr.getFuncExpr().equals(funcExpr))
                return true;
        }
        analysisCtx.matchedFuncExprs.add(newOptFuncExpr);
        return true;
    }

    public static int getNumSecondaryKeys(Index index, ARecordType recordType) throws AlgebricksException {
        switch (index.getIndexType()) {
            case BTREE:
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX: {
                return index.getKeyFieldNames().size();
            }
            case RTREE: {
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0),
                        index.getKeyFieldNames().get(0), recordType);
                IAType keyType = keyPairType.first;
                int numDimensions = NonTaggedFormatUtil.getNumDimensions(keyType.getTypeTag());
                return numDimensions * 2;
            }
            default: {
                throw new AlgebricksException("Unknown index kind: " + index.getIndexType());
            }
        }
    }

    /**
     * Appends the types of the fields produced by the given secondary index to dest.
     */
    public static void appendSecondaryIndexTypes(Dataset dataset, ARecordType recordType, Index index,
            boolean primaryKeysOnly, List<Object> dest, boolean isIndexOnlyPlanEnabled) throws AlgebricksException {
        if (!primaryKeysOnly) {
            switch (index.getIndexType()) {
                case BTREE:
                case SINGLE_PARTITION_WORD_INVIX:
                case SINGLE_PARTITION_NGRAM_INVIX: {
                    for (int i = 0; i < index.getKeyFieldNames().size(); i++) {
                        Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes()
                                .get(i), index.getKeyFieldNames().get(i), recordType);
                        dest.add(keyPairType.first);
                    }
                    break;
                }
                case RTREE: {
                    Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(
                            index.getKeyFieldTypes().get(0), index.getKeyFieldNames().get(0), recordType);
                    IAType keyType = keyPairType.first;
                    IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());
                    int numKeys = getNumSecondaryKeys(index, recordType);
                    for (int i = 0; i < numKeys; i++) {
                        dest.add(nestedKeyType);
                    }
                    break;
                }
                case LENGTH_PARTITIONED_NGRAM_INVIX:
                case LENGTH_PARTITIONED_WORD_INVIX:
                default:
                    break;
            }
        }
        // Primary keys.
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            //add primary keys
            try {
                appendExternalRecPrimaryKeys(dataset, dest);
            } catch (AsterixException e) {
                throw new AlgebricksException(e);
            }
        } else {
            List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            for (List<String> partitioningKey : partitioningKeys) {
                try {
                    dest.add(recordType.getSubFieldType(partitioningKey));
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }
        }

        // Add one more type to do an optimization using tryLock().
        // We are using AINT32 to decode result values for this.
        // Refer to appendSecondaryIndexOutputVars() for the details.
        if (isIndexOnlyPlanEnabled) {
            dest.add(BuiltinType.AINT32);
        }

    }

    /**
     * Create output variables for the given unnest-map operator that does a secondary index lookup
     */
    public static void appendSecondaryIndexOutputVars(Dataset dataset, ARecordType recordType, Index index,
            boolean primaryKeysOnly, IOptimizationContext context, List<LogicalVariable> dest,
            boolean isIndexOnlyPlanEnabled) throws AlgebricksException {
        int numPrimaryKeys = 0;
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            numPrimaryKeys = IndexingConstants.getRIDSize(dataset);
        } else {
            numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        }
        int numSecondaryKeys = getNumSecondaryKeys(index, recordType);
        int numVars = (primaryKeysOnly) ? numPrimaryKeys : numPrimaryKeys + numSecondaryKeys;

        // If an index can generate a false positive result,
        // then we can't use an optimization since we need to do a post-verification.
        // If not, add one more variables to put the result of tryLock - whether a lock can be granted on a primary key
        // If it is granted, then we don't need to do a post verification (select)
        // If it is not granted, then we need to do a secondary index lookup, sort PKs, do a primary index lookup, and select.
        if (isIndexOnlyPlanEnabled) {
            numVars += 1;
        }

        for (int i = 0; i < numVars; i++) {
            dest.add(context.newVar());
        }
    }

    /**
     * Get the primary key variables from the unnest-map operator that does a secondary index lookup.
     * The order: SK, PK, [Optional: The result of a TryLock on PK]
     *
     * @throws AlgebricksException
     */
    public static List<LogicalVariable> getKeyVarsFromSecondaryUnnestMap(Dataset dataset, ARecordType recordType,
            ILogicalOperator unnestMapOp, Index index, int keyType, boolean outputPrimaryKeysOnlyFromSIdxSearch)
            throws AlgebricksException {
        int numPrimaryKeys;
        int numSecondaryKeys = getNumSecondaryKeys(index, recordType);
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            numPrimaryKeys = IndexingConstants.getRIDSize(dataset);
        } else {
            numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        }
        List<LogicalVariable> keyVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> sourceVars = ((UnnestMapOperator) unnestMapOp).getVariables();
        // Assumes the primary keys are located at the end.
        //        int start = sourceVars.size() - numPrimaryKeys;
        //        int stop = sourceVars.size();
        // Assumes the primary keys are located after the secondary key.
        int start = 0;
        int stop = 0;

        // If a secondary-index search didn't generate SKs
        if (outputPrimaryKeysOnlyFromSIdxSearch) {
            numSecondaryKeys = 0;
        }

        // Fetch primary keys
        switch (keyType) {
            case 0:
                // Fetch primary keys
                start = numSecondaryKeys;
                stop = numSecondaryKeys + numPrimaryKeys;
                break;
            case 1:
                // Fetch secondary keys
                stop = numSecondaryKeys;
                break;
            case 2:
                // Fetch conditional splitter
                start = numSecondaryKeys + numPrimaryKeys;
                stop = sourceVars.size();
        }
        for (int i = start; i < stop; i++) {
            keyVars.add(sourceVars.get(i));
        }
        return keyVars;
    }

    public static List<LogicalVariable> getPrimaryKeyVarsFromPrimaryUnnestMap(Dataset dataset,
            ILogicalOperator unnestMapOp) {
        int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        List<LogicalVariable> primaryKeyVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> sourceVars = ((UnnestMapOperator) unnestMapOp).getVariables();
        // Assumes the primary keys are located at the beginning.
        for (int i = 0; i < numPrimaryKeys; i++) {
            primaryKeyVars.add(sourceVars.get(i));
        }
        return primaryKeyVars;
    }

    /**
     * Returns the search key expression which feeds a secondary-index search. If we are optimizing a selection query then this method returns
     * the a ConstantExpression from the first constant value in the optimizable function expression.
     * If we are optimizing a join, then this method returns the VariableReferenceExpression that should feed the secondary index probe.
     *
     * @throws AlgebricksException
     */
    public static Pair<ILogicalExpression, ILogicalExpression> createSearchKeyExpr(IOptimizableFuncExpr optFuncExpr,
            OptimizableOperatorSubTree indexSubTree, OptimizableOperatorSubTree probeSubTree)
            throws AlgebricksException {
        if (probeSubTree == null) {
            // We are optimizing a selection query. Search key is a constant.
            // Type Checking and type promotion is done here
            IAType fieldType = optFuncExpr.getFieldType(0);
            IAObject constantObj = ((AsterixConstantValue) optFuncExpr.getConstantVal(0)).getObject();
            ATypeTag constantValueTag = constantObj.getType().getTypeTag();

            // type casting applied?
            boolean typeCastingApplied = false;
            // type casting happened from real (FLOAT, DOUBLE) value -> INT value?
            boolean realTypeConvertedToIntegerType = false;
            mathFunctionType mathFunction = mathFunctionType.NONE;
            AsterixConstantValue replacedConstantValue = null;
            AsterixConstantValue replacedConstantValue2 = null;

            // if the constant type and target type does not match, we do a type conversion
            if (constantValueTag != fieldType.getTypeTag()) {
                // To check whether the constant is REAL values, and target field is an INT type field.
                // In this case, we need to change the search parameter. Refer to the caller section for the detail.
                switch (constantValueTag) {
                    case DOUBLE:
                    case FLOAT:
                        switch (fieldType.getTypeTag()) {
                            case INT8:
                            case INT16:
                            case INT32:
                            case INT64:
                                // If a DOUBLE or FLOAT constant is converted to an INT type value,
                                // we need to check a corner case where two real values are located between an INT value.
                                // For example, for the following query,
                                //
                                // for $emp in dataset empDataset
                                // where $emp.age > double("2.3") and $emp.age < double("3.3")
                                // return $emp.id;
                                //
                                // It should generate a result if there is a tuple that satisfies the condition, which is 3,
                                // however, it does not generate the desired result since finding candidates
                                // fail after truncating the fraction part (there is no INT whose value is greater than 2 and less than 3.)
                                //
                                // Thus,
                                // when converting FLOAT or DOUBLE values, we need to apply ceil() or floor().
                                //
                                // LT
                                // IntVar < 4.9 ==> round-up: IntVar < 5
                                //
                                // LE
                                // IntVar <= 4.9  ==> round-down: IntVar <= 4
                                //
                                // GT
                                // IntVar > 4.9 ==> round-down: IntVar > 4
                                //
                                // GE
                                // IntVar >= 4.9 ==> round-up: IntVar >= 5
                                //
                                // EQ
                                // IntVar = 4.3 ==> round-down and round-up: IntVar = 4 and IntVar = 5
                                // IntVar = 4.0 ==> round-down and round-up: IntVar = 4 and IntVar = 4
                                FunctionIdentifier function = optFuncExpr.getFuncExpr().getFunctionIdentifier();
                                if (function == AlgebricksBuiltinFunctions.LT
                                        || function == AlgebricksBuiltinFunctions.GE) {
                                    mathFunction = mathFunctionType.FLOOR; // FLOOR
                                } else if (function == AlgebricksBuiltinFunctions.LE
                                        || function == AlgebricksBuiltinFunctions.GT) {
                                    mathFunction = mathFunctionType.CEIL; // CEIL
                                } else if (function == AlgebricksBuiltinFunctions.EQ) {
                                    mathFunction = mathFunctionType.CEIL_FLOOR; // BOTH
                                }
                                realTypeConvertedToIntegerType = true;
                                break;
                            default:
                                break;
                        }
                    default:
                        break;
                }

                if (mathFunction != mathFunctionType.CEIL_FLOOR) {
                    replacedConstantValue = ATypeHierarchy.getAsterixConstantValueFromNumericTypeObject(constantObj,
                            fieldType.getTypeTag(), mathFunction);
                } else {
                    replacedConstantValue = ATypeHierarchy.getAsterixConstantValueFromNumericTypeObject(constantObj,
                            fieldType.getTypeTag(), mathFunctionType.FLOOR);
                    replacedConstantValue2 = ATypeHierarchy.getAsterixConstantValueFromNumericTypeObject(constantObj,
                            fieldType.getTypeTag(), mathFunctionType.CEIL);
                }
                if (replacedConstantValue != null) {
                    typeCastingApplied = true;
                }

            }

            if (typeCastingApplied) {
                if (replacedConstantValue2 != null) {
                    return new Pair<ILogicalExpression, ILogicalExpression>(new ConstantExpression(
                            replacedConstantValue), new ConstantExpression(replacedConstantValue2));
                } else {
                    return new Pair<ILogicalExpression, ILogicalExpression>(new ConstantExpression(
                            replacedConstantValue), null);
                }
            } else {
                return new Pair<ILogicalExpression, ILogicalExpression>(new ConstantExpression(
                        optFuncExpr.getConstantVal(0)), null);
            }
        } else {
            // We are optimizing a join query. Determine which variable feeds the secondary index.
            if (optFuncExpr.getOperatorSubTree(0) == null || optFuncExpr.getOperatorSubTree(0) == probeSubTree) {
                return new Pair<ILogicalExpression, ILogicalExpression>(new VariableReferenceExpression(
                        optFuncExpr.getLogicalVar(0)), null);
            } else {
                return new Pair<ILogicalExpression, ILogicalExpression>(new VariableReferenceExpression(
                        optFuncExpr.getLogicalVar(1)), null);
            }
        }
    }

    /**
     * Returns the first expr optimizable by this index.
     */
    public static IOptimizableFuncExpr chooseFirstOptFuncExpr(Index chosenIndex, AccessMethodAnalysisContext analysisCtx) {
        List<Pair<Integer, Integer>> indexExprs = analysisCtx.getIndexExprs(chosenIndex);
        int firstExprIndex = indexExprs.get(0).first;
        return analysisCtx.matchedFuncExprs.get(firstExprIndex);
    }

    public static int chooseFirstOptFuncVar(Index chosenIndex, AccessMethodAnalysisContext analysisCtx) {
        List<Pair<Integer, Integer>> indexExprs = analysisCtx.getIndexExprs(chosenIndex);
        return indexExprs.get(0).second;
    }

    /**
     * Check whether the given plan is an index-only plan
     * Returns the following:
     * 1. isIndexOnlyPlan?
     * 2. secondaryKeyFieldUsedInSelectCondition?
     * 3. secondaryKeyFieldUsedAfterSelectOp?
     * 4. verificationAfterSIdxSearchRequired?
     */
    public static Quadruple<Boolean, Boolean, Boolean, Boolean> isIndexOnlyPlan(
            List<Mutable<ILogicalOperator>> aboveSelectRefs, Mutable<ILogicalOperator> selectRef,
            OptimizableOperatorSubTree subTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context) throws AlgebricksException {

        // index-only plan, a.k.a. TryLock() on PK optimization:
        // If the given secondary index can't generate false-positive results (a super-set of the true result set) and
        // the plan only deals with PK, and/or SK,
        // we can generate an optimized plan that does not require verification for PKs where
        // tryLock() is succeeded.
        // If tryLock() on PK from a secondary index search is succeeded,
        // SK, PK from a secondary index-search will be fed into Union Operators without looking the primary index.
        // If fails, a primary-index lookup will be placed and the results will be fed into Union Operators.
        // So, we need to push-down select and assign (unnest) to the after primary-index lookup.

        // secondary key field usage in the select condition
        boolean secondaryKeyFieldUsedInSelectCondition = false;

        // secondary key field usage after the select operator
        boolean secondaryKeyFieldUsedAfterSelectOp = false;

        // For R-Tree only: whether a verification is required after the secondary index search
        boolean verificationAfterSIdxSearchRequired = true;

        // logical variables that select operator is using
        List<LogicalVariable> usedVarsInSelectTemp = new ArrayList<LogicalVariable>();
        List<LogicalVariable> usedVarsInSelect = new ArrayList<LogicalVariable>();

        // live variables that select operator can access
        List<LogicalVariable> liveVarsInSelect = new ArrayList<LogicalVariable>();

        // PK, record variable
        List<LogicalVariable> dataScanPKRecordVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> dataScanPKVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> dataScanRecordVars = new ArrayList<LogicalVariable>();

        // From now on, check whether the given plan is an index-only plan
        VariableUtilities.getUsedVariables((ILogicalOperator) selectRef.getValue(), usedVarsInSelectTemp);

        // Remove the duplicated variables used in the SELECT operator
        for (int i = 0; i < usedVarsInSelectTemp.size(); i++) {
            if (!usedVarsInSelect.contains(usedVarsInSelectTemp.get(i))) {
                usedVarsInSelect.add(usedVarsInSelectTemp.get(i));
            }
        }
        usedVarsInSelectTemp.clear();

        // Get the live variables in the SELECT operator
        VariableUtilities.getLiveVariables((ILogicalOperator) selectRef.getValue(), liveVarsInSelect);

        // Get PK, record variables
        dataScanPKRecordVars = subTree.getDataSourceVariables();
        // In external dataset, there is no PK.
        if (dataScanPKRecordVars.size() > 1) {
            subTree.getPrimaryKeyVars(dataScanPKVars);
        }
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

        boolean isIndexOnlyPlanPossible = false;

        // #1. Check whether variables in the SELECT operator are from secondary key fields and/or PK fields
        int selectVarFoundCount = 0;
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

        // #2. Check whether operators after the SELECT operator only use PK or secondary field variables.
        //     We exclude the variables produced after the SELECT operator.
        boolean countIsUsedInThePlan = false;
        List<LogicalVariable> countUsedVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> producedVarsAfterSelect = new ArrayList<LogicalVariable>();

        // From now on, check whether the given plan is an index-only plan
        // VariableUtilities.getUsedVariables((ILogicalOperator) selectRef.getValue(), usedVarsInSelect);

        AbstractLogicalOperator aboveSelectRefOp = null;
        AggregateOperator aggOp = null;
        ILogicalExpression condExpr = null;
        List<Mutable<ILogicalExpression>> condExprs = null;
        AbstractFunctionCallExpression condExprFnCall = null;

        if (isIndexOnlyPlanPossible) {
            if (aboveSelectRefs == null) {
                isIndexOnlyPlanPossible = false;
            } else {
                List<LogicalVariable> usedVarsAfterSelect = new ArrayList<LogicalVariable>();
                // for each operator above SELECT operator
                for (Mutable<ILogicalOperator> aboveSelectRef : aboveSelectRefs) {
                    usedVarsAfterSelect.clear();
                    producedVarsAfterSelect.clear();
                    VariableUtilities.getUsedVariables((ILogicalOperator) aboveSelectRef.getValue(),
                            usedVarsAfterSelect);
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
                                if (chosenIndex.getIndexType() == IndexType.BTREE
                                        || chosenIndex.getIndexType() == IndexType.RTREE) {
                                    // From SK?
                                    isIndexOnlyPlanPossible = true;
                                    secondaryKeyFieldUsedAfterSelectOp = true;
                                } else if (chosenIndex.getIndexType() == IndexType.SINGLE_PARTITION_WORD_INVIX
                                        || chosenIndex.getIndexType() == IndexType.SINGLE_PARTITION_NGRAM_INVIX
                                        || chosenIndex.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                                        || chosenIndex.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
                                    // Inverted Index Case.
                                    // Unlike B+Tree or R-Tree on POINT or RECTANGLE type, we can't use or reconstruct
                                    // secondary key field value from SK since a SK is just part of a field value.
                                    // Therefore, if a secondary key field value is used after SELECT operator, this cannot be
                                    // an index-only select plan. Therefore, we only check whether PK is used after SELECT operator.
                                    // We exclude checking on the variables produced after the SELECT operator.
                                    isIndexOnlyPlanPossible = false;
                                    secondaryKeyFieldUsedAfterSelectOp = true;
                                    break;
                                }
                            } else if (subTree.fieldNames.containsKey(usedVarAfterSelect)) {
                                // If ASSIGNs or UNNESTs before SELECT operator contains the given variable and
                                // the given variable is a secondary key field (this happens when we have a composite secondary index)
                                if (chosenIndexFieldNames.contains(subTree.fieldNames.get(usedVarAfterSelect))) {
                                    isIndexOnlyPlanPossible = true;
                                    secondaryKeyFieldUsedAfterSelectOp = true;
                                } else {
                                    // Non-PK or non-secondary key field is used after SELECT operator.
                                    // This is not an index-only plan.
                                    isIndexOnlyPlanPossible = false;
                                    break;
                                }
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

        }

        // For R-Tree only check condition:
        // At this point, we are sure that either an index-only plan is possible or not.
        // If the following two conditions are met, then we don't need to do a post-processing.
        // That is, the given index will not generate false positive results.
        // If not, we need to put "select" condition to the path where tryLock on PK succeeds, too.
        // 1) Query shape should be rectangle and
        // 2) key field type of the index should be either point or rectangle.

        if (isIndexOnlyPlanPossible && chosenIndex.getIndexType() == IndexType.RTREE) {
            // TODO: We can probably do something smarter here based on selectivity or MBR area.
            IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(chosenIndex, analysisCtx);
            ARecordType recordType = subTree.recordType;
            int optFieldIdx = AccessMethodUtils.chooseFirstOptFuncVar(chosenIndex, analysisCtx);
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(
                    optFuncExpr.getFieldType(optFieldIdx), optFuncExpr.getFieldName(optFieldIdx), recordType);
            if (keyPairType == null) {
                return null;
            }

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
                                if (keyPairType.first == BuiltinType.APOINT
                                        || keyPairType.first == BuiltinType.ARECTANGLE) {
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
        }

        return new Quadruple<Boolean, Boolean, Boolean, Boolean>(isIndexOnlyPlanPossible,
                secondaryKeyFieldUsedInSelectCondition, secondaryKeyFieldUsedAfterSelectOp,
                verificationAfterSIdxSearchRequired);
    }

    /**
     * Create an unnest-map operator that does a secondary index lookup
     */
    public static UnnestMapOperator createSecondaryIndexUnnestMap(Dataset dataset, ARecordType recordType, Index index,
            ILogicalOperator inputOp, AccessMethodJobGenParams jobGenParams, IOptimizationContext context,
            boolean outputPrimaryKeysOnly, boolean retainInput, boolean isIndexOnlyPlanEnabled)
            throws AlgebricksException {
        // The job gen parameters are transferred to the actual job gen via the UnnestMapOperator's function arguments.
        ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        jobGenParams.writeToFuncArgs(secondaryIndexFuncArgs);
        // Variables and types coming out of the secondary-index search.
        List<LogicalVariable> secondaryIndexUnnestVars = new ArrayList<LogicalVariable>();
        List<Object> secondaryIndexOutputTypes = new ArrayList<Object>();
        // Append output variables/types generated by the secondary-index search (not forwarded from input).
        // Output: SK, PK, [Optional: The result of TryLock]
        appendSecondaryIndexOutputVars(dataset, recordType, index, outputPrimaryKeysOnly, context,
                secondaryIndexUnnestVars, isIndexOnlyPlanEnabled);
        appendSecondaryIndexTypes(dataset, recordType, index, outputPrimaryKeysOnly, secondaryIndexOutputTypes,
                isIndexOnlyPlanEnabled);
        // An index search is expressed as an unnest-map over an index-search function.
        IFunctionInfo secondaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression secondaryIndexSearchFunc = new UnnestingFunctionCallExpression(
                secondaryIndexSearch, secondaryIndexFuncArgs);
        secondaryIndexSearchFunc.setReturnsUniqueValues(true);
        // This is the operator that jobgen will be looking for. It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.
        UnnestMapOperator secondaryIndexUnnestOp = new UnnestMapOperator(secondaryIndexUnnestVars,
                new MutableObject<ILogicalExpression>(secondaryIndexSearchFunc), secondaryIndexOutputTypes, retainInput);
        secondaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        context.computeAndSetTypeEnvironmentForOperator(secondaryIndexUnnestOp);
        secondaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
        return secondaryIndexUnnestOp;
    }

    /**
     * Create an unnest-map operator that does a primary index lookup
     */
    public static ILogicalOperator createPrimaryIndexUnnestMap(List<Mutable<ILogicalOperator>> afterTopOpRefs,
            Mutable<ILogicalOperator> topOpRef, Mutable<ILogicalOperator> assignBeforeTopOpRef,
            AbstractDataSourceOperator dataSourceOp, Dataset dataset, ARecordType recordType, ILogicalOperator inputOp,
            IOptimizationContext context, boolean sortPrimaryKeys, boolean retainInput, boolean retainNull,
            boolean requiresBroadcast, Index secondaryIndex, AccessMethodAnalysisContext analysisCtx,
            boolean outputPrimaryKeysOnlyFromSIdxSearch, boolean verificationAfterSIdxSearchRequired,
            boolean secondaryKeyFieldUsedInSelectCondition, boolean secondaryKeyFieldUsedAfterSelectOp,
            OptimizableOperatorSubTree subTree) throws AlgebricksException {

        // If the chosen secondary index cannot generate false positive results,
        // and this is an index-only plan (using PK and/or secondary field after SELECT operator),
        // we can apply tryLock() on PK optimization since a result from these indexes
        // since a result doesn't have to be verified by a primary index-lookup.
        // left path: if a tryLock() on PK fails:
        //             secondary index-search -> conditional split -> primary index-search -> verification (select) -> union ->
        // right path: secondary index-search -> conditional split -> union -> ...

        // The following information is required only when tryLock() on PK optimization is possible.
        AbstractLogicalOperator afterTopOp = null;
        SelectOperator topOp = null;
        AssignOperator assignBeforeTopOp = null;
        UnionAllOperator unionAllOp = null;
        SelectOperator newSelectOp = null;
        SelectOperator newSelectOp2 = null;
        AssignOperator newAssignOp = null;
        AssignOperator newAssignOp2 = null;
        SplitOperator splitOp = null;
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> unionVarMap = null;
        List<LogicalVariable> conditionalSplitVars = null;
        boolean isIndexOnlyPlanEnabled = analysisCtx.isIndexOnlyPlanEnabled();
        List<LogicalVariable> fetchedSecondaryKeyFieldVarsFromPIdxLookUp = null;

        IndexType idxType = secondaryIndex.getIndexType();

        // Fetch SK variable from secondary-index search
        List<LogicalVariable> secondaryKeyVars = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset,
                recordType, inputOp, secondaryIndex, 1, outputPrimaryKeysOnlyFromSIdxSearch);

        List<List<String>> chosenIndexFieldNames = secondaryIndex.getKeyFieldNames();

        // If the secondary key field is used after SELECT operator, then we need to keep secondary keys.
        // However, in case of R tree, the result of R-tree index search is an MBR.
        // So, we need to reconstruct original values from the result.
        AssignOperator assignSecondaryKeyFieldOp = null;
        List<LogicalVariable> restoredSecondaryKeyFieldVars = null;
        ArrayList<Mutable<ILogicalExpression>> restoredSecondaryKeyFieldExprs = null;
        IAType spatialType = null;

        if (isIndexOnlyPlanEnabled && (secondaryKeyFieldUsedAfterSelectOp || verificationAfterSIdxSearchRequired)
                && idxType == IndexType.RTREE) {
            IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(secondaryIndex, analysisCtx);
            int optFieldIdx = AccessMethodUtils.chooseFirstOptFuncVar(secondaryIndex, analysisCtx);
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(
                    optFuncExpr.getFieldType(optFieldIdx), optFuncExpr.getFieldName(optFieldIdx), recordType);
            if (keyPairType == null) {
                return null;
            }

            // Get the number of dimensions corresponding to the field indexed by chosenIndex.
            spatialType = keyPairType.first;

            restoredSecondaryKeyFieldExprs = new ArrayList<Mutable<ILogicalExpression>>();
            restoredSecondaryKeyFieldVars = new ArrayList<LogicalVariable>();

            if (spatialType == BuiltinType.APOINT) {
                // generate APoint values
                AbstractFunctionCallExpression createPoint = new ScalarFunctionCallExpression(
                        FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CREATE_POINT));
                List<Mutable<ILogicalExpression>> expressions = new ArrayList<Mutable<ILogicalExpression>>();
                expressions.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVars
                        .get(0))));
                expressions.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVars
                        .get(1))));
                createPoint.getArguments().addAll(expressions);
                restoredSecondaryKeyFieldVars.add(context.newVar());
                restoredSecondaryKeyFieldExprs.add(new MutableObject<ILogicalExpression>(createPoint));
                assignSecondaryKeyFieldOp = new AssignOperator(restoredSecondaryKeyFieldVars,
                        restoredSecondaryKeyFieldExprs);
            } else if (spatialType == BuiltinType.ARECTANGLE) {
                // generate ARectangle values
                AbstractFunctionCallExpression createRectangle = new ScalarFunctionCallExpression(
                        FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CREATE_RECTANGLE));
                List<Mutable<ILogicalExpression>> expressions = new ArrayList<Mutable<ILogicalExpression>>();
                expressions.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVars
                        .get(0))));
                expressions.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVars
                        .get(1))));
                expressions.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVars
                        .get(2))));
                expressions.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVars
                        .get(3))));
                createRectangle.getArguments().addAll(expressions);
                restoredSecondaryKeyFieldVars.add(context.newVar());
                restoredSecondaryKeyFieldExprs.add(new MutableObject<ILogicalExpression>(createRectangle));
                assignSecondaryKeyFieldOp = new AssignOperator(restoredSecondaryKeyFieldVars,
                        restoredSecondaryKeyFieldExprs);
            }
        }

        // Fetch PK variable from secondary-index search
        List<LogicalVariable> primaryKeyVars = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset, recordType,
                inputOp, secondaryIndex, 0, outputPrimaryKeysOnlyFromSIdxSearch);

        List<List<String>> PKfieldNames = DatasetUtils.getPartitioningKeys(dataset);

        // Variables and types coming out of the primary-index search.
        List<LogicalVariable> primaryIndexUnnestVars = new ArrayList<LogicalVariable>();
        List<Object> primaryIndexOutputTypes = new ArrayList<Object>();
        // Append output variables/types generated by the primary-index search (not forwarded from input).
        primaryIndexUnnestVars.addAll(dataSourceOp.getVariables());
        try {
            appendPrimaryIndexTypes(dataset, recordType, primaryIndexOutputTypes);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }

        // If this plan is an index-only plan, add a SPLIT operator to propagate <SK, PK> pair from
        // the secondary-index search to the two paths
        List<LogicalVariable> varsUsedInTopOp = null;
        List<LogicalVariable> varsUsedInAssignUnnestBeforeTopOp = null;
        if (isIndexOnlyPlanEnabled) {
            // If there is an assign operator before SELECT operator, we need to propagate
            // this variable to the UNION operator too.

            // variables used in ASSIGN before SELECT operator
            varsUsedInAssignUnnestBeforeTopOp = new ArrayList<LogicalVariable>();

            if (assignBeforeTopOpRef != null) {
                assignBeforeTopOp = (AssignOperator) assignBeforeTopOpRef.getValue();
                VariableUtilities.getProducedVariables((ILogicalOperator) assignBeforeTopOp,
                        varsUsedInAssignUnnestBeforeTopOp);
            }

            // variable map that will be used as input to UNION operator: <left, right, output>
            // In our case, left: tryLock fail path, right: tryLock success path
            unionVarMap = new ArrayList<Triple<LogicalVariable, LogicalVariable, LogicalVariable>>();

            // variables used after SELECT operator
            List<LogicalVariable> varsUsedAfterTopOp = new ArrayList<LogicalVariable>();

            // variables used in SELECT operator
            varsUsedInTopOp = new ArrayList<LogicalVariable>();
            List<LogicalVariable> tmpVars = new ArrayList<LogicalVariable>();

            // Get used variables from the SELECT operators
            VariableUtilities.getUsedVariables((ILogicalOperator) topOpRef.getValue(), varsUsedInTopOp);

            // Generate the list of variables that are used after the SELECT operator
            for (Mutable<ILogicalOperator> afterTopOpRef : afterTopOpRefs) {
                tmpVars.clear();
                VariableUtilities.getUsedVariables((ILogicalOperator) afterTopOpRef.getValue(), tmpVars);
                for (LogicalVariable tmpVar : tmpVars) {
                    if (!varsUsedAfterTopOp.contains(tmpVar)) {
                        varsUsedAfterTopOp.add(tmpVar);
                    }
                }
            }

            // Is the used variables after SELECT operator from the primary index?
            boolean varAlreadyAdded = false;
            for (Iterator<LogicalVariable> iterator = varsUsedAfterTopOp.iterator(); iterator.hasNext();) {
                LogicalVariable tVar = iterator.next();

                // Check whether this variable is already added to the union variable map
                for (Iterator<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> it = unionVarMap.iterator(); it
                        .hasNext();) {
                    LogicalVariable dupCheckVar = it.next().first;
                    if (dupCheckVar.equals(tVar)) {
                        varAlreadyAdded = true;
                        break;
                    }
                }
                if (primaryIndexUnnestVars.contains(tVar) && !varAlreadyAdded) {
                    int pIndexPKIdx = primaryIndexUnnestVars.indexOf(tVar);
                    unionVarMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(tVar, primaryKeyVars
                            .get(pIndexPKIdx), tVar));
                    iterator.remove();
                    varsUsedInTopOp.remove(tVar);
                }
            }

            // [R-Tree specific]
            // we need this variable in case of R-Tree index if we need an additional verification, or
            // the secondary key field is used after SELECT operator

            // Is the used variables after SELECT operator from the given secondary index?
            varAlreadyAdded = false;
            for (Iterator<LogicalVariable> iterator = varsUsedAfterTopOp.iterator(); iterator.hasNext();) {
                LogicalVariable tVar = iterator.next();
                // Check whether this variable is already added to the union variable map
                for (Iterator<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> it = unionVarMap.iterator(); it
                        .hasNext();) {
                    LogicalVariable dupCheckVar = it.next().first;
                    if (dupCheckVar.equals(tVar)) {
                        varAlreadyAdded = true;
                        break;
                    }
                }
                if (varsUsedInTopOp.contains(tVar)) {
                    if (idxType != IndexType.RTREE) {
                        int sIndexIdx = chosenIndexFieldNames.indexOf(subTree.fieldNames.get(tVar));

                        unionVarMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(tVar,
                                secondaryKeyVars.get(sIndexIdx), tVar));
                    } else {
                        if (fetchedSecondaryKeyFieldVarsFromPIdxLookUp == null) {
                            fetchedSecondaryKeyFieldVarsFromPIdxLookUp = new ArrayList<LogicalVariable>();
                        }
                        // if the given index is R-Tree, we need to use the re-constructed secondary key from
                        // the r-tree search
                        int sIndexIdx = chosenIndexFieldNames.indexOf(subTree.fieldNames.get(tVar));

                        unionVarMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(tVar,
                                restoredSecondaryKeyFieldVars.get(sIndexIdx), tVar));
                        fetchedSecondaryKeyFieldVarsFromPIdxLookUp.add(tVar);
                    }
                    iterator.remove();
                    varsUsedInTopOp.remove(tVar);
                } else if (varsUsedInAssignUnnestBeforeTopOp.contains(tVar)) {
                    int sIndexIdx = chosenIndexFieldNames.indexOf(subTree.fieldNames.get(tVar));
                    unionVarMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(tVar,
                            secondaryKeyVars.get(sIndexIdx), tVar));
                }
            }

            // Fetch Conditional Split variable from secondary-index search
            conditionalSplitVars = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset, recordType, inputOp,
                    secondaryIndex, 2, outputPrimaryKeysOnlyFromSIdxSearch);

            splitOp = new SplitOperator(2, conditionalSplitVars.get(0));
            splitOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
            splitOp.setExecutionMode(ExecutionMode.PARTITIONED);
            context.computeAndSetTypeEnvironmentForOperator(splitOp);
        }

        // Optionally add a sort on the primary-index keys before searching the primary index.
        // If tryLock() optimization is possible, this ORDER (sort) operator is not necessary by our design choice.
        OrderOperator order = null;
        if (sortPrimaryKeys && !isIndexOnlyPlanEnabled) {
            order = new OrderOperator();
            for (LogicalVariable pkVar : primaryKeyVars) {
                Mutable<ILogicalExpression> vRef = new MutableObject<ILogicalExpression>(
                        new VariableReferenceExpression(pkVar));
                order.getOrderExpressions().add(
                        new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, vRef));
            }
            // The secondary-index search feeds into the sort.
            order.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
            order.setExecutionMode(ExecutionMode.LOCAL);
            context.computeAndSetTypeEnvironmentForOperator(order);
        }

        // Create the primary index lookup operator
        // The job gen parameters are transferred to the actual job gen via the UnnestMapOperator's function arguments.
        List<Mutable<ILogicalExpression>> primaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        BTreeJobGenParams jobGenParams = new BTreeJobGenParams(dataset.getDatasetName(), IndexType.BTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, retainNull, requiresBroadcast, false);
        // Set low/high inclusive to true for a point lookup.
        jobGenParams.setLowKeyInclusive(true);
        jobGenParams.setHighKeyInclusive(true);
        jobGenParams.setLowKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.setHighKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.setIsEqCondition(true);
        jobGenParams.writeToFuncArgs(primaryIndexFuncArgs);

        // Primary index search is expressed as an unnest over an index-search function.
        IFunctionInfo primaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        AbstractFunctionCallExpression primaryIndexSearchFunc = new ScalarFunctionCallExpression(primaryIndexSearch,
                primaryIndexFuncArgs);
        // This is the operator that job gen will be looking for.
        // It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.
        UnnestMapOperator primaryIndexUnnestOp = new UnnestMapOperator(primaryIndexUnnestVars,
                new MutableObject<ILogicalExpression>(primaryIndexSearchFunc), primaryIndexOutputTypes, retainInput);

        // Fed by the order operator or the secondaryIndexUnnestOp.
        // In case of an index-only plan, SPLIT operator will be fed into the primary index lookup
        if (sortPrimaryKeys && !isIndexOnlyPlanEnabled) {
            primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(order));
        } else {
            if (isIndexOnlyPlanEnabled) {
                // If index-only plan is possible, the splitOp provides PK into this primary-index search
                primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(splitOp));
            } else {
                primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
            }
        }
        primaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
        context.computeAndSetTypeEnvironmentForOperator(primaryIndexUnnestOp);

        // Generate UnionOperator to merge the left and right paths
        if (isIndexOnlyPlanEnabled) {
            // Copy the original SELECT operator and put it after the primary index lookup
            topOp = (SelectOperator) topOpRef.getValue();

            newSelectOp = new SelectOperator(topOp.getCondition(), topOp.getRetainNull(),
                    topOp.getNullPlaceholderVariable());

            // If there is an ASSIGN operator before SELECT operator, we need to put this before SELECT operator,
            // and after the primary index lookup.
            if (assignBeforeTopOp != null) {
                newAssignOp = new AssignOperator(assignBeforeTopOp.getVariables(), assignBeforeTopOp.getExpressions());
                newAssignOp.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestOp));
                newAssignOp.setExecutionMode(assignBeforeTopOp.getExecutionMode());
                context.computeAndSetTypeEnvironmentForOperator(newAssignOp);
                newSelectOp.getInputs().add(new MutableObject<ILogicalOperator>(newAssignOp));
            } else {
                newSelectOp.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestOp));
            }

            newSelectOp.setExecutionMode(topOp.getExecutionMode());
            context.computeAndSetTypeEnvironmentForOperator(newSelectOp);

            ILogicalOperator rightTopOp = splitOp;

            // For an R-Tree index, if there is an operator that is using the secondary key field value,
            // we need to reconstruct that field value from the result of R-Tree search.
            // This is done by adding the assign operator that we have made in the beginning of this method
            if (idxType == IndexType.RTREE
                    && (secondaryKeyFieldUsedAfterSelectOp || verificationAfterSIdxSearchRequired)) {
                assignSecondaryKeyFieldOp.getInputs().add(new MutableObject<ILogicalOperator>(splitOp));
                assignSecondaryKeyFieldOp.setExecutionMode(assignBeforeTopOp.getExecutionMode());
                context.computeAndSetTypeEnvironmentForOperator(assignSecondaryKeyFieldOp);
                rightTopOp = assignSecondaryKeyFieldOp;
            }

            // For an R-Tree index, if the given query shape is not RECTANGLE or POINT,
            // we need to add the original SELECT operator to filter out the false positive results.
            // (e.g., spatial-intersect($o.pointfield, create-circle(create-point(30.0,70.0), 5.0)) )
            if (idxType == IndexType.RTREE && verificationAfterSIdxSearchRequired) {
                newSelectOp2 = (SelectOperator) OperatorManipulationUtil.deepCopy(topOp);
                //                        new SelectOperator( .topOp.getCondition(), topOp.getRetainNull(),
                //                        topOp.getNullPlaceholderVariable());
                newSelectOp2.getInputs().clear();
                newSelectOp2.getInputs().add(new MutableObject<ILogicalOperator>(assignSecondaryKeyFieldOp));

                ILogicalExpression condExpr = newSelectOp2.getCondition().getValue();
                if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    return null;
                } else {
                    AbstractFunctionCallExpression condExprFnCall = (AbstractFunctionCallExpression) condExpr;
                    // If this list is null, then secondary key field is not used after SELECT operator.
                    // However, we need to put the secondary key field anyway
                    if (fetchedSecondaryKeyFieldVarsFromPIdxLookUp != null) {
                        for (int i = 0; i < fetchedSecondaryKeyFieldVarsFromPIdxLookUp.size(); i++) {
                            condExprFnCall.substituteVar(fetchedSecondaryKeyFieldVarsFromPIdxLookUp.get(i),
                                    restoredSecondaryKeyFieldVars.get(i));
                        }
                    } else {
                        for (int i = 0; i < varsUsedInTopOp.size(); i++) {
                            condExprFnCall.substituteVar(varsUsedInTopOp.get(i), restoredSecondaryKeyFieldVars.get(i));
                        }
                    }
                }
                newSelectOp2.setExecutionMode(topOp.getExecutionMode());
                context.computeAndSetTypeEnvironmentForOperator(newSelectOp2);
                rightTopOp = newSelectOp2;
            }

            unionAllOp = new UnionAllOperator(unionVarMap);
            unionAllOp.getInputs().add(new MutableObject<ILogicalOperator>(newSelectOp));
            unionAllOp.getInputs().add(new MutableObject<ILogicalOperator>(rightTopOp));
            context.computeAndSetTypeEnvironmentForOperator(unionAllOp);

            return unionAllOp;
        } else {
            return primaryIndexUnnestOp;
        }

    }

    public static ScalarFunctionCallExpression findLOJIsNullFuncInGroupBy(GroupByOperator lojGroupbyOp)
            throws AlgebricksException {
        //find IS_NULL function of which argument has the nullPlaceholder variable in the nested plan of groupby.
        ALogicalPlanImpl subPlan = (ALogicalPlanImpl) lojGroupbyOp.getNestedPlans().get(0);
        Mutable<ILogicalOperator> subPlanRootOpRef = subPlan.getRoots().get(0);
        AbstractLogicalOperator subPlanRootOp = (AbstractLogicalOperator) subPlanRootOpRef.getValue();
        boolean foundSelectNonNull = false;
        ScalarFunctionCallExpression isNullFuncExpr = null;
        AbstractLogicalOperator inputOp = subPlanRootOp;
        while (inputOp != null) {
            if (inputOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                SelectOperator selectOp = (SelectOperator) inputOp;
                if (selectOp.getCondition().getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    if (((AbstractFunctionCallExpression) selectOp.getCondition().getValue()).getFunctionIdentifier()
                            .equals(AlgebricksBuiltinFunctions.NOT)) {
                        ScalarFunctionCallExpression notFuncExpr = (ScalarFunctionCallExpression) selectOp
                                .getCondition().getValue();
                        if (notFuncExpr.getArguments().get(0).getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                            if (((AbstractFunctionCallExpression) notFuncExpr.getArguments().get(0).getValue())
                                    .getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.IS_NULL)) {
                                isNullFuncExpr = (ScalarFunctionCallExpression) notFuncExpr.getArguments().get(0)
                                        .getValue();
                                if (isNullFuncExpr.getArguments().get(0).getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                                    foundSelectNonNull = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            inputOp = inputOp.getInputs().size() > 0 ? (AbstractLogicalOperator) inputOp.getInputs().get(0).getValue()
                    : null;
        }

        if (!foundSelectNonNull) {
            throw new AlgebricksException(
                    "Could not find the non-null select operator in GroupByOperator for LEFTOUTERJOIN plan optimization.");
        }
        return isNullFuncExpr;
    }

    public static void resetLOJNullPlaceholderVariableInGroupByOp(AccessMethodAnalysisContext analysisCtx,
            LogicalVariable newNullPlaceholderVaraible, IOptimizationContext context) throws AlgebricksException {

        //reset the null placeholder variable in groupby operator
        ScalarFunctionCallExpression isNullFuncExpr = analysisCtx.getLOJIsNullFuncInGroupBy();
        isNullFuncExpr.getArguments().clear();
        isNullFuncExpr.getArguments().add(
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(newNullPlaceholderVaraible)));

        //recompute type environment.
        OperatorPropertiesUtil.typeOpRec(analysisCtx.getLOJGroupbyOpRef(), context);
    }

    // New < For external datasets indexing>
    private static void appendExternalRecTypes(Dataset dataset, IAType itemType, List<Object> target) {
        target.add(itemType);
    }

    private static void appendExternalRecPrimaryKeys(Dataset dataset, List<Object> target) throws AsterixException {
        int numPrimaryKeys = IndexingConstants.getRIDSize(dataset);
        for (int i = 0; i < numPrimaryKeys; i++) {
            target.add(IndexingConstants.getFieldType(i));
        }
    }

    private static void writeVarList(List<LogicalVariable> varList, List<Mutable<ILogicalExpression>> funcArgs) {
        Mutable<ILogicalExpression> numKeysRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                new AsterixConstantValue(new AInt32(varList.size()))));
        funcArgs.add(numKeysRef);
        for (LogicalVariable keyVar : varList) {
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(
                    new VariableReferenceExpression(keyVar));
            funcArgs.add(keyVarRef);
        }
    }

    private static void addStringArg(String argument, List<Mutable<ILogicalExpression>> funcArgs) {
        Mutable<ILogicalExpression> stringRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                new AsterixConstantValue(new AString(argument))));
        funcArgs.add(stringRef);
    }

    public static ExternalDataLookupOperator createExternalDataLookupUnnestMap(AbstractDataSourceOperator dataSourceOp,
            Dataset dataset, ARecordType recordType, ILogicalOperator inputOp, IOptimizationContext context,
            Index secondaryIndex, boolean retainInput, boolean retainNull, boolean outputPrimaryKeysOnlyFromSIdxSearch)
            throws AlgebricksException {
        List<LogicalVariable> primaryKeyVars = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset, recordType,
                inputOp, secondaryIndex, 0, outputPrimaryKeysOnlyFromSIdxSearch);

        // add a sort on the RID fields before fetching external data.
        OrderOperator order = new OrderOperator();
        for (LogicalVariable pkVar : primaryKeyVars) {
            Mutable<ILogicalExpression> vRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                    pkVar));
            order.getOrderExpressions().add(
                    new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, vRef));
        }
        // The secondary-index search feeds into the sort.
        order.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        order.setExecutionMode(ExecutionMode.LOCAL);
        context.computeAndSetTypeEnvironmentForOperator(order);
        List<Mutable<ILogicalExpression>> externalRIDAccessFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        //Add dataverse and dataset to the arguments
        AccessMethodUtils.addStringArg(dataset.getDataverseName(), externalRIDAccessFuncArgs);
        AccessMethodUtils.addStringArg(dataset.getDatasetName(), externalRIDAccessFuncArgs);
        AccessMethodUtils.writeVarList(primaryKeyVars, externalRIDAccessFuncArgs);

        // Variables and types coming out of the external access.
        List<LogicalVariable> externalAccessByRIDVars = new ArrayList<LogicalVariable>();
        List<Object> externalAccessOutputTypes = new ArrayList<Object>();
        // Append output variables/types generated by the data scan (not forwarded from input).
        externalAccessByRIDVars.addAll(dataSourceOp.getVariables());
        appendExternalRecTypes(dataset, recordType, externalAccessOutputTypes);

        IFunctionInfo externalAccessByRID = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.EXTERNAL_LOOKUP);
        AbstractFunctionCallExpression externalAccessFunc = new ScalarFunctionCallExpression(externalAccessByRID,
                externalRIDAccessFuncArgs);

        ExternalDataLookupOperator externalLookupOp = new ExternalDataLookupOperator(externalAccessByRIDVars,
                new MutableObject<ILogicalExpression>(externalAccessFunc), externalAccessOutputTypes, retainInput,
                dataSourceOp.getDataSource());
        // Fed by the order operator or the secondaryIndexUnnestOp.
        externalLookupOp.getInputs().add(new MutableObject<ILogicalOperator>(order));

        context.computeAndSetTypeEnvironmentForOperator(externalLookupOp);
        externalLookupOp.setExecutionMode(ExecutionMode.PARTITIONED);

        //set the physical operator
        AqlSourceId dataSourceId = new AqlSourceId(dataset.getDataverseName(), dataset.getDatasetName());
        externalLookupOp.setPhysicalOperator(new ExternalDataLookupPOperator(dataSourceId, dataset, recordType,
                secondaryIndex, primaryKeyVars, false, retainInput, retainNull));
        return externalLookupOp;
    }
}
