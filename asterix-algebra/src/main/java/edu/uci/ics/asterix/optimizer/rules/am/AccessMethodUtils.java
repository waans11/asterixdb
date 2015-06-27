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
import java.util.Collections;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
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
        // One of the args must be a constant, and the other arg must be a variable.
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT
                && arg2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            // The arguments of contains-substring() function or contains text() are asymmetrical.
        	// We can only use index if the variable is on the first argument
            if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS_FUNCTION
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
        if (!index.canProduceFalsePositive() && isIndexOnlyPlanEnabled) {
            dest.add(BuiltinType.AINT32);
        }

    }

    /**
     * Create output variables for the given unnest-map operator that does a secondary index lookup
     */
    public static void appendSecondaryIndexOutputVars(Dataset dataset, ARecordType recordType, Index index,
            boolean primaryKeysOnly, IOptimizationContext context, List<LogicalVariable> dest,
            boolean isIndexOnlyPlanEnabled)
            throws AlgebricksException {
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
        if (!index.getCanProduceFalsePositive() && isIndexOnlyPlanEnabled) {
            numVars += 1;
        }

        for (int i = 0; i < numVars; i++) {
            dest.add(context.newVar());
        }
    }

    /**
     * Get the primary key variables from the unnest-map operator that does a secondary index lookup.
     * The order: SK, PK, [Optional: The result of a TryLock on PK]
     * @throws AlgebricksException
     */
    public static List<LogicalVariable> getKeyVarsFromSecondaryUnnestMap(Dataset dataset, ARecordType recordType,
            ILogicalOperator unnestMapOp, Index index, int keyType) throws AlgebricksException {
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
                                if (function == AlgebricksBuiltinFunctions.LT || function == AlgebricksBuiltinFunctions.GE) {
                                	mathFunction = mathFunctionType.FLOOR; // FLOOR
                                } else if (function == AlgebricksBuiltinFunctions.LE || function == AlgebricksBuiltinFunctions.GT) {
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
                return new Pair<ILogicalExpression, ILogicalExpression>(new ConstantExpression(replacedConstantValue), new ConstantExpression(replacedConstantValue2));
            } else {
                return new Pair<ILogicalExpression, ILogicalExpression>(new ConstantExpression(optFuncExpr.getConstantVal(0)), null);
            }
        } else {
            // We are optimizing a join query. Determine which variable feeds the secondary index.
            if (optFuncExpr.getOperatorSubTree(0) == null || optFuncExpr.getOperatorSubTree(0) == probeSubTree) {
                return new Pair<ILogicalExpression, ILogicalExpression>(new VariableReferenceExpression(optFuncExpr.getLogicalVar(0)), null);
            } else {
                return new Pair<ILogicalExpression, ILogicalExpression>(new VariableReferenceExpression(optFuncExpr.getLogicalVar(1)), null);
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
     * Create an unnest-map operator that does a secondary index lookup
     */
    public static UnnestMapOperator createSecondaryIndexUnnestMap(Dataset dataset, ARecordType recordType, Index index,
            ILogicalOperator inputOp, AccessMethodJobGenParams jobGenParams, IOptimizationContext context,
            boolean outputPrimaryKeysOnly, boolean retainInput, boolean isIndexOnlyPlanEnabled) throws AlgebricksException {
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
        appendSecondaryIndexTypes(dataset, recordType, index, outputPrimaryKeysOnly,
        		secondaryIndexOutputTypes, isIndexOnlyPlanEnabled);
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
    		Mutable<ILogicalOperator> topOpRef, Mutable<ILogicalExpression> conditionRef,
    		Mutable<ILogicalOperator> assignBeforeTopOpRef, AbstractDataSourceOperator dataSourceOp,
            Dataset dataset, ARecordType recordType, ILogicalOperator inputOp, IOptimizationContext context,
            boolean sortPrimaryKeys, boolean retainInput, boolean retainNull, boolean requiresBroadcast,
            Index secondaryIndex, AccessMethodAnalysisContext analysisCtx)
            throws AlgebricksException {

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
        SplitOperator splitOp = null;
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> unionVarMap = null;
        List<LogicalVariable> conditionalSplitVars = null;
        boolean isIndexOnlyPlanEnabled = !secondaryIndex.canProduceFalsePositive() && analysisCtx.isIndexOnlyPlanEnabled();

        // Fetch SK variable from secondary-index search
        List<LogicalVariable> secondaryKeyVars = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset, recordType,
                inputOp, secondaryIndex, 1);

        // Fetch PK variable from secondary-index search
        List<LogicalVariable> primaryKeyVars = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset, recordType,
                inputOp, secondaryIndex, 0);

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

        // If this plan is an index-only plan, add a replicate operator to propagate <SK, PK> pair from
        // the secondary-index search to the two paths
        // TODO: ReplicateOperator needs to be a conditional Replicate Operator
    	if (isIndexOnlyPlanEnabled) {

            // If there is an assign operator before SELECT operator, we need to propagate this variable to UNION too.
            if (assignBeforeTopOpRef != null) {
                assignBeforeTopOp = (AssignOperator) assignBeforeTopOpRef.getValue();
            }

        	unionVarMap = new ArrayList<Triple<LogicalVariable, LogicalVariable, LogicalVariable>>();

            List<LogicalVariable> varsUsedAfterTopOp = new ArrayList<LogicalVariable>();
            List<LogicalVariable> varsUsedInTopOp = new ArrayList<LogicalVariable>();
        	List<LogicalVariable> tmpVars = new ArrayList<LogicalVariable>();

        	// Get used variables from the SELECT operators
        	VariableUtilities.getUsedVariables((ILogicalOperator) topOpRef.getValue(), varsUsedInTopOp);

        	// Generate the list of variables that are used after the SELECE operator
            for (Mutable<ILogicalOperator> afterTopOpRef : afterTopOpRefs) {
            	tmpVars.clear();
                VariableUtilities.getUsedVariables((ILogicalOperator) afterTopOpRef.getValue(), tmpVars);
                for (LogicalVariable tmpVar: tmpVars) {
                	if (!varsUsedAfterTopOp.contains(tmpVar)) {
                		varsUsedAfterTopOp.add(tmpVar);
                	}
                }
            }

            // Is the used variables after SELECT operator from the primary-index?
            int pIndexPKIdx = 0;
            for (Iterator<LogicalVariable> iterator = varsUsedAfterTopOp.iterator(); iterator.hasNext();) {
            	LogicalVariable tVar = iterator.next();
            	if (primaryIndexUnnestVars.contains(tVar)) {
                    unionVarMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(tVar, primaryKeyVars.get(pIndexPKIdx), tVar));
                    pIndexPKIdx++;
                    iterator.remove();
                    varsUsedInTopOp.remove(tVar);
            	}
            }

            // Is the used variables after SELECT operator from the secondary-index?
            int sIndexIdx = 0;
            for (Iterator<LogicalVariable> iterator = varsUsedAfterTopOp.iterator(); iterator.hasNext();) {
            	LogicalVariable tVar = iterator.next();
            	if (varsUsedInTopOp.contains(tVar)) {
                    unionVarMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(tVar, secondaryKeyVars.get(sIndexIdx), tVar));
                    sIndexIdx++;
                    iterator.remove();
                    varsUsedInTopOp.remove(tVar);
            	}
            }



            // Gather information about the variables used in the SELECT operator
            // This information should match the variables used in the ASSIGN operator
//            List<IOptimizableFuncExpr> sidxFuncExprs = analysisCtx.matchedFuncExprs;
//            List<LogicalVariable> varsFromSelectOp = new ArrayList<LogicalVariable>();
//
//            for (int i = 0; i < sidxFuncExprs.size(); i++) {
//            	for (int j = 0; j < sidxFuncExprs.get(i).getNumLogicalVars(); j++) {
//            		varsFromSelectOp.add(sidxFuncExprs.get(i).getSourceVar(j));
//            	}
//            }

            // Is this VAR from the secondary-index field?
//            int sIndexFieldIdx = 0;
//            if (assignBeforeTopOp != null) {
//            	VariableUtilities.getProducedVariables(assignBeforeTopOp, varsProducedBeforeTopOp);
//
//                for (int i = 0; i< varsUsedAfterTopOp.size(); i++) {
//                	// Variables produced before the SELECT operator are used after SELECT operator?
//                	if (varsProducedBeforeTopOp.contains(varsUsedAfterTopOp.get(i))) {
//
//                		// Only variables used in the SELECT operator should be used after the SELECT operator
//                		// since we already removed PK in the previous step.
//                		if (!varsFromSelectOp.contains(varsUsedAfterTopOp.get(i))) {
//                			// Other field variables exist. We can't apply tryLock optimization.
//                			isIndexOnlyPlanEnabled = false;
//                			break;
//                		} else {
//                    		secondaryKeyFieldIsUsedAfterTopOp = true;
//                			isIndexOnlyPlanEnabled = true;
//                    		unionVarMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(varsUsedAfterTopOp.get(i), secondaryKeyVars.get(sIndexFieldIdx), varsUsedAfterTopOp.get(i)));
//                    		sIndexFieldIdx++;
//                		}
//                	}
//                }
//            } else {
//            	isIndexOnlyPlanEnabled = true;
//            	analysisCtx.setIndexOnlyPlanEnabled(isIndexOnlyPlanEnabled);
//            }

            // If this is an index-only plan
//            if (isIndexOnlyPlanEnabled) {

            // Fetch Conditional Split variable from secondary-index search
            conditionalSplitVars = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset, recordType,
                    inputOp, secondaryIndex, 2);

    		splitOp = new SplitOperator(2, conditionalSplitVars.get(0));
            splitOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
            splitOp.setExecutionMode(ExecutionMode.PARTITIONED);
            context.computeAndSetTypeEnvironmentForOperator(splitOp);
//            }

    	}

        // Optionally add a sort on the primary-index keys before searching the primary index.
        OrderOperator order = null;
        if (sortPrimaryKeys) {
            order = new OrderOperator();
            for (LogicalVariable pkVar : primaryKeyVars) {
                Mutable<ILogicalExpression> vRef = new MutableObject<ILogicalExpression>(
                        new VariableReferenceExpression(pkVar));
                order.getOrderExpressions().add(
                        new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, vRef));
            }
            // The secondary-index search feeds into the sort.
//            order.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));

            // If tryLock() optimization is possible, the replicateOp provides PK into this Order Operator
            if (!secondaryIndex.canProduceFalsePositive() && isIndexOnlyPlanEnabled) {
                order.getInputs().add(new MutableObject<ILogicalOperator>(splitOp));
            } else {
                order.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
            }
            order.setExecutionMode(ExecutionMode.LOCAL);
            context.computeAndSetTypeEnvironmentForOperator(order);
        }

        // The job gen parameters are transferred to the actual job gen via the UnnestMapOperator's function arguments.
        List<Mutable<ILogicalExpression>> primaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        BTreeJobGenParams jobGenParams = new BTreeJobGenParams(dataset.getDatasetName(), IndexType.BTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, retainNull, requiresBroadcast);
        // Set low/high inclusive to true for a point lookup.
        jobGenParams.setLowKeyInclusive(true);
        jobGenParams.setHighKeyInclusive(true);
        jobGenParams.setLowKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.setHighKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.setIsEqCondition(true);
        jobGenParams.writeToFuncArgs(primaryIndexFuncArgs);

        // An index search is expressed as an unnest over an index-search function.
        IFunctionInfo primaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        AbstractFunctionCallExpression primaryIndexSearchFunc = new ScalarFunctionCallExpression(primaryIndexSearch,
                primaryIndexFuncArgs);
        // This is the operator that jobgen will be looking for. It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.
        UnnestMapOperator primaryIndexUnnestOp = new UnnestMapOperator(primaryIndexUnnestVars,
                new MutableObject<ILogicalExpression>(primaryIndexSearchFunc), primaryIndexOutputTypes, retainInput);
        // Fed by the order operator or the secondaryIndexUnnestOp.
        if (sortPrimaryKeys) {
            primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(order));
        } else {
//            primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        	if (!secondaryIndex.canProduceFalsePositive() && isIndexOnlyPlanEnabled) {
                // If tryLock() optimization is possible, the replicateOp provides PK into this primary-index search
                primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(splitOp));
        	} else {
                primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        	}
        }
        primaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
        context.computeAndSetTypeEnvironmentForOperator(primaryIndexUnnestOp);


        // Copy original SELECT operator
        topOp = (SelectOperator) topOpRef.getValue();

//        SelectOperator newSelectOp = new SelectOperator(topOp.getCondition(), topOp.getRetainNull(), topOp.getNullPlaceholderVariable());
//        newSelectOp.getInputs().clear();

        // If there is an assign operator before SELECT operator, we need to propagate this variable to UNION too.
        if (assignBeforeTopOp != null) {
//            AssignOperator newAssignOp = new AssignOperator(assignBeforeTopOp.getVariables(), assignBeforeTopOp.getExpressions());
//            newAssignOp.getInputs().clear();
//            newAssignOp.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestOp));
//            context.computeAndSetTypeEnvironmentForOperator(newAssignOp);
            assignBeforeTopOp.getInputs().clear();
            assignBeforeTopOp.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestOp));
//            newSelectOp.getInputs().add(new MutableObject<ILogicalOperator>(newAssignOp));
            context.computeAndSetTypeEnvironmentForOperator(assignBeforeTopOp);
//            newSelectOp.getInputs().add(new MutableObject<ILogicalOperator>(assignBeforeTopOp));
//            newSelectOp.getInputs().add(new MutableObject<ILogicalOperator>(assignBeforeTopOp));
        } else {
//            newSelectOp.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestOp));
            topOp.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestOp));
        }

//        newSelectOp.setExecutionMode(ExecutionMode.PARTITIONED);
        topOp.setExecutionMode(ExecutionMode.PARTITIONED);
//        context.computeAndSetTypeEnvironmentForOperator(newSelectOp);
        context.computeAndSetTypeEnvironmentForOperator(topOp);

        // Generate UnionOperator to merge the left and right paths
        if (!secondaryIndex.canProduceFalsePositive() && isIndexOnlyPlanEnabled) {

//            // If there is an assign operator before SELECT operator, we need to propagate this variable to UNION too.
//            if (assignBeforeTopOp != null) {
//                ILogicalOperator newAssignOp = new AssignOperator(assignBeforeTopOp.getVariables(), assignBeforeTopOp.getExpressions());
//                newAssignOp.getInputs().clear();
//                newAssignOp.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestOp));
//                context.computeAndSetTypeEnvironmentForOperator(newAssignOp);
//                newSelect.getInputs().add(new MutableObject<ILogicalOperator>(newAssignOp));
//            } else {
//                newSelect.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestOp));
//            }
//            context.computeAndSetTypeEnvironmentForOperator(newSelect);

            unionAllOp = new UnionAllOperator(unionVarMap);
//            unionAllOp.getInputs().add(new MutableObject<ILogicalOperator>(newSelectOp));
            unionAllOp.getInputs().add(new MutableObject<ILogicalOperator>(topOp));
            unionAllOp.getInputs().add(new MutableObject<ILogicalOperator>(splitOp));
//            unionAllOp.setExecutionMode(ExecutionMode.PARTITIONED);
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
            Index secondaryIndex, boolean retainInput, boolean retainNull) throws AlgebricksException {
        List<LogicalVariable> primaryKeyVars = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset, recordType,
                inputOp, secondaryIndex, 0);

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
