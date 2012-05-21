package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.accessors.base.DefaultOpenFieldType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceStaticTypeCastRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        /**
         * pattern match: sink/insert/assign record type is propagated from
         * insert data source to the record-constructor expression
         */
        if (context.checkIfInDontApplySet(this, opRef.getValue()))
            return false;
        context.addToDontApplySet(this, opRef.getValue());
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.SINK)
            return false;
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE)
            return false;
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) op2.getInputs().get(0).getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.ASSIGN)
            return false;

        /**
         * get required record type
         */
        InsertDeleteOperator insertDeleteOperator = (InsertDeleteOperator) op2;
        AssignOperator oldAssignOperator = (AssignOperator) op3;
        AqlDataSource dataSource = (AqlDataSource) insertDeleteOperator.getDataSource();
        IAType[] schemaTypes = (IAType[]) dataSource.getSchemaTypes();
        ARecordType requiredRecordType = (ARecordType) schemaTypes[schemaTypes.length - 1];

        /**
         * get input record type to the insert operator
         */
        List<LogicalVariable> usedVariables = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(oldAssignOperator, usedVariables);
        if (usedVariables.size() == 0)
            return false;
        LogicalVariable oldRecordVariable = usedVariables.get(0);
        LogicalVariable inputRecordVar = usedVariables.get(0);
        IVariableTypeEnvironment env = oldAssignOperator.computeOutputTypeEnvironment(context);
        ARecordType inputRecordType = (ARecordType) env.getVarType(inputRecordVar);

        AbstractLogicalOperator currentOperator = oldAssignOperator;
        List<LogicalVariable> producedVariables = new ArrayList<LogicalVariable>();

        /**
         * find the assign operator for the "input record" to the insert_delete
         * operator
         */
        do {
            context.addToDontApplySet(this, currentOperator);
            if (currentOperator.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                producedVariables.clear();
                VariableUtilities.getProducedVariables(currentOperator, producedVariables);
                int position = producedVariables.indexOf(oldRecordVariable);

                /**
                 * set the top-down propagated type
                 */
                if (position >= 0) {
                    AssignOperator originalAssign = (AssignOperator) currentOperator;
                    List<Mutable<ILogicalExpression>> expressionPointers = originalAssign.getExpressions();
                    ILogicalExpression expr = expressionPointers.get(position).getValue();
                    if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        ScalarFunctionCallExpression funcExpr = (ScalarFunctionCallExpression) expr;
                        if (TypeComputerUtilities.getRequiredType(funcExpr) != null)
                            return false;
                        rewriteRecordFuncExpr(funcExpr, requiredRecordType, inputRecordType);
                    }
                    context.computeAndSetTypeEnvironmentForOperator(originalAssign);
                }
            }
            if (currentOperator.getInputs().size() > 0)
                currentOperator = (AbstractLogicalOperator) currentOperator.getInputs().get(0).getValue();
            else
                break;
        } while (currentOperator != null);
        return true;
    }

    private void rewriteRecordFuncExpr(ScalarFunctionCallExpression funcExpr, ARecordType requiredRecordType,
            ARecordType inputRecordType) {
        if (TypeComputerUtilities.getRequiredType(funcExpr) != null)
            return;
        TypeComputerUtilities.setRequiredAndInputTypes(funcExpr, requiredRecordType, inputRecordType);
        staticRecordTypeCast(funcExpr, requiredRecordType, inputRecordType);
    }

    private void rewriteFuncExpr(ScalarFunctionCallExpression funcExpr, IAType reqType, IAType inputType) {
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR) {
            rewriteListFuncExpr(funcExpr, (AbstractCollectionType) reqType, (AbstractCollectionType) inputType);
        }
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR) {
            rewriteListFuncExpr(funcExpr, (AbstractCollectionType) reqType, (AbstractCollectionType) inputType);
        } else if (reqType.getTypeTag().equals(ATypeTag.RECORD)) {
            rewriteRecordFuncExpr(funcExpr, (ARecordType) reqType, (ARecordType) inputType);
        }
    }

    private void rewriteListFuncExpr(ScalarFunctionCallExpression funcExpr, AbstractCollectionType requiredListType,
            AbstractCollectionType inputListType) {
        if (TypeComputerUtilities.getRequiredType(funcExpr) != null)
            return;

        TypeComputerUtilities.setRequiredAndInputTypes(funcExpr, requiredListType, inputListType);
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();

        IAType itemType = requiredListType.getItemType();
        if (itemType == null || itemType.getTypeTag().equals(ATypeTag.ANY))
            return;
        IAType inputItemType = inputListType.getItemType();
        for (int j = 0; j < args.size(); j++) {
            ILogicalExpression arg = args.get(j).getValue();
            if (arg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                ScalarFunctionCallExpression argFunc = (ScalarFunctionCallExpression) arg;
                rewriteFuncExpr(argFunc, itemType, inputItemType);
            }
        }
    }

    private void staticRecordTypeCast(ScalarFunctionCallExpression func, ARecordType reqType, ARecordType inputType) {
        IAType[] reqFieldTypes = reqType.getFieldTypes();
        String[] reqFieldNames = reqType.getFieldNames();
        IAType[] inputFieldTypes = inputType.getFieldTypes();
        String[] inputFieldNames = inputType.getFieldNames();

        int[] fieldPermutation = new int[reqFieldTypes.length];
        boolean[] nullFields = new boolean[reqFieldTypes.length];
        boolean[] openFields = new boolean[inputFieldTypes.length];

        for (int i = 0; i < nullFields.length; i++)
            nullFields[i] = false;
        for (int i = 0; i < openFields.length; i++)
            openFields[i] = true;
        for (int i = 0; i < fieldPermutation.length; i++)
            fieldPermutation[i] = -1;

        // forward match: match from actual to required
        boolean matched = false;
        for (int i = 0; i < inputFieldNames.length; i++) {
            String fieldName = inputFieldNames[i];
            IAType fieldType = inputFieldTypes[i];

            ILogicalExpression arg = func.getArguments().get(2 * i + 1).getValue();
            matched = false;
            for (int j = 0; j < reqFieldNames.length; j++) {
                String reqFieldName = reqFieldNames[j];
                IAType reqFieldType = reqFieldTypes[j];
                if (fieldName.equals(reqFieldName)) {
                    if (fieldType.equals(reqFieldType)) {
                        fieldPermutation[j] = i;
                        openFields[i] = false;
                        matched = true;

                        if (arg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                            ScalarFunctionCallExpression scalarFunc = (ScalarFunctionCallExpression) arg;
                            rewriteFuncExpr(scalarFunc, reqFieldType, fieldType);
                        }
                        break;
                    }

                    // match the optional field
                    if (reqFieldType.getTypeTag() == ATypeTag.UNION
                            && NonTaggedFormatUtil.isOptionalField((AUnionType) reqFieldType)) {
                        IAType itemType = ((AUnionType) reqFieldType).getUnionList().get(
                                NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
                        reqFieldType = itemType;
                        if (fieldType.equals(BuiltinType.ANULL) || fieldType.equals(itemType)) {
                            fieldPermutation[j] = i;
                            openFields[i] = false;
                            matched = true;

                            // rewrite record expr
                            if (arg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                                ScalarFunctionCallExpression scalarFunc = (ScalarFunctionCallExpression) arg;
                                rewriteFuncExpr(scalarFunc, reqFieldType, fieldType);
                            }
                            break;
                        }
                    }

                    // match the record field: need cast
                    if (arg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        ScalarFunctionCallExpression scalarFunc = (ScalarFunctionCallExpression) arg;
                        rewriteFuncExpr(scalarFunc, reqFieldType, fieldType);
                        fieldPermutation[j] = i;
                        openFields[i] = false;
                        matched = true;
                        break;
                    }
                }
            }
            if (matched)
                continue;
            // the input has extra fields
            if (!reqType.isOpen())
                throw new IllegalStateException("static type mismatch: including extra closed fields");
        }

        // backward match: match from required to actual
        for (int i = 0; i < reqFieldNames.length; i++) {
            String reqFieldName = reqFieldNames[i];
            IAType reqFieldType = reqFieldTypes[i];
            matched = false;
            for (int j = 0; j < inputFieldNames.length; j++) {
                String fieldName = inputFieldNames[j];
                IAType fieldType = inputFieldTypes[j];
                if (fieldName.equals(reqFieldName)) {
                    if (!openFields[j]) {
                        matched = true;
                        break;
                    }

                    // match the optional field
                    if (reqFieldType.getTypeTag() == ATypeTag.UNION
                            && NonTaggedFormatUtil.isOptionalField((AUnionType) reqFieldType)) {
                        IAType itemType = ((AUnionType) reqFieldType).getUnionList().get(
                                NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
                        if (fieldType.equals(BuiltinType.ANULL) || fieldType.equals(itemType)) {
                            matched = true;
                            break;
                        }
                    }
                }
            }
            if (matched)
                continue;

            IAType t = reqFieldTypes[i];
            if (t.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t)) {
                // add a null field
                nullFields[i] = true;
            } else {
                // no matched field in the input for a required closed field
                throw new IllegalStateException("static type mismatch: miss a required closed field");
            }
        }

        List<Mutable<ILogicalExpression>> arguments = func.getArguments();
        List<Mutable<ILogicalExpression>> argumentsClone = new ArrayList<Mutable<ILogicalExpression>>();
        argumentsClone.addAll(arguments);
        arguments.clear();
        // re-order the closed part and fill in null fields
        for (int i = 0; i < fieldPermutation.length; i++) {
            int pos = fieldPermutation[i];
            if (pos >= 0) {
                arguments.add(argumentsClone.get(2 * pos));
                arguments.add(argumentsClone.get(2 * pos + 1));
            }
            if (nullFields[i]) {
                // add a null field
                arguments.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                        new AString(reqFieldNames[i])))));
                arguments.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                        ANull.NULL))));
            }
        }

        // add the open part
        for (int i = 0; i < openFields.length; i++) {
            if (openFields[i]) {
                arguments.add(argumentsClone.get(2 * i));
                Mutable<ILogicalExpression> fExprRef = argumentsClone.get(2 * i + 1);
                ILogicalExpression argExpr = fExprRef.getValue();

                if (argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    IAType reqFieldType = inputFieldTypes[i];
                    if (inputFieldTypes[i].getTypeTag() == ATypeTag.RECORD) {
                        reqFieldType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                    }
                    if (inputFieldTypes[i].getTypeTag() == ATypeTag.ORDEREDLIST) {
                        reqFieldType = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
                    }
                    if (inputFieldTypes[i].getTypeTag() == ATypeTag.UNORDEREDLIST) {
                        reqFieldType = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
                    }
                    if (TypeComputerUtilities.getRequiredType((AbstractFunctionCallExpression)argExpr) == null) {
                        ScalarFunctionCallExpression argFunc = (ScalarFunctionCallExpression) argExpr;
                        rewriteFuncExpr(argFunc, reqFieldType, inputFieldTypes[i]);
                    }
                }
                arguments.add(fExprRef);
            }
        }
    }
}
