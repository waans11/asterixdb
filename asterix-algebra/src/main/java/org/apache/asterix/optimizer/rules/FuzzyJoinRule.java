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
package org.apache.asterix.optimizer.rules;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.algebra.base.LogicalOperatorDeepCopyVisitor;
import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.aqlplus.parser.AQLPlusParser;
import edu.uci.ics.asterix.aqlplus.parser.ParseException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.asterix.optimizer.base.FuzzyUtils;
import edu.uci.ics.asterix.translator.AqlPlusExpressionToPlanTranslator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.Counter;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IndexedNLJoinExpressionAnnotation;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class FuzzyJoinRule implements IAlgebraicRewriteRule {

    private static HashSet<FunctionIdentifier> simFuncs = new HashSet<FunctionIdentifier>();
    static {
        simFuncs.add(AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK);
    }

    private static final String AQLPLUS = ""
            //
            // -- - Stage 3 - --
            //
            + "((#LEFT_0), "
            + "  (join((#RIGHT_0), "
            //
            // -- -- - Stage 2 - --
            //
            + "    ("
            + "    join( "
            + "      ( "
            + "      #LEFT_1 "
            + "      let $tokensUnrankedLeft := %s($$LEFT_1) "
            + "      let $lenLeft := len($tokensUnrankedLeft) "
            + "      let $tokensLeft := "
            + "        for $token in $tokensUnrankedLeft "
            + "        for $tokenRanked at $i in "
            //
            // -- -- -- - Stage 1 - --
            //
            // + "          #LEFT_2 "
            // + "          let $id := $$LEFTPK_2 "
            // + "          for $token in %s($$LEFT_2) "
            + "          #RIGHT_2 "
            + "          let $id := $$RIGHTPK_2_0 "
            + "          for $token in %s($$RIGHT_2) "
            + "          /*+ hash */ "
            + "          group by $tokenGroupped := $token with $id "
            + "          /*+ inmem 34 198608 */ "
            + "          order by count($id), $tokenGroupped "
            + "          return $tokenGroupped "
            //
            // -- -- -- -
            //
            + "        where $token = /*+ bcast */ $tokenRanked "
            + "        order by $i "
            + "        return $i "
            + "      for $prefixTokenLeft in subset-collection($tokensLeft, 0, prefix-len-%s(len($tokensLeft), %ff)) "
            + "      ),( "
            + "      #RIGHT_1 "
            + "      let $tokensUnrankedRight := %s($$RIGHT_1) "
            + "      let $lenRight := len($tokensUnrankedRight) "
            + "      let $tokensRight := "
            + "        for $token in $tokensUnrankedRight "
            + "        for $tokenRanked at $i in "
            //
            // -- -- -- - Stage 1 - --
            //
            // + "          #LEFT_3 "
            // + "          let $id := $$LEFTPK_3 "
            // + "          for $token in %s($$LEFT_3) "
            + "          #RIGHT_3 "
            + "          let $id := $$RIGHTPK_3_0 "
            + "          for $token in %s($$RIGHT_3) "
            + "          /*+ hash */ "
            + "          group by $tokenGroupped := $token with $id "
            + "          /*+ inmem 34 198608 */ "
            + "          order by count($id), $tokenGroupped "
            + "          return $tokenGroupped "
            //
            // -- -- -- -
            //
            + "        where $token = /*+ bcast */ $tokenRanked "
            + "        order by $i "
            + "        return $i "
            + "      for $prefixTokenRight in subset-collection($tokensRight, 0, prefix-len-%s(len($tokensRight), %ff)) "
            + "      ), $prefixTokenLeft = $prefixTokenRight) "
            + "    let $sim := similarity-%s-prefix($lenLeft, $tokensLeft, $lenRight, $tokensRight, $prefixTokenLeft, %ff) "
            + "    where $sim >= %ff " + "    /*+ hash*/ "
            + "    group by %s, %s with $sim "
            //          GROUPBY_LEFT, GROUPBY_RIGHT
            //
            // -- -- -
            //
            + "    ), %s)),  %s)";
            // JOIN_COND_RIGHT  JOIN_COND_LEFT

    private static final String GROUPBY_LEFT = "$idLeft_%d := $$LEFTPK_1_%d";
    private static final String GROUPBY_RIGHT = "$idRight_%d := $$RIGHTPK_1_%d";
    private static final String JOIN_COND_LEFT = "$$LEFTPK_0_%d = $idLeft_%d";
    private static final String JOIN_COND_RIGHT = "$$RIGHTPK_0_%d = $idRight_%d";

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        // current opperator is join
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }

        // Find GET_ITEM function.
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        Mutable<ILogicalExpression> expRef = joinOp.getCondition();
        Mutable<ILogicalExpression> getItemExprRef = getSimilarityExpression(expRef);
        if (getItemExprRef == null) {
            return false;
        }
        // Check if the GET_ITEM function is on one of the supported similarity-check functions.
        AbstractFunctionCallExpression getItemFuncExpr = (AbstractFunctionCallExpression) getItemExprRef.getValue();
        Mutable<ILogicalExpression> argRef = getItemFuncExpr.getArguments().get(0);
        AbstractFunctionCallExpression simFuncExpr = (AbstractFunctionCallExpression) argRef.getValue();
        if (!simFuncs.contains(simFuncExpr.getFunctionIdentifier())) {
            return false;
        }
        // Skip this rule based on annotations.
        if (simFuncExpr.getAnnotations().containsKey(IndexedNLJoinExpressionAnnotation.INSTANCE)) {
            return false;
        }

        List<Mutable<ILogicalOperator>> inputOps = joinOp.getInputs();
        ILogicalOperator leftInputOp = inputOps.get(0).getValue();
        ILogicalOperator rightInputOp = inputOps.get(1).getValue();

        List<Mutable<ILogicalExpression>> inputExps = simFuncExpr.getArguments();
        if (inputExps.size() != 3) {
            return false;
        }

        ILogicalExpression inputExp0 = inputExps.get(0).getValue();
        ILogicalExpression inputExp1 = inputExps.get(1).getValue();
        ILogicalExpression inputExp2 = inputExps.get(2).getValue();

        // left and right expressions are variables
        if (inputExp0.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || inputExp1.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || inputExp2.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }

        LogicalVariable inputVar0 = ((VariableReferenceExpression) inputExp0).getVariableReference();
        LogicalVariable inputVar1 = ((VariableReferenceExpression) inputExp1).getVariableReference();

        LogicalVariable leftInputVar;
        LogicalVariable rightInputVar;
        Collection<LogicalVariable> liveVars = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(leftInputOp, liveVars);
        if (liveVars.contains(inputVar0)) {
            leftInputVar = inputVar0;
            rightInputVar = inputVar1;
        } else {
            leftInputVar = inputVar1;
            rightInputVar = inputVar0;
        }       
        List<LogicalVariable> leftInputPKs = findPrimaryKeysInSubplan(liveVars, context);
        liveVars.clear();
        VariableUtilities.getLiveVariables(rightInputOp, liveVars);   
        List<LogicalVariable> rightInputPKs = findPrimaryKeysInSubplan(liveVars, context);
        
        // Bail if primary keys could not be inferred.
        if (leftInputPKs == null || rightInputPKs == null) {
            return false;
        }

        IAType leftType = (IAType) context.getOutputTypeEnvironment(leftInputOp).getVarType(leftInputVar);
        IAType rightType = (IAType) context.getOutputTypeEnvironment(rightInputOp).getVarType(rightInputVar);
        // left-hand side and right-hand side of "~=" has the same type
        IAType left2 = TypeHelper.getNonOptionalType(leftType);
        IAType right2 = TypeHelper.getNonOptionalType(rightType);
        if (!left2.deepEqual(right2)) {
            return false;
        }
        //
        // -- - FIRE - --
        //
        AqlMetadataProvider metadataProvider = ((AqlMetadataProvider) context.getMetadataProvider());
        FunctionIdentifier funcId = FuzzyUtils.getTokenizer(leftType.getTypeTag());
        String tokenizer;
        if (funcId == null) {
            tokenizer = "";
        } else {
            tokenizer = funcId.getName();
        }

        String simFunction = FuzzyUtils.getSimFunction(simFuncExpr.getFunctionIdentifier());
        float simThreshold;
        ConstantExpression constExpr = (ConstantExpression) inputExp2;
        AsterixConstantValue constVal = (AsterixConstantValue) constExpr.getValue();
        if (constVal.getObject() instanceof AFloat) {
            simThreshold = ((AFloat) constVal.getObject()).getFloatValue();
        } else {
            simThreshold = FuzzyUtils.getSimThreshold(metadataProvider);
        }

        // finalize AQL+ query
        String prepareJoin;
        switch (joinOp.getJoinKind()) {
            case INNER: {
                prepareJoin = "join" + AQLPLUS;
                break;
            }
            case LEFT_OUTER: {
                prepareJoin = "loj" + AQLPLUS;
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
        String groupByLeft = "";
        String joinCondLeft = "";
        for (int i = 0; i < leftInputPKs.size(); i++) {
            if (i > 0) {
                groupByLeft += ", ";
                joinCondLeft += " and ";
            }
            groupByLeft += String.format(Locale.US, GROUPBY_LEFT, i, i);
            joinCondLeft += String.format(Locale.US, JOIN_COND_LEFT, i, i);
        }
        
        String groupByRight = "";
        String joinCondRight = "";
        for (int i = 0; i < rightInputPKs.size(); i++) {
            if (i > 0) {
                groupByRight += ", ";
                joinCondRight += " and ";
            }
            groupByRight += String.format(Locale.US, GROUPBY_RIGHT, i, i);
            joinCondRight += String.format(Locale.US, JOIN_COND_RIGHT, i, i);
        }
        String aqlPlus = String.format(Locale.US, prepareJoin, tokenizer, tokenizer, simFunction, simThreshold,
                tokenizer, tokenizer, simFunction, simThreshold, simFunction, simThreshold, simThreshold, groupByLeft,
                groupByRight, joinCondRight, joinCondLeft);

        Counter counter = new Counter(context.getVarCounter());

        AQLPlusParser parser = new AQLPlusParser(new StringReader(aqlPlus));
        parser.initScope();
        parser.setVarCounter(counter);
        List<Clause> clauses;
        try {
            clauses = parser.Clauses();
        } catch (ParseException e) {
            throw new AlgebricksException(e);
        }
        // The translator will compile metadata internally. Run this compilation
        // under the same transaction id as the "outer" compilation.
        AqlPlusExpressionToPlanTranslator translator = new AqlPlusExpressionToPlanTranslator(
                metadataProvider.getJobId(), metadataProvider, counter, null, null);

        LogicalOperatorDeepCopyVisitor deepCopyVisitor = new LogicalOperatorDeepCopyVisitor(counter);

        translator.addOperatorToMetaScope(new Identifier("#LEFT_0"), leftInputOp);
        translator.addVariableToMetaScope(new Identifier("$$LEFT_0"), leftInputVar);
        for (int i = 0; i < leftInputPKs.size(); i++) {
            translator.addVariableToMetaScope(new Identifier("$$LEFTPK_0_" + i), leftInputPKs.get(i));
        }

        translator.addOperatorToMetaScope(new Identifier("#RIGHT_0"), rightInputOp);
        translator.addVariableToMetaScope(new Identifier("$$RIGHT_0"), rightInputVar);
        for (int i = 0; i < rightInputPKs.size(); i++) {
            translator.addVariableToMetaScope(new Identifier("$$RIGHTPK_0_" + i), rightInputPKs.get(i));
        }

        translator.addOperatorToMetaScope(new Identifier("#LEFT_1"), deepCopyVisitor.deepCopy(leftInputOp, null));
        translator.addVariableToMetaScope(new Identifier("$$LEFT_1"), deepCopyVisitor.varCopy(leftInputVar));
        for (int i = 0; i < leftInputPKs.size(); i++) {
            translator.addVariableToMetaScope(new Identifier("$$LEFTPK_1_" + i), deepCopyVisitor.varCopy(leftInputPKs.get(i)));
        }
        deepCopyVisitor.updatePrimaryKeys(context);
        deepCopyVisitor.reset();

        // TODO pick side to run Stage 1, currently always picks RIGHT side
        for (int i = 1; i < 4; i++) {
            translator.addOperatorToMetaScope(new Identifier("#RIGHT_" + i), deepCopyVisitor.deepCopy(rightInputOp, null));
            translator.addVariableToMetaScope(new Identifier("$$RIGHT_" + i), deepCopyVisitor.varCopy(rightInputVar));
            for (int j = 0; j < rightInputPKs.size(); j++) {
                translator.addVariableToMetaScope(new Identifier("$$RIGHTPK_" + i + "_" + j), deepCopyVisitor.varCopy(rightInputPKs.get(j)));
            }
            deepCopyVisitor.updatePrimaryKeys(context);
            deepCopyVisitor.reset();
        }

        ILogicalPlan plan;
        try {
            plan = translator.translate(clauses);
        } catch (AsterixException e) {
            throw new AlgebricksException(e);
        }
        context.setVarCounter(counter.get());

        ILogicalOperator outputOp = plan.getRoots().get(0).getValue();

        SelectOperator extraSelect = null;
        if (getItemExprRef != expRef) {
            // more than one join condition
            getItemExprRef.setValue(ConstantExpression.TRUE);
            switch (joinOp.getJoinKind()) {
                case INNER: {
                    extraSelect = new SelectOperator(expRef, false, null);
                    extraSelect.getInputs().add(new MutableObject<ILogicalOperator>(outputOp));
                    outputOp = extraSelect;
                    break;
                }
                case LEFT_OUTER: {
                    if (((AbstractLogicalOperator) outputOp).getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
                        throw new IllegalStateException();
                    }
                    LeftOuterJoinOperator topJoin = (LeftOuterJoinOperator) outputOp;

                    // Combine the conditions of top join of aqlplus plan and the original join
                    AbstractFunctionCallExpression andFunc = new ScalarFunctionCallExpression(
                            FunctionUtils.getFunctionInfo(AlgebricksBuiltinFunctions.AND));

                    List<Mutable<ILogicalExpression>> conjs = new ArrayList<Mutable<ILogicalExpression>>();
                    if (topJoin.getCondition().getValue().splitIntoConjuncts(conjs)) {
                        andFunc.getArguments().addAll(conjs);     
                    } else {
                        andFunc.getArguments().add(new MutableObject<ILogicalExpression>(topJoin.getCondition().getValue()));
                    }
                    
                    List<Mutable<ILogicalExpression>> conjs2 = new ArrayList<Mutable<ILogicalExpression>>();
                    if (expRef.getValue().splitIntoConjuncts(conjs2)) {
                        andFunc.getArguments().addAll(conjs2);     
                    } else {
                        andFunc.getArguments().add(expRef);
                    }
                    topJoin.getCondition().setValue(andFunc);
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
        }      
        opRef.setValue(outputOp);
        OperatorPropertiesUtil.typeOpRec(opRef, context);
        return true;
    }

    /**
     * Look for GET_ITEM function call.
     */
    private Mutable<ILogicalExpression> getSimilarityExpression(Mutable<ILogicalExpression> expRef) {
        ILogicalExpression exp = expRef.getValue();
        if (exp.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) exp;
            if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.GET_ITEM)) {
                return expRef;
            }
            if (funcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.AND)) {
                for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                    Mutable<ILogicalExpression> expRefRet = getSimilarityExpression(arg);
                    if (expRefRet != null) {
                        return expRefRet;
                    }
                }
            }
        }
        return null;
    }
    
    private List<LogicalVariable> findPrimaryKeysInSubplan(Collection<LogicalVariable> liveVars, IOptimizationContext context) {
        
        Collection<LogicalVariable> primaryKeys = new HashSet<LogicalVariable>();
        
        for (LogicalVariable var : liveVars) {
            List<LogicalVariable> pks = context.findPrimaryKey(var);
            if (pks != null) {
                primaryKeys.addAll(pks);
            }
        }
        if (primaryKeys.isEmpty()) {
            return null;
        }
        return new ArrayList<LogicalVariable>(primaryKeys);
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }
}
