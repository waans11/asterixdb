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

package org.apache.asterix.lang.sqlpp.parser;

import java.io.StringReader;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.entities.Function;

public class FunctionParser {

    private final IParserFactory parserFactory;

    public FunctionParser(IParserFactory parserFactory) {
        this.parserFactory = parserFactory;
    }

    public FunctionDecl getFunctionDecl(Function function) throws CompilationException {
        String functionBody = function.getFunctionBody();
        List<String> params = function.getParams();

        StringBuilder builder = new StringBuilder();
        builder.append(" use " + function.getDataverseName() + ";");
        builder.append(" declare function " + function.getName().split("@")[0]);
        builder.append("(");
        for (String param : params) {
            VarIdentifier varId = SqlppVariableUtil.toUserDefinedVariableName(param);
            builder.append(varId);
            builder.append(",");
        }
        if (params.size() > 0) {
            builder.delete(builder.length() - 1, builder.length());
        }
        builder.append(")");
        builder.append("{");
        builder.append("\n");
        builder.append(functionBody);
        builder.append("\n");
        builder.append("}");

        IParser parser = parserFactory.createParser(new StringReader(new String(builder)));
        List<Statement> statements = parser.parse();
        FunctionDecl decl = (FunctionDecl) statements.get(1);
        return decl;
    }

}
