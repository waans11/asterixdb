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
package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.feeds.BuiltinFeedPolicies;
import org.apache.hyracks.algebricks.common.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class ConnectFeedStatement implements Statement {

    private final Identifier dataverseName;
    private final Identifier datasetName;
    private final String feedName;
    private final String policy;
    private int varCounter;
    private final ArrayList<FunctionSignature> appliedFunctions;

    public ConnectFeedStatement(Pair<Identifier, Identifier> feedNameCmp, Pair<Identifier, Identifier> datasetNameCmp,
            FunctionSignature appliedFunction, String policy, int varCounter) {
        appliedFunctions = new ArrayList<>();
        if (feedNameCmp.first != null && datasetNameCmp.first != null
                && !feedNameCmp.first.getValue().equals(datasetNameCmp.first.getValue())) {
            throw new IllegalArgumentException("Dataverse for source feed and target dataset do not match");
        }
        this.dataverseName = feedNameCmp.first != null ? feedNameCmp.first
                : datasetNameCmp.first != null ? datasetNameCmp.first : null;
        this.datasetName = datasetNameCmp.second;
        this.feedName = feedNameCmp.second.getValue();
        this.policy = policy != null ? policy : BuiltinFeedPolicies.DEFAULT_POLICY.getPolicyName();
        this.varCounter = varCounter;
        if (appliedFunction != null) {
            this.appliedFunctions.add(appliedFunction);
        }
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public byte getKind() {
        return Statement.Kind.CONNECT_FEED;
    }

    public String getPolicy() {
        return policy;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    public String getFeedName() {
        return feedName;
    }

    public List<FunctionSignature> getAppliedFunctions() {
        return appliedFunctions;
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }

}
