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
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hyracks.algebricks.common.utils.Pair;

import java.util.Map;

/**
 * The new create feed statement only concerns the feed adaptor configuration.
 * All feeds are considered as primary feeds.
 */
public class CreateFeedStatement implements Statement {

    private final Pair<Identifier, Identifier> qName;
    private final boolean ifNotExists;
    private final String adaptorName;
    private final Map<String, String> adaptorConfiguration;

    public CreateFeedStatement(Pair<Identifier, Identifier> qName, String adaptorName,
            Map<String, String> adaptorConfiguration, boolean ifNotExists) {
        this.qName = qName;
        this.ifNotExists = ifNotExists;
        this.adaptorName = adaptorName;
        this.adaptorConfiguration = adaptorConfiguration;
    }

    public Identifier getDataverseName() {
        return qName.first;
    }

    public Identifier getFeedName() {
        return qName.second;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    public String getAdaptorName() {
        return adaptorName;
    }

    public Map<String, String> getAdaptorConfiguration() {
        return adaptorConfiguration;
    }

    @Override
    public byte getKind() {
        return Kind.CREATE_FEED;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }
}
