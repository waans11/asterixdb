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

package org.apache.asterix.aql.translator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.MasterNode;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.RunStatement;
import org.apache.asterix.runtime.utils.AppContextInfo;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionConfig;
import org.junit.Assert;
import org.junit.Test;

import junit.extensions.PA;

@SuppressWarnings({ "unchecked" })
public class QueryTranslatorTest {

    @Test
    public void test() throws Exception {
        List<Statement> statements = new ArrayList<>();
        SessionConfig mockSessionConfig = mock(SessionConfig.class);
        RunStatement mockRunStatement = mock(RunStatement.class);

        // Mocks AppContextInfo.
        AppContextInfo mockAsterixAppContextInfo = mock(AppContextInfo.class);
        setFinalStaticField(AppContextInfo.class.getDeclaredField("INSTANCE"), mockAsterixAppContextInfo);
        ExternalProperties mockAsterixExternalProperties = mock(ExternalProperties.class);
        when(mockAsterixAppContextInfo.getExternalProperties()).thenReturn(mockAsterixExternalProperties);
        when(mockAsterixExternalProperties.getAPIServerPort()).thenReturn(19002);

        // Mocks AsterixClusterProperties.
        Cluster mockCluster = mock(Cluster.class);
        MasterNode mockMasterNode = mock(MasterNode.class);
        ClusterProperties mockClusterProperties = mock(ClusterProperties.class);
        setFinalStaticField(ClusterProperties.class.getDeclaredField("INSTANCE"), mockClusterProperties);
        when(mockClusterProperties.getCluster()).thenReturn(mockCluster);
        when(mockCluster.getMasterNode()).thenReturn(mockMasterNode);
        when(mockMasterNode.getClientIp()).thenReturn("127.0.0.1");

        IStatementExecutor aqlTranslator = new DefaultStatementExecutorFactory().create(statements, mockSessionConfig,
                new AqlCompilationProvider(), new StorageComponentProvider());
        List<String> parameters = new ArrayList<>();
        parameters.add("examples/pregelix-example-jar-with-dependencies.jar");
        parameters.add("org.apache.pregelix.example.PageRankVertex");
        parameters.add("-ip 10.0.2.15 -port 3199");
        when(mockRunStatement.getParameters()).thenReturn(parameters);
        // Test a customer command without "-cust-prop".
        List<String> cmds = (List<String>) PA.invokeMethod(aqlTranslator,
                "constructPregelixCommand(org.apache.asterix.lang.common.statement.RunStatement,"
                        + "String,String,String,String)",
                mockRunStatement, "fromDataverse", "fromDataset", "toDataverse", "toDataset");
        List<String> expectedCmds = Arrays.asList(new String[] { "bin/pregelix",
                "examples/pregelix-example-jar-with-dependencies.jar", "org.apache.pregelix.example.PageRankVertex",
                "-ip", "10.0.2.15", "-port", "3199", "-cust-prop",
                "pregelix.asterixdb.url=http://127.0.0.1:19002,pregelix.asterixdb.source=true,pregelix.asterixdb.sink=true,pregelix.asterixdb.input.dataverse=fromDataverse,pregelix.asterixdb.input.dataset=fromDataset,pregelix.asterixdb.output.dataverse=toDataverse,pregelix.asterixdb.output.dataset=toDataset,pregelix.asterixdb.output.cleanup=false,pregelix.asterixdb.input.converterclass=org.apache.pregelix.example.converter.VLongIdInputVertexConverter,pregelix.asterixdb.output.converterclass=org.apache.pregelix.example.converter.VLongIdOutputVertexConverter" });
        Assert.assertEquals(cmds, expectedCmds);

        parameters.remove(parameters.size() - 1);
        parameters.add("-ip 10.0.2.15 -port 3199 -cust-prop "
                + "pregelix.asterixdb.input.converterclass=org.apache.pregelix.example.converter.TestInputVertexConverter,"
                + "pregelix.asterixdb.output.converterclass=org.apache.pregelix.example.converter.TestOutputVertexConverter");
        // Test a customer command with "-cust-prop".
        cmds = (List<String>) PA.invokeMethod(aqlTranslator,
                "constructPregelixCommand(org.apache.asterix.lang.common.statement.RunStatement,"
                        + "String,String,String,String)",
                mockRunStatement, "fromDataverse", "fromDataset", "toDataverse", "toDataset");
        expectedCmds = Arrays.asList(new String[] { "bin/pregelix",
                "examples/pregelix-example-jar-with-dependencies.jar", "org.apache.pregelix.example.PageRankVertex",
                "-ip", "10.0.2.15", "-port", "3199", "-cust-prop",
                "pregelix.asterixdb.url=http://127.0.0.1:19002,pregelix.asterixdb.source=true,pregelix.asterixdb.sink=true,pregelix.asterixdb.input.dataverse=fromDataverse,pregelix.asterixdb.input.dataset=fromDataset,pregelix.asterixdb.output.dataverse=toDataverse,pregelix.asterixdb.output.dataset=toDataset,pregelix.asterixdb.output.cleanup=false,pregelix.asterixdb.input.converterclass=org.apache.pregelix.example.converter.TestInputVertexConverter,pregelix.asterixdb.output.converterclass=org.apache.pregelix.example.converter.TestOutputVertexConverter" });
        Assert.assertEquals(cmds, expectedCmds);
    }

    private void setFinalStaticField(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        // remove final modifier from field
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, newValue);
    }
}
