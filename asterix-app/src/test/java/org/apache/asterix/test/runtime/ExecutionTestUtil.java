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
package org.apache.asterix.test.runtime;

import java.io.File;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.config.AsterixPropertiesAccessor;
import org.apache.asterix.common.config.AsterixTransactionProperties;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import org.apache.asterix.external.util.IdentitiyResolverFactory;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.asterix.testframework.xml.TestSuite;
import org.apache.commons.io.FileUtils;

public class ExecutionTestUtil {

    protected static final Logger LOGGER = Logger.getLogger(ExecutionTest.class.getName());

    protected static final String PATH_ACTUAL = "rttest" + File.separator;

    protected static final String TEST_CONFIG_FILE_NAME = "asterix-build-configuration.xml";

    protected static AsterixTransactionProperties txnProperties;

    protected static TestGroup FailedGroup;

    public static void setUp() throws Exception {
        System.out.println("Starting setup");
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting setup");
        }
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);

        AsterixPropertiesAccessor apa = new AsterixPropertiesAccessor();
        txnProperties = new AsterixTransactionProperties(apa);

        deleteTransactionLogs();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("initializing pseudo cluster");
        }
        AsterixHyracksIntegrationUtil.init();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("initializing HDFS");
        }

        HDFSCluster.getInstance().setup();

        // Set the node resolver to be the identity resolver that expects node
        // names
        // to be node controller ids; a valid assumption in test environment.
        System.setProperty(FileSystemBasedAdapter.NODE_RESOLVER_FACTORY_PROPERTY,
                IdentitiyResolverFactory.class.getName());

        FailedGroup = new TestGroup();
        FailedGroup.setName("failed");
    }

    private static void deleteTransactionLogs() throws Exception {
        for (String ncId : AsterixHyracksIntegrationUtil.getNcNames()) {
            File log = new File(txnProperties.getLogDirectory(ncId));
            if (log.exists()) {
                FileUtils.deleteDirectory(log);
            }
        }
    }

    public static void tearDown() throws Exception {
        AsterixHyracksIntegrationUtil.deinit();
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
        HDFSCluster.getInstance().cleanup();

        if (FailedGroup != null && FailedGroup.getTestCase().size() > 0) {
            File temp = File.createTempFile("failed", ".xml");
            javax.xml.bind.JAXBContext jaxbCtx = null;
            jaxbCtx = javax.xml.bind.JAXBContext.newInstance(TestSuite.class.getPackage().getName());
            javax.xml.bind.Marshaller marshaller = null;
            marshaller = jaxbCtx.createMarshaller();
            marshaller.setProperty(javax.xml.bind.Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            TestSuite failedSuite = new TestSuite();
            failedSuite.setResultOffsetPath("results");
            failedSuite.setQueryOffsetPath("queries");
            failedSuite.getTestGroup().add(FailedGroup);
            marshaller.marshal(failedSuite, temp);
            System.err.println("The failed.xml is written to :" + temp.getAbsolutePath()
                    + ". You can copy it to only.xml by the following cmd:" + "\rcp " + temp.getAbsolutePath() + " "
                    + Paths.get("./src/test/resources/runtimets/only.xml").toAbsolutePath());
        }
    }

}
