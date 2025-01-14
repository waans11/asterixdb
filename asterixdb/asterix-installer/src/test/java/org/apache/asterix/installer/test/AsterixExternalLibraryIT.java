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
package org.apache.asterix.installer.test;

import java.io.File;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.test.base.RetainLogsRule;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class AsterixExternalLibraryIT {

    private static final String LIBRARY_NAME = "testlib";
    private static final String LIBRARY_DATAVERSE = "externallibtest";
    private static final String PATH_BASE = "src/test/resources/integrationts/library";
    private static final String PATH_ACTUAL = "target" + File.separator + "ittest" + File.separator;
    private static final String LIBRARY_PATH = "target" + File.separator + "resources" + File.separator + "externallib"
            + File.separator + "testlib-zip-binary-assembly.zip";
    private static final Logger LOGGER = Logger.getLogger(AsterixExternalLibraryIT.class.getName());
    private static List<TestCaseContext> testCaseCollection;
    private static String reportPath =
            new File(StringUtils.join(new String[] { "target", "failsafe-reports" }, File.separator)).getAbsolutePath();

    private final TestExecutor testExecutor = new TestExecutor();

    @Rule
    public TestRule retainLogs = new RetainLogsRule(
            AsterixInstallerIntegrationUtil.getManagixHome(), reportPath);

    @BeforeClass
    public static void setUp() throws Exception {
        try {
            AsterixInstallerIntegrationUtil.init(AsterixInstallerIntegrationUtil.LOCAL_CLUSTER_PATH);
            File asterixInstallerProjectDir = new File(System.getProperty("user.dir"));
            String asterixExternalLibraryPath = asterixInstallerProjectDir.getAbsolutePath() + File.separator
                    + LIBRARY_PATH;
            LOGGER.info("Installing library :" + LIBRARY_NAME + " located at " + asterixExternalLibraryPath
                    + " in dataverse " + LIBRARY_DATAVERSE);
            AsterixInstallerIntegrationUtil.installLibrary(LIBRARY_NAME, LIBRARY_DATAVERSE, asterixExternalLibraryPath);
            AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.ACTIVE);
            TestCaseContext.Builder b = new TestCaseContext.Builder();
            testCaseCollection = b.build(new File(PATH_BASE));
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        AsterixInstallerIntegrationUtil.deinit();
    }

    @Test
    public void test() throws Exception {
        for (TestCaseContext testCaseCtx : testCaseCollection) {
            testExecutor.executeTest(PATH_ACTUAL, testCaseCtx, null, false);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            setUp();
            new AsterixExternalLibraryIT().test();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.info("TEST CASES FAILED");
        } finally {
            tearDown();
        }
    }

}
