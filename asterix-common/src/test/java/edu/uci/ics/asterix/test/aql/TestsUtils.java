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
package edu.uci.ics.asterix.test.aql;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;
import edu.uci.ics.asterix.testframework.context.TestCaseContext.OutputFormat;
import edu.uci.ics.asterix.testframework.context.TestFileContext;
import edu.uci.ics.asterix.testframework.xml.TestCase.CompilationUnit;

public class TestsUtils {

    private static final Logger LOGGER = Logger.getLogger(TestsUtils.class.getName());
    private static Method managixExecuteMethod = null;

    /**
     * Probably does not work well with symlinks.
     */
    public static boolean deleteRec(File path) {
        if (path.isDirectory()) {
            for (File f : path.listFiles()) {
                if (!deleteRec(f)) {
                    return false;
                }
            }
        }
        return path.delete();
    }

    private static void runScriptAndCompareWithResult(File scriptFile, PrintWriter print, File expectedFile,
            File actualFile) throws Exception {
        System.err.println("Expected results file: " + expectedFile.toString());
        BufferedReader readerExpected = new BufferedReader(new InputStreamReader(new FileInputStream(expectedFile),
                "UTF-8"));
        BufferedReader readerActual = new BufferedReader(
                new InputStreamReader(new FileInputStream(actualFile), "UTF-8"));
        String lineExpected, lineActual;
        int num = 1;
        try {
            while ((lineExpected = readerExpected.readLine()) != null) {
                lineActual = readerActual.readLine();
                // Assert.assertEquals(lineExpected, lineActual);
                if (lineActual == null) {
                    if (lineExpected.isEmpty()) {
                        continue;
                    }
                    throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< " + lineExpected
                            + "\n> ");
                }

                if (!equalStrings(lineExpected.split("Time")[0], lineActual.split("Time")[0])) {
                    throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< " + lineExpected
                            + "\n> " + lineActual);
                }

                ++num;
            }
            lineActual = readerActual.readLine();
            // Assert.assertEquals(null, lineActual);
            if (lineActual != null) {
                throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< \n> " + lineActual);
            }
            // actualFile.delete();
        } finally {
            readerExpected.close();
            readerActual.close();
        }

    }

    private static boolean equalStrings(String s1, String s2) {
        String[] rowsOne = s1.split("\n");
        String[] rowsTwo = s2.split("\n");

        for (int i = 0; i < rowsOne.length; i++) {
            String row1 = rowsOne[i];
            String row2 = rowsTwo[i];

            if (row1.equals(row2))
                continue;

            String[] fields1 = row1.split(" ");
            String[] fields2 = row2.split(" ");

            boolean bagEncountered = false;
            Set<String> bagElements1 = new HashSet<String>();
            Set<String> bagElements2 = new HashSet<String>();

            for (int j = 0; j < fields1.length; j++) {
                if (j >= fields2.length) {
                    return false;
                } else if (fields1[j].equals(fields2[j])) {
                    if (fields1[j].equals("{{"))
                        bagEncountered = true;
                    if (fields1[j].startsWith("}}")) {
                        if (!bagElements1.equals(bagElements2))
                            return false;
                        bagEncountered = false;
                        bagElements1.clear();
                        bagElements2.clear();
                    }
                    continue;
                } else if (fields1[j].indexOf('.') < 0) {
                    if (bagEncountered) {
                        bagElements1.add(fields1[j].replaceAll(",$", ""));
                        bagElements2.add(fields2[j].replaceAll(",$", ""));
                        continue;
                    }
                    return false;
                } else {
                    // If the fields are floating-point numbers, test them
                    // for equality safely
                    fields1[j] = fields1[j].split(",")[0];
                    fields2[j] = fields2[j].split(",")[0];
                    try {
                        Double double1 = Double.parseDouble(fields1[j]);
                        Double double2 = Double.parseDouble(fields2[j]);
                        float float1 = (float) double1.doubleValue();
                        float float2 = (float) double2.doubleValue();

                        if (Math.abs(float1 - float2) == 0)
                            continue;
                        else {
                            return false;
                        }
                    } catch (NumberFormatException ignored) {
                        // Guess they weren't numbers - must simply not be equal
                        return false;
                    }
                }
            }
        }
        return true;
    }

    // For tests where you simply want the byte-for-byte output.
    private static void writeOutputToFile(File actualFile, InputStream resultStream) throws Exception {
        byte[] buffer = new byte[10240];
        int len;
        java.io.FileOutputStream out = new java.io.FileOutputStream(actualFile);
        try {
            while ((len = resultStream.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
        } finally {
            out.close();
        }
    }

    private static int executeHttpMethod(HttpMethod method) throws Exception {
        HttpClient client = new HttpClient();
        int statusCode;
        try {
            statusCode = client.executeMethod(method);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            e.printStackTrace();
            throw e;
        }
        if (statusCode != HttpStatus.SC_OK) {
            // QQQ For now, we are indeed assuming we get back JSON errors.
            // In future this may be changed depending on the requested
            // output format sent to the servlet.
            String errorBody = method.getResponseBodyAsString();
            JSONObject result = new JSONObject(errorBody);
            String[] errors = { result.getJSONArray("error-code").getString(0), result.getString("summary"),
                    result.getString("stacktrace") };
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, errors[2]);
            throw new Exception("HTTP operation failed: " + errors[0] + "\nSTATUS LINE: " + method.getStatusLine()
                    + "\nSUMMARY: " + errors[1] + "\nSTACKTRACE: " + errors[2]);
        }
        return statusCode;
    }

    // Executes Query and returns results as JSONArray
    public static InputStream executeQuery(String str, OutputFormat fmt) throws Exception {
        final String url = "http://localhost:19002/query";

        // Create a method instance.
        GetMethod method = new GetMethod(url);
        method.setQueryString(new NameValuePair[] { new NameValuePair("query", str) });
        method.setRequestHeader("Accept", fmt.mimeType());

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
        executeHttpMethod(method);
        return method.getResponseBodyAsStream();
    }

    // To execute Update statements
    // Insert and Delete statements are executed here
    public static void executeUpdate(String str) throws Exception {
        final String url = "http://localhost:19002/update";

        // Create a method instance.
        PostMethod method = new PostMethod(url);
        method.setRequestEntity(new StringRequestEntity(str));

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        // Execute the method.
        executeHttpMethod(method);
    }

    //Executes AQL in either async or async-defer mode.
    public static InputStream executeAnyAQLAsync(String str, boolean defer, OutputFormat fmt) throws Exception {
        final String url = "http://localhost:19002/aql";

        // Create a method instance.
        PostMethod method = new PostMethod(url);
        if (defer) {
            method.setQueryString(new NameValuePair[] { new NameValuePair("mode", "asynchronous-deferred") });
        } else {
            method.setQueryString(new NameValuePair[] { new NameValuePair("mode", "asynchronous") });
        }
        method.setRequestEntity(new StringRequestEntity(str));
        method.setRequestHeader("Accept", fmt.mimeType());

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
        executeHttpMethod(method);
        InputStream resultStream = method.getResponseBodyAsStream();

        String theHandle = IOUtils.toString(resultStream, "UTF-8");

        //take the handle and parse it so results can be retrieved
        InputStream handleResult = getHandleResult(theHandle, fmt);
        return handleResult;
    }

    private static InputStream getHandleResult(String handle, OutputFormat fmt) throws Exception {
        final String url = "http://localhost:19002/query/result";

        // Create a method instance.
        GetMethod method = new GetMethod(url);
        method.setQueryString(new NameValuePair[] { new NameValuePair("handle", handle) });
        method.setRequestHeader("Accept", fmt.mimeType());

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        executeHttpMethod(method);
        return method.getResponseBodyAsStream();
    }

    // To execute DDL and Update statements
    // create type statement
    // create dataset statement
    // create index statement
    // create dataverse statement
    // create function statement
    public static void executeDDL(String str) throws Exception {
        final String url = "http://localhost:19002/ddl";

        // Create a method instance.
        PostMethod method = new PostMethod(url);
        method.setRequestEntity(new StringRequestEntity(str));
        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        // Execute the method.
        executeHttpMethod(method);
    }

    // Method that reads a DDL/Update/Query File
    // and returns the contents as a string
    // This string is later passed to REST API for execution.
    private static String readTestFile(File testFile) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(testFile));
        String line = null;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");

        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }

        return stringBuilder.toString();
    }

    public static void executeManagixCommand(String command) throws ClassNotFoundException, NoSuchMethodException,
            SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        if (managixExecuteMethod == null) {
            Class<?> clazz = Class.forName("edu.uci.ics.asterix.installer.test.AsterixInstallerIntegrationUtil");
            managixExecuteMethod = clazz.getMethod("executeCommand", String.class);
        }
        managixExecuteMethod.invoke(null, command);
    }

    public static String executeScript(ProcessBuilder pb, String scriptPath) throws Exception {
        pb.command(scriptPath);
        Process p = pb.start();
        p.waitFor();
        return getProcessOutput(p);
    }

    private static String getScriptPath(String queryPath, String scriptBasePath, String scriptFileName) {
        String targetWord = "queries" + File.separator;
        int targetWordSize = targetWord.lastIndexOf(File.separator);
        int beginIndex = queryPath.lastIndexOf(targetWord) + targetWordSize;
        int endIndex = queryPath.lastIndexOf(File.separator);
        String prefix = queryPath.substring(beginIndex, endIndex);
        String scriptPath = scriptBasePath + prefix + File.separator + scriptFileName;
        return scriptPath;
    }

    private static String getProcessOutput(Process p) throws Exception {
        StringBuilder s = new StringBuilder();
        BufferedInputStream bisIn = new BufferedInputStream(p.getInputStream());
        StringWriter writerIn = new StringWriter();
        IOUtils.copy(bisIn, writerIn, "UTF-8");
        s.append(writerIn.toString());

        BufferedInputStream bisErr = new BufferedInputStream(p.getErrorStream());
        StringWriter writerErr = new StringWriter();
        IOUtils.copy(bisErr, writerErr, "UTF-8");
        s.append(writerErr.toString());
        if (writerErr.toString().length() > 0) {
            StringBuilder sbErr = new StringBuilder();
            sbErr.append("script execution failed - error message:\n");
            sbErr.append("-------------------------------------------\n");
            sbErr.append(s.toString());
            sbErr.append("-------------------------------------------\n");
            LOGGER.info(sbErr.toString().trim());
            throw new Exception(s.toString().trim());
        }
        return s.toString();
    }

    public static void executeTest(String actualPath, TestCaseContext testCaseCtx, ProcessBuilder pb,
            boolean isDmlRecoveryTest) throws Exception {

        File testFile;
        File expectedResultFile;
        String statement;
        List<TestFileContext> expectedResultFileCtxs;
        List<TestFileContext> testFileCtxs;
        File qbcFile = null;
        File qarFile = null;
        int queryCount = 0;

        List<CompilationUnit> cUnits = testCaseCtx.getTestCase().getCompilationUnit();
        for (CompilationUnit cUnit : cUnits) {

            // Uncomment the following two lines to test only desired test case
            if (!cUnit.getName().startsWith("load-with-fulltext-index"))
              continue;

        	LOGGER.info("Starting [TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName() + " ... ");
            testFileCtxs = testCaseCtx.getTestFiles(cUnit);
            expectedResultFileCtxs = testCaseCtx.getExpectedResultFiles(cUnit);
            for (TestFileContext ctx : testFileCtxs) {
                testFile = ctx.getFile();
                statement = TestsUtils.readTestFile(testFile);
                System.out.println("[TEST] File Seq:" + ctx.getSeqNum());
                try {
                    switch (ctx.getType()) {
                        case "ddl":
                            TestsUtils.executeDDL(statement);
                            break;
                        case "update":
                            //isDmlRecoveryTest: set IP address
                            if (isDmlRecoveryTest && statement.contains("nc1://")) {
                                statement = statement
                                        .replaceAll("nc1://", "127.0.0.1://../../../../../../asterix-app/");
                            }

                            TestsUtils.executeUpdate(statement);
                            break;
                        case "query":
                        case "async":
                        case "asyncdefer":
                            // isDmlRecoveryTest: insert Crash and Recovery
                            if (isDmlRecoveryTest) {
                                executeScript(pb, pb.environment().get("SCRIPT_HOME") + File.separator + "dml_recovery"
                                        + File.separator + "kill_cc_and_nc.sh");
                                executeScript(pb, pb.environment().get("SCRIPT_HOME") + File.separator + "dml_recovery"
                                        + File.separator + "stop_and_start.sh");
                            }
                            InputStream resultStream = null;
                            OutputFormat fmt = OutputFormat.forCompilationUnit(cUnit);
                            if (ctx.getType().equalsIgnoreCase("query"))
                                resultStream = executeQuery(statement, fmt);
                            else if (ctx.getType().equalsIgnoreCase("async"))
                                resultStream = executeAnyAQLAsync(statement, false, fmt);
                            else if (ctx.getType().equalsIgnoreCase("asyncdefer"))
                                resultStream = executeAnyAQLAsync(statement, true, fmt);

                            if (queryCount >= expectedResultFileCtxs.size()) {
                                throw new IllegalStateException("no result file for " + testFile.toString());
                            }
                            expectedResultFile = expectedResultFileCtxs.get(queryCount).getFile();

                            File actualResultFile = testCaseCtx.getActualResultFile(cUnit, new File(actualPath));
                            actualResultFile.getParentFile().mkdirs();
                            TestsUtils.writeOutputToFile(actualResultFile, resultStream);

                            TestsUtils.runScriptAndCompareWithResult(testFile, new PrintWriter(System.err),
                                    expectedResultFile, actualResultFile);
                            LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName()
                                    + " PASSED ");

                            queryCount++;
                            break;
                        case "mgx":
                            executeManagixCommand(statement);
                            break;
                        case "txnqbc": //qbc represents query before crash
                            resultStream = executeQuery(statement, OutputFormat.forCompilationUnit(cUnit));
                            qbcFile = new File(actualPath + File.separator
                                    + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                                    + cUnit.getName() + "_qbc.adm");
                            qbcFile.getParentFile().mkdirs();
                            TestsUtils.writeOutputToFile(qbcFile, resultStream);
                            break;
                        case "txnqar": //qar represents query after recovery
                            resultStream = executeQuery(statement, OutputFormat.forCompilationUnit(cUnit));
                            qarFile = new File(actualPath + File.separator
                                    + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                                    + cUnit.getName() + "_qar.adm");
                            qarFile.getParentFile().mkdirs();
                            TestsUtils.writeOutputToFile(qarFile, resultStream);
                            TestsUtils.runScriptAndCompareWithResult(testFile, new PrintWriter(System.err), qbcFile,
                                    qarFile);

                            LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName()
                                    + " PASSED ");
                            break;
                        case "txneu": //eu represents erroneous update
                            try {
                                TestsUtils.executeUpdate(statement);
                            } catch (Exception e) {
                                //An exception is expected.
                            }
                            break;
                        case "script":
                            try {
                                String output = executeScript(
                                        pb,
                                        getScriptPath(testFile.getAbsolutePath(), pb.environment().get("SCRIPT_HOME"),
                                                statement.trim()));
                                if (output.contains("ERROR")) {
                                    throw new Exception(output);
                                }
                            } catch (Exception e) {
                                throw new Exception("Test \"" + testFile + "\" FAILED!\n", e);
                            }
                            break;
                        case "sleep":
                            Thread.sleep(Long.parseLong(statement.trim()));
                            break;
                        case "errddl": // a ddlquery that expects error
                            try {
                                TestsUtils.executeDDL(statement);

                            } catch (Exception e) {
                                // expected error happens
                            }
                            break;
                        default:
                            throw new IllegalArgumentException("No statements of type " + ctx.getType());
                    }

                } catch (Exception e) {
                    System.err.println("testFile " + testFile.toString() + " raised an exception:");
                    e.printStackTrace();
                    if (cUnit.getExpectedError().isEmpty()) {
                        System.err.println("...Unexpected!");
                        throw new Exception("Test \"" + testFile + "\" FAILED!", e);
                    } else {
                        System.err.println("...but that was expected.");
                        LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName()
                                + " failed as expected: " + e.getMessage());
                    }
                }
            }
        }
    }
}
