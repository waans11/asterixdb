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
package edu.uci.ics.asterix.api.http.servlet;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.logging.Level;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.api.common.SessionConfig.OutputFormat;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.base.Statement.Kind;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.aql.parser.TokenMgrError;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.result.ResultReader;
import edu.uci.ics.asterix.result.ResultUtils;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.util.ExecutionTimeProfiler;
import edu.uci.ics.hyracks.api.util.OperatorExecutionTimeProfiler;
import edu.uci.ics.hyracks.client.dataset.HyracksDataset;

abstract class RESTAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    private static final String HYRACKS_DATASET_ATTR = "edu.uci.ics.asterix.HYRACKS_DATASET";

    /**
     * Initialize the Content-Type of the response, and construct a
     * SessionConfig with the appropriate output writer and output-format
     * based on the Accept: header and other servlet parameters.
     */
    static SessionConfig initResponse(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
        response.setCharacterEncoding("utf-8");

        // JSON output is the default; most generally useful for a
        // programmatic HTTP API
        OutputFormat format = OutputFormat.JSON;

        // First check the "output" servlet parameter.
        String output = request.getParameter("output");
        String accept = request.getHeader("Accept");
        if (output != null) {
            if (output.equals("CSV")) {
                format = OutputFormat.CSV;
            }
            else if (output.equals("ADM")) {
                format = OutputFormat.ADM;
            }
        }
        else {
            // Second check the Accept: HTTP header.
            if (accept != null) {
                if (accept.contains("application/x-adm")) {
                    format = OutputFormat.ADM;
                } else if (accept.contains("text/csv")) {
                    format = OutputFormat.CSV;
                }
            }
        }

        SessionConfig sessionConfig = new SessionConfig(response.getWriter(), format);

        // Now that format is set, output the content-type
        switch (format) {
            case ADM:
                response.setContentType("application/x-adm");
                break;
            case JSON:
                response.setContentType("application/json");
                break;
            case CSV: {
                // Check for header parameter or in Accept:.
                if ("present".equals(request.getParameter("header")) ||
                    (accept != null && accept.contains("header=present"))) {
                    response.setContentType("text/csv; header=present");
                    sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, true);
                }
                else {
                    response.setContentType("text/csv; header=absent");
                }
            }
        };

        return sessionConfig;
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException {
        StringWriter sw = new StringWriter();
        IOUtils.copy(request.getInputStream(), sw, StandardCharsets.UTF_8.name());
        String query = sw.toString();
        handleRequest(request, response, query);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String query = getQueryParameter(request);
        handleRequest(request, response, query);
    }

    public void handleRequest(HttpServletRequest request, HttpServletResponse response, String query)
            throws IOException {
        SessionConfig sessionConfig = initResponse(request, response);
        AqlTranslator.ResultDelivery resultDelivery = whichResultDelivery(request);

        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        IHyracksDataset hds;

        try {
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
                hds = (IHyracksDataset) context.getAttribute(HYRACKS_DATASET_ATTR);
                if (hds == null) {
                    hds = new HyracksDataset(hcc, ResultReader.FRAME_SIZE, ResultReader.NUM_READERS);
                    context.setAttribute(HYRACKS_DATASET_ATTR, hds);
                }
            }

            AQLParser parser = new AQLParser(query);

//            if(ExperimentProfiler.PROFILE_MODE) {
//                OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add("RESTAPIServlet", "Initialized", false);
//            }

            List<Statement> aqlStatements = parser.parse();
            if (!containsForbiddenStatements(aqlStatements)) {
                MetadataManager.INSTANCE.init();
                AqlTranslator aqlTranslator = new AqlTranslator(aqlStatements, sessionConfig);
                aqlTranslator.compileAndExecute(hcc, hds, resultDelivery);
            }

//            if(ExperimentProfiler.PROFILE_MODE) {
//            	String messageToWrite = "\n\n" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").format(System.currentTimeMillis()) + "\t***** Query:\n" + query;
//                OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add("RESTAPIServlet", messageToWrite + "\n\n", false);
//            }


        } catch (ParseException | TokenMgrError | edu.uci.ics.asterix.aqlplus.parser.TokenMgrError pe) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, pe.getMessage(), pe);
            String errorMessage = ResultUtils.buildParseExceptionMessage(pe, query);
            JSONObject errorResp = ResultUtils.getErrorResponse(2, errorMessage, "", "");
            sessionConfig.out().write(errorResp.toString());
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            ResultUtils.apiErrorHandler(sessionConfig.out(), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    private boolean containsForbiddenStatements(List<Statement> aqlStatements) throws AsterixException {
        for (Statement st : aqlStatements) {
            if (!getAllowedStatements().contains(st.getKind())) {
                throw new AsterixException(String.format(getErrorMessage(), st.getKind()));
            }
        }
        return false;
    }

    protected AqlTranslator.ResultDelivery whichResultDelivery(HttpServletRequest request) {
        String mode = request.getParameter("mode");
        if (mode != null) {
            if (mode.equals("asynchronous")) {
                return AqlTranslator.ResultDelivery.ASYNC;
            } else if (mode.equals("asynchronous-deferred")) {
                return AqlTranslator.ResultDelivery.ASYNC_DEFERRED;
            }
        }
        return AqlTranslator.ResultDelivery.SYNC;
    }

    protected abstract String getQueryParameter(HttpServletRequest request);

    protected abstract List<Kind> getAllowedStatements();

    protected abstract String getErrorMessage();
}
