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
package org.apache.asterix.api.http.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.runtime.utils.AppContextInfo;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.StaticResourceServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryWebInterfaceServlet extends StaticResourceServlet {
    private static final Logger LOGGER = Logger.getLogger(QueryWebInterfaceServlet.class.getName());

    public QueryWebInterfaceServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        String requestURI = request.getHttpRequest().uri();
        if ("/".equals(requestURI)) {
            HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML);
            deliverResource("/queryui/queryui.html", response);
        } else {
            deliverResource(requestURI, response);
        }
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);
        ExternalProperties externalProperties = AppContextInfo.INSTANCE.getExternalProperties();
        response.setStatus(HttpResponseStatus.OK);
        ObjectMapper om = new ObjectMapper();
        ObjectNode obj = om.createObjectNode();
        try {
            PrintWriter out = response.writer();
            obj.put("api_port", String.valueOf(externalProperties.getAPIServerPort()));
            out.println(obj.toString());
            return;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failure writing response", e);
        }
        try {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failure setting response status", e);
        }
    }
}
