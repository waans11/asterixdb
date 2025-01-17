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

import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.utils.FlushDatasetUtil;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * The REST API that takes a dataverse name and a dataset name as the input
 * and returns an array of file splits (IP, file-path) of the dataset in LOSSLESS_JSON.
 * It is mostly used by external runtime, e.g., Pregelix or IMRU to pull data
 * in parallel from existing AsterixDB datasets.
 */
public class ConnectorApiServlet extends AbstractServlet {
    private static final Logger LOGGER = Logger.getLogger(ConnectorApiServlet.class.getName());

    public ConnectorApiServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) {
        response.setStatus(HttpResponseStatus.OK);
        try {
            HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, HttpUtil.Encoding.UTF8);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failure setting content type", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }
        PrintWriter out = response.writer();
        try {
            ObjectMapper om = new ObjectMapper();
            ObjectNode jsonResponse = om.createObjectNode();
            String dataverseName = request.getParameter("dataverseName");
            String datasetName = request.getParameter("datasetName");
            if (dataverseName == null || datasetName == null) {
                jsonResponse.put("error", "Parameter dataverseName or datasetName is null,");
                out.write(jsonResponse.toString());
                out.flush();
                return;
            }

            IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
            // Metadata transaction begins.
            MetadataManager.INSTANCE.init();
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();

            // Retrieves file splits of the dataset.
            MetadataProvider metadataProvider = new MetadataProvider(null, new StorageComponentProvider());
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
            if (dataset == null) {
                jsonResponse.put("error",
                        "Dataset " + datasetName + " does not exist in " + "dataverse " + dataverseName);
                out.write(jsonResponse.toString());
                out.flush();
                return;
            }
            boolean temp = dataset.getDatasetDetails().isTemp();
            FileSplit[] fileSplits =
                    metadataProvider.splitsForDataset(mdTxnCtx, dataverseName, datasetName, datasetName, temp);
            ARecordType recordType = (ARecordType) metadataProvider.findType(dataset.getItemTypeDataverseName(),
                    dataset.getItemTypeName());
            List<List<String>> primaryKeys = DatasetUtil.getPartitioningKeys(dataset);
            StringBuilder pkStrBuf = new StringBuilder();
            for (List<String> keys : primaryKeys) {
                for (String key : keys) {
                    pkStrBuf.append(key).append(",");
                }
            }
            pkStrBuf.delete(pkStrBuf.length() - 1, pkStrBuf.length());

            // Constructs the returned json object.
            formResponseObject(jsonResponse, fileSplits, recordType, pkStrBuf.toString(), temp,
                    hcc.getNodeControllerInfos());

            // Flush the cached contents of the dataset to file system.
            FlushDatasetUtil.flushDataset(hcc, metadataProvider, dataverseName, datasetName, datasetName);

            // Metadata transaction commits.
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            // Writes file splits.
            out.write(jsonResponse.toString());
            out.flush();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failure handling a request", e);
            out.println(e.getMessage());
            out.flush();
            e.printStackTrace(out);
        }
    }

    private void formResponseObject(ObjectNode jsonResponse, FileSplit[] fileSplits, ARecordType recordType,
            String primaryKeys, boolean temp, Map<String, NodeControllerInfo> nodeMap) {
        ObjectMapper om = new ObjectMapper();
        ArrayNode partititons = om.createArrayNode();
        // Whether the dataset is temp or not
        jsonResponse.put("temp", temp);
        // Adds a primary key.
        jsonResponse.put("keys", primaryKeys);
        // Adds record type.
        jsonResponse.set("type", recordType.toJSON());
        // Generates file partitions.
        for (FileSplit split : fileSplits) {
            String ipAddress = nodeMap.get(split.getNodeName()).getNetworkAddress().getAddress();
            String path = split.getPath();
            FilePartition partition = new FilePartition(ipAddress, path);
            partititons.add(partition.toObjectNode());
        }
        // Generates the response object which contains the splits.
        jsonResponse.set("splits", partititons);
    }
}

class FilePartition {
    private final String ipAddress;
    private final String path;

    public FilePartition(String ipAddress, String path) {
        this.ipAddress = ipAddress;
        this.path = path;
    }

    public String getIPAddress() {
        return ipAddress;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return ipAddress + ":" + path;
    }

    public ObjectNode toObjectNode() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode partition = om.createObjectNode();
        partition.put("ip", ipAddress);
        partition.put("path", path);
        return partition;
    }
}
