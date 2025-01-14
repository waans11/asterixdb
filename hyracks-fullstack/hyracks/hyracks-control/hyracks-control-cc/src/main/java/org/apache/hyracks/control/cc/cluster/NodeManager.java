/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.control.cc.cluster;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.resource.NodeCapacity;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.scheduler.IResourceManager;
import org.apache.hyracks.control.common.controllers.CCConfig;

public class NodeManager implements INodeManager {
    private static final Logger LOGGER = Logger.getLogger(NodeManager.class.getName());

    private final CCConfig ccConfig;
    private final IResourceManager resourceManager;
    private final Map<String, NodeControllerState> nodeRegistry;
    private final Map<InetAddress, Set<String>> ipAddressNodeNameMap;

    public NodeManager(CCConfig ccConfig, IResourceManager resourceManager) {
        this.ccConfig = ccConfig;
        this.resourceManager = resourceManager;
        this.nodeRegistry = new LinkedHashMap<>();
        this.ipAddressNodeNameMap = new HashMap<>();
    }

    @Override
    public Map<InetAddress, Set<String>> getIpAddressNodeNameMap() {
        return Collections.unmodifiableMap(ipAddressNodeNameMap);
    }

    @Override
    public Collection<String> getAllNodeIds() {
        return Collections.unmodifiableSet(nodeRegistry.keySet());
    }

    @Override
    public Collection<NodeControllerState> getAllNodeControllerStates() {
        return Collections.unmodifiableCollection(nodeRegistry.values());
    }

    @Override
    public NodeControllerState getNodeControllerState(String nodeId) {
        return nodeRegistry.get(nodeId);
    }

    @Override
    public void addNode(String nodeId, NodeControllerState ncState) throws HyracksException {
        if (nodeId == null || ncState == null) {
            throw HyracksException.create(ErrorCode.INVALID_INPUT_PARAMETER);
        }
        // Updates the node registry.
        if (nodeRegistry.containsKey(nodeId)) {
            LOGGER.warning("Node with name " + nodeId + " has already registered.");
            return;
        }
        nodeRegistry.put(nodeId, ncState);

        // Updates the IP address to node names map.
        try {
            InetAddress ipAddress = getIpAddress(ncState);
            Set<String> nodes = ipAddressNodeNameMap.get(ipAddress);
            if (nodes == null) {
                nodes = new HashSet<>();
                ipAddressNodeNameMap.put(ipAddress, nodes);
            }
            nodes.add(nodeId);
        } catch (HyracksException e) {
            // If anything fails, we ignore the node.
            nodeRegistry.remove(nodeId);
            throw e;
        }

        // Updates the cluster capacity.
        resourceManager.update(nodeId, ncState.getCapacity());
    }

    @Override
    public void removeNode(String nodeId) throws HyracksException {
        NodeControllerState ncState = nodeRegistry.remove(nodeId);
        removeNodeFromIpAddressMap(nodeId, ncState);

        // Updates the cluster capacity.
        resourceManager.update(nodeId, new NodeCapacity(0L, 0));
    }

    @Override
    public Map<String, NodeControllerInfo> getNodeControllerInfoMap() {
        Map<String, NodeControllerInfo> result = new LinkedHashMap<>();
        for (Map.Entry<String, NodeControllerState> e : nodeRegistry.entrySet()) {
            NodeControllerState ncState = e.getValue();
            result.put(e.getKey(), new NodeControllerInfo(e.getKey(), NodeStatus.ALIVE, ncState.getDataPort(),
                    ncState.getDatasetPort(), ncState.getMessagingPort(), ncState.getCapacity().getCores()));
        }
        return result;
    }

    @Override
    public Pair<Collection<String>, Collection<JobId>> removeDeadNodes() throws HyracksException {
        Set<String> deadNodes = new HashSet<>();
        Set<JobId> affectedJobIds = new HashSet<>();
        Iterator<Map.Entry<String, NodeControllerState>> nodeIterator = nodeRegistry.entrySet().iterator();
        while (nodeIterator.hasNext()) {
            Map.Entry<String, NodeControllerState> entry = nodeIterator.next();
            String nodeId = entry.getKey();
            NodeControllerState state = entry.getValue();
            if (state.incrementLastHeartbeatDuration() >= ccConfig.maxHeartbeatLapsePeriods) {
                deadNodes.add(nodeId);
                affectedJobIds.addAll(state.getActiveJobIds());
                // Removes the node from node map.
                nodeIterator.remove();
                // Removes the node from IP map.
                removeNodeFromIpAddressMap(nodeId, state);
                // Updates the cluster capacity.
                resourceManager.update(nodeId, new NodeCapacity(0L, 0));
                LOGGER.info(entry.getKey() + " considered dead");
            }
        }
        return Pair.of(deadNodes, affectedJobIds);
    }

    @Override
    public void apply(NodeFunction nodeFunction) {
        nodeRegistry.forEach(nodeFunction::apply);
    }

    // Removes the entry of the node in <code>ipAddressNodeNameMap</code>.
    private void removeNodeFromIpAddressMap(String nodeId, NodeControllerState ncState) throws HyracksException {
        InetAddress ipAddress = getIpAddress(ncState);
        Set<String> nodes = ipAddressNodeNameMap.get(ipAddress);
        if (nodes != null) {
            nodes.remove(nodeId);
            if (nodes.isEmpty()) {
                // Removes the ip if no corresponding node exists.
                ipAddressNodeNameMap.remove(ipAddress);
            }
        }
    }

    // Retrieves the IP address for a given node.
    private InetAddress getIpAddress(NodeControllerState ncState) throws HyracksException {
        String ipAddress = ncState.getNCConfig().dataIPAddress;
        if (ncState.getNCConfig().dataPublicIPAddress != null) {
            ipAddress = ncState.getNCConfig().dataPublicIPAddress;
        }
        try {
            return InetAddress.getByName(ipAddress);
        } catch (UnknownHostException e) {
            throw HyracksException.create(ErrorCode.INVALID_NETWORK_ADDRESS, e, e.getMessage());
        }
    }

}
