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
package org.apache.asterix.external.api;

import java.io.Serializable;
import java.util.Map;

import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Base interface for IGenericDatasetAdapterFactory and ITypedDatasetAdapterFactory.
 * Acts as a marker interface indicating that the implementation provides functionality
 * for creating an adapter.
 */
public interface IAdapterFactory extends Serializable {

    /**
     * Returns the display name corresponding to the Adapter type that is created by the factory.
     *
     * @return the display name
     */
    public String getAlias();

    /**
     * Gets a list of partition constraints. A partition constraint can be a
     * requirement to execute at a particular location or could be cardinality
     * constraints indicating the number of instances that need to run in
     * parallel. example, a IDatasourceAdapter implementation written for data
     * residing on the local file system of a node cannot run on any other node
     * and thus has a location partition constraint. The location partition
     * constraint can be expressed as a node IP address or a node controller id.
     * In the former case, the IP address is translated to a node controller id
     * running on the node with the given IP address.
     *
     * @throws AlgebricksException
     * @throws HyracksDataException
     */
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint()
            throws HyracksDataException, AlgebricksException;

    /**
     * Creates an instance of IDatasourceAdapter.
     *
     * @param HyracksTaskContext
     * @param partition
     * @return An instance of IDatasourceAdapter.
     * @throws Exception
     */
    public IDataSourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws HyracksDataException;

    /**
     * @param libraryManager
     * @param configuration
     * @throws AlgebricksException
     * @throws HyracksDataException
     */
    public void configure(ILibraryManager libraryManager, Map<String, String> configuration)
            throws HyracksDataException, AlgebricksException;

    public void setOutputType(ARecordType outputType);

    public void setMetaType(ARecordType metaType);

    public ARecordType getOutputType();

    public ARecordType getMetaType();
}
