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
package org.apache.asterix.external.operators;

import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;

// This is an operator that takes a single file index and an array of secondary indexes
// it is intended to be used for
// 1. commit transaction operation
// 2. abort transaction operation
// 3. recover transaction operation
public abstract class AbstractExternalDatasetIndexesOperatorDescriptor
        extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private IIndexDataflowHelperFactory filesIndexDataflowHelperFactory;
    private IndexInfoOperatorDescriptor fileIndexInfo;
    private List<IIndexDataflowHelperFactory> treeIndexesDataflowHelperFactories;
    private List<IndexInfoOperatorDescriptor> treeIndexesInfos;

    public AbstractExternalDatasetIndexesOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory filesIndexDataflowHelperFactory, IndexInfoOperatorDescriptor fileIndexesInfo,
            List<IIndexDataflowHelperFactory> treeIndexesDataflowHelperFactories,
            List<IndexInfoOperatorDescriptor> indexesInfos) {
        super(spec, 0, 0);
        this.filesIndexDataflowHelperFactory = filesIndexDataflowHelperFactory;
        this.fileIndexInfo = fileIndexesInfo;
        this.treeIndexesDataflowHelperFactories = treeIndexesDataflowHelperFactories;
        this.treeIndexesInfos = indexesInfos;
    }

    // opening and closing the index is done inside these methods since we don't always need open indexes
    protected abstract void performOpOnIndex(IIndexDataflowHelperFactory indexDataflowHelperFactory,
            IHyracksTaskContext ctx, IndexInfoOperatorDescriptor fileIndexInfo, int partition) throws Exception;

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
        return new AbstractOperatorNodePushable() {

            @Override
            public void initialize() throws HyracksDataException {
                try {
                    FileSplit fileSplit = fileIndexInfo.getFileSplitProvider().getFileSplits()[partition];
                    FileReference fileRef = fileSplit.getFileReference(ctx.getIOManager());
                    // only in partition of device id = 0, we perform the operation on the files index
                    if (fileRef.getDeviceHandle() == ctx.getIOManager().getIODevices().get(0)) {
                        performOpOnIndex(filesIndexDataflowHelperFactory, ctx, fileIndexInfo, partition);
                    }
                    // perform operation on btrees
                    for (int i = 0; i < treeIndexesDataflowHelperFactories.size(); i++) {
                        performOpOnIndex(treeIndexesDataflowHelperFactories.get(i), ctx, treeIndexesInfos.get(i),
                                partition);
                    }
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void deinitialize() throws HyracksDataException {
            }

            @Override
            public int getInputArity() {
                return 0;
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                    throws HyracksDataException {
            }

            @Override
            public IFrameWriter getInputFrameWriter(int index) {
                return null;
            }

        };
    }
}
