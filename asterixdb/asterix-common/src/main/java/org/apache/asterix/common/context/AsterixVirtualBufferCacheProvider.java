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
package org.apache.asterix.common.context;

import java.util.List;

import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;

public class AsterixVirtualBufferCacheProvider implements IVirtualBufferCacheProvider {

    private static final long serialVersionUID = 1L;
    private final int datasetID;

    public AsterixVirtualBufferCacheProvider(int datasetID) {
        this.datasetID = datasetID;
    }

    @Override
    public List<IVirtualBufferCache> getVirtualBufferCaches(IHyracksTaskContext ctx,
            IFileSplitProvider fileSplitProvider) throws HyracksDataException {
        final int partition = ctx.getTaskAttemptId().getTaskId().getPartition();
        IIOManager ioManager = ctx.getIOManager();
        FileSplit fileSplit = fileSplitProvider.getFileSplits()[partition];
        FileReference fileRef = fileSplit.getFileReference(ioManager);
        IODeviceHandle device = fileRef.getDeviceHandle();
        List<IODeviceHandle> devices = ioManager.getIODevices();
        int deviceId = 0;
        for (int i = 0; i < devices.size(); i++) {
            IODeviceHandle next = devices.get(i);
            if (next == device) {
                deviceId = i;
            }
        }
        return ((IAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getDatasetLifecycleManager().getVirtualBufferCaches(datasetID, deviceId);
    }

}
