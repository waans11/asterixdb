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

package org.apache.hyracks.storage.am.common.dataflow;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;

public class IndexCreateOperatorNodePushable extends AbstractOperatorNodePushable {
    private final IIndexDataflowHelper indexHelper;

    public IndexCreateOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        this.indexHelper = opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(opDesc, ctx, partition);
    }

    @Override
    public void deinitialize() throws HyracksDataException {
    }

    @Override
    public int getInputArity() {
        return 0;
    }

    @Override
    public IFrameWriter getInputFrameWriter(int index) {
        return null;
    }

    @Override
    public void initialize() throws HyracksDataException {
        indexHelper.create();
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
    }
}
