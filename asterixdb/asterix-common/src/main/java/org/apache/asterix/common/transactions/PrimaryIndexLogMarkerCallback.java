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
package org.apache.asterix.common.transactions;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.utils.ComponentMetadataUtil;

/**
 * A basic callback used to write marker to transaction logs
 */
public class PrimaryIndexLogMarkerCallback implements ILogMarkerCallback {
    private final LongPointable pointable = LongPointable.FACTORY.createPointable();
    private final ILSMIndex index;

    /**
     * @param index:
     *            a pointer to the primary index used to store marker log info
     * @throws HyracksDataException
     */
    public PrimaryIndexLogMarkerCallback(ILSMIndex index) throws HyracksDataException {
        this.index = index;
    }

    @Override
    public void before(ByteBuffer buffer) {
        buffer.putLong(getLsn());
    }

    private long getLsn() {
        long lsn;
        try {
            lsn = ComponentMetadataUtil.getLong(index.getCurrentMemoryComponent().getMetadata(),
                    ComponentMetadataUtil.MARKER_LSN_KEY, ComponentMetadataUtil.NOT_FOUND);
        } catch (HyracksDataException e) {
            // Should never happen since this is a memory component
            throw new IllegalStateException(e);
        }
        if (lsn == ComponentMetadataUtil.NOT_FOUND) {
            synchronized (index.getOperationTracker()) {
                // look for it in previous memory component if exists
                lsn = lsnFromImmutableMemoryComponents();
                if (lsn == ComponentMetadataUtil.NOT_FOUND) {
                    // look for it in disk component
                    lsn = lsnFromDiskComponents();
                }
            }
        }
        return lsn;
    }

    private long lsnFromDiskComponents() {
        List<ILSMDiskComponent> diskComponents = index.getImmutableComponents();
        for (ILSMDiskComponent c : diskComponents) {
            try {
                long lsn = ComponentMetadataUtil.getLong(c.getMetadata(), ComponentMetadataUtil.MARKER_LSN_KEY,
                        ComponentMetadataUtil.NOT_FOUND);
                if (lsn != ComponentMetadataUtil.NOT_FOUND) {
                    return lsn;
                }
            } catch (HyracksDataException e) {
                throw new IllegalStateException("Unable to read metadata page. Disk Error?", e);
            }
        }
        return ComponentMetadataUtil.NOT_FOUND;
    }

    private long lsnFromImmutableMemoryComponents() {
        List<ILSMMemoryComponent> memComponents = index.getMemoryComponents();
        int numOtherMemComponents = memComponents.size() - 1;
        int next = index.getCurrentMemoryComponentIndex();
        long lsn = ComponentMetadataUtil.NOT_FOUND;
        for (int i = 0; i < numOtherMemComponents; i++) {
            next = next - 1;
            if (next < 0) {
                next = memComponents.size() - 1;
            }
            ILSMMemoryComponent c = index.getMemoryComponents().get(next);
            if (c.isReadable()) {
                try {
                    lsn = ComponentMetadataUtil.getLong(c.getMetadata(), ComponentMetadataUtil.MARKER_LSN_KEY,
                            ComponentMetadataUtil.NOT_FOUND);
                } catch (HyracksDataException e) {
                    // Should never happen since this is a memory component
                    throw new IllegalStateException(e);
                }
                if (lsn != ComponentMetadataUtil.NOT_FOUND) {
                    return lsn;
                }
            }
        }
        return lsn;
    }

    @Override
    public void after(long lsn) {
        pointable.setLong(lsn);
        index.getCurrentMemoryComponent().getMetadata().put(ComponentMetadataUtil.MARKER_LSN_KEY, pointable);
    }
}
