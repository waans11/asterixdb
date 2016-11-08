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

package org.apache.hyracks.dataflow.std.buffermanager;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This is a simple frame based buffer manager that uses a deallocatable frame pool.
 * We assume that the list of assigned buffers are managed by another classes that call the methods of this class.
 */
public class SimpleFrameBufferManager implements ISimpleFrameBufferManager {

    private IDeallocatableFramePool framePool;

    public SimpleFrameBufferManager(IDeallocatableFramePool framePool) {
        this.framePool = framePool;
    }

    @Override
    public ByteBuffer acquireFrame(int frameSize) throws HyracksDataException {
        ByteBuffer newBuffer = framePool.allocateFrame(frameSize);
        if (newBuffer == null) {
            return null;
        }
        return newBuffer;
    }

    @Override
    public void releaseFrame(ByteBuffer frame) {
        framePool.deAllocateBuffer(frame);
    }

}

