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
package org.apache.hyracks.dataflow.std.structures;

import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;

public interface ISerializableTable {

    /**
     * Make space for the given hash entry. Before calling insert(), this method should be called.
     */
    boolean makeSpaceBeforeInsert(int entry) throws HyracksDataException;

    /**
     * Insert a tuplePointer with the given hash entry. Before calling this method,
     * makeSpaceForInsert() should be called.
     */
    boolean insertAfterMakeSpace(int entry, TuplePointer tuplePointer) throws HyracksDataException;

    /**
     * Normal insert operation. This operation might fail based on the space availability.
     * To make sure to preserve the space before an insertion, use makeSpaceForInsert() and guaranteedInsert().
     */
    boolean insert(int entry, TuplePointer tuplePointer) throws HyracksDataException;

    void delete(int entry);

    /**
     * Cancel the effect of the last insertion.
     */
    void cancelInsert(int entry);

    boolean getTuplePointer(int entry, int offset, TuplePointer tuplePointer);

    /**
     * Return the byte size of entire frames that are currently allocated to the table.
     */
    int getCurrentByteSize();

    int getTupleCount();

    int getTupleCount(int entry);

    void reset();

    void close();

    boolean isGarbageCollectionNeeded();

    /**
     * Execute the garbage collection of the given table.
     *
     * @param bufferAccessor:
     *            required to access the real tuple to calculate the original hash value
     * @param tpc:
     *            hash function
     * @return the number of frames that are reclaimed.
     * @throws HyracksDataException
     */
    int executeGarbageCollection(ITuplePointerAccessor bufferAccessor, ITuplePartitionComputer tpc)
            throws HyracksDataException;
}
