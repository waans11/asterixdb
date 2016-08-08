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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This table consists of header frames and content frames.
 * .
 * Header indicates the first entry slot location for the given integer value.
 * A header slot consists of [content frame number], [offset in that frame] to get
 * the first tuple's pointer information that shares the same value.
 * .
 * An entry slot in the content frame is as follows.
 * [capacity of the slot], [# of occupied elements], {[frameIndex], [tupleIndex]}+;
 * fIndex, tIndex; .... <fIndex, tIndex> forms a tuple pointer
 */
public class SerializableHashTable implements ISerializableTable {

    private static final int INT_SIZE = 4;
    // Initial entry slot size
    private static final int INIT_ENTRY_SIZE = 4;

    // Header frame array
    private IntSerDeBuffer[] headers;
    // Content frame list
    private List<IntSerDeBuffer> contents = new ArrayList<>();
    private List<Integer> currentOffsetInFrameList = new ArrayList<>();
    private final IHyracksFrameMgrContext ctx;
    private final int frameCapacity;
    private int currentLargestFrameNumber = 0;
    private int tupleCount = 0;
    // The number of total frames that are allocated to the headers and contents
    private int totalFrameCount = 0;
    private TuplePointer tempTuplePointer = new TuplePointer();

    public SerializableHashTable(int tableSize, final IHyracksFrameMgrContext ctx) throws HyracksDataException {
        this.ctx = ctx;
        int frameSize = ctx.getInitialFrameSize();

        int residual = tableSize * INT_SIZE * 2 % frameSize == 0 ? 0 : 1;
        int headerSize = tableSize * INT_SIZE * 2 / frameSize + residual;
        headers = new IntSerDeBuffer[headerSize];

        IntSerDeBuffer frame = new IntSerDeBuffer(ctx.allocateFrame().array());
        contents.add(frame);
        totalFrameCount++;
        currentOffsetInFrameList.add(0);
        frameCapacity = frame.capacity();
    }

    @Override
    public void insert(int entry, TuplePointer pointer) throws HyracksDataException {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer headerFrame = headers[headerFrameIndex];
        if (headerFrame == null) {
            headerFrame = new IntSerDeBuffer(ctx.allocateFrame().array());
            headers[headerFrameIndex] = headerFrame;
            resetFrame(headerFrame);
            totalFrameCount++;
        }
        int contentFrameIndex = headerFrame.getInt(offsetInHeaderFrame);
        if (contentFrameIndex < 0) {
            // Since the initial value of index and offset is -1, this means that the entry slot for
            // this entry is not created yet. So, create the entry slot and insert first tuple into that slot.
            insertNewEntry(headerFrame, offsetInHeaderFrame, INIT_ENTRY_SIZE, pointer);
        } else {
            // The entry slot already exists. Insert non-first tuple into the entry slot
            int offsetInContentFrame = headerFrame.getInt(offsetInHeaderFrame + 1);
            insertNonFirstTuple(headerFrame, offsetInHeaderFrame, contentFrameIndex, offsetInContentFrame, pointer);
        }
        tupleCount++;
    }

    @Override
    // Reset the slot information for the entry. Specifically, we reset the number of used count in the slot as 0.
    public void delete(int entry) {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer header = headers[headerFrameIndex];
        if (header != null) {
            int contentFrameIndex = header.getInt(offsetInHeaderFrame);
            int offsetInContentFrame = header.getInt(offsetInHeaderFrame + 1);
            if (contentFrameIndex >= 0) {
                IntSerDeBuffer frame = contents.get(contentFrameIndex);
                int entryUsedCountInSlot = frame.getInt(offsetInContentFrame + 1);
                // Set used count as 0 in the slot.
                frame.writeInt(offsetInContentFrame + 1, 0);
                tupleCount -= entryUsedCountInSlot;
            }
        }
    }

    @Override
    public boolean getTuplePointer(int entry, int offset, TuplePointer dataPointer) {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer header = headers[headerFrameIndex];
        if (header == null) {
            dataPointer.reset(-1, -1);
            return false;
        }
        int frameIndex = header.getInt(offsetInHeaderFrame);
        int offsetIndex = header.getInt(offsetInHeaderFrame + 1);
        if (frameIndex < 0) {
            dataPointer.reset(-1, -1);
            return false;
        }
        IntSerDeBuffer frame = contents.get(frameIndex);
        int entryUsedItems = frame.getInt(offsetIndex + 1);
        if (offset > entryUsedItems - 1) {
            dataPointer.reset(-1, -1);
            return false;
        }
        int startIndex = offsetIndex + 2 + offset * 2;
        while (startIndex >= frameCapacity) {
            ++frameIndex;
            startIndex -= frameCapacity;
        }
        frame = contents.get(frameIndex);
        dataPointer.reset(frame.getInt(startIndex), frame.getInt(startIndex + 1));
        return true;
    }

    @Override
    public void reset() {
        for (IntSerDeBuffer frame : headers)
            if (frame != null)
                resetFrame(frame);

        currentOffsetInFrameList.clear();
        for (int i = 0; i < contents.size(); i++) {
            currentOffsetInFrameList.add(0);
        }

        currentLargestFrameNumber = 0;
        tupleCount = 0;
    }

    @Override
    public int getFrameCount() {
        return totalFrameCount;
    }

    @Override
    public int getTupleCount() {
        return tupleCount;
    }

    @Override
    /**
     * Returns the tuple count in the slot for the given entry.
     */
    public int getTupleCount(int entry) {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer headerFrame = headers[headerFrameIndex];
        if (headerFrame != null) {
            int contentFrameIndex = headerFrame.getInt(offsetInHeaderFrame);
            int offsetInContentFrame = headerFrame.getInt(offsetInHeaderFrame + 1);
            if (contentFrameIndex >= 0) {
                IntSerDeBuffer frame = contents.get(contentFrameIndex);
                int entryUsedCountInSlot = frame.getInt(offsetInContentFrame + 1);
                return entryUsedCountInSlot;
            }
        }
        return 0;
    }

    @Override
    public void close() {
        int nFrames = contents.size();
        for (int i = 0; i < headers.length; i++)
            headers[i] = null;
        contents.clear();
        currentOffsetInFrameList.clear();
        tupleCount = 0;
        totalFrameCount = 0;
        currentLargestFrameNumber = 0;
        ctx.deallocateFrames(nFrames);
    }

    private void insertNewEntry(IntSerDeBuffer header, int offsetInHeaderFrame, int entryCapacity, TuplePointer pointer)
            throws HyracksDataException {
        IntSerDeBuffer lastContentFrame = contents.get(currentLargestFrameNumber);
        int lastOffsetInCurrentFrame = currentOffsetInFrameList.get(currentLargestFrameNumber);
        int requiredIntCapacity = entryCapacity * 2;
        int currentFrameNumber = currentLargestFrameNumber;

        if (lastOffsetInCurrentFrame + requiredIntCapacity >= frameCapacity) {
            IntSerDeBuffer newContentFrame;
            currentFrameNumber++;
            do {
                if (currentLargestFrameNumber >= contents.size() - 1) {
                    newContentFrame = new IntSerDeBuffer(ctx.allocateFrame().array());
                    currentLargestFrameNumber++;
                    contents.add(newContentFrame);
                    totalFrameCount++;
                    currentOffsetInFrameList.add(0);
                } else {
                    currentLargestFrameNumber++;
                    currentOffsetInFrameList.set(currentLargestFrameNumber, 0);
                }
                requiredIntCapacity -= frameCapacity;
            } while (requiredIntCapacity > 0);
            lastOffsetInCurrentFrame = 0;
            lastContentFrame = contents.get(currentFrameNumber);
        }

        // set header
        header.writeInt(offsetInHeaderFrame, currentFrameNumber);
        header.writeInt(offsetInHeaderFrame + 1, lastOffsetInCurrentFrame);

        // set the entry & its slot.
        // 1. slot capacity
        lastContentFrame.writeInt(lastOffsetInCurrentFrame, entryCapacity - 1);
        // 2. used count in the slot
        lastContentFrame.writeInt(lastOffsetInCurrentFrame + 1, 1);
        // 3. initial entry in the slot
        lastContentFrame.writeInt(lastOffsetInCurrentFrame + 2, pointer.getFrameIndex());
        lastContentFrame.writeInt(lastOffsetInCurrentFrame + 3, pointer.getTupleIndex());
        int newLastOffsetInContentFrame = lastOffsetInCurrentFrame + entryCapacity * 2;
        newLastOffsetInContentFrame = newLastOffsetInContentFrame < frameCapacity ? newLastOffsetInContentFrame
                : frameCapacity - 1;
        currentOffsetInFrameList.set(currentFrameNumber, newLastOffsetInContentFrame);

        requiredIntCapacity = entryCapacity * 2 - (frameCapacity - lastOffsetInCurrentFrame);
        while (requiredIntCapacity > 0) {
            currentFrameNumber++;
            requiredIntCapacity -= frameCapacity;
            newLastOffsetInContentFrame = requiredIntCapacity < 0 ? requiredIntCapacity + frameCapacity
                    : frameCapacity - 1;
            currentOffsetInFrameList.set(currentFrameNumber, newLastOffsetInContentFrame);
        }
    }

    private void insertNonFirstTuple(IntSerDeBuffer header, int offsetInHeaderFrame, int contentFrameIndex,
            int offsetInContentFrame,
            TuplePointer pointer) throws HyracksDataException {
        int frameIndex = contentFrameIndex;
        IntSerDeBuffer contentFrame = contents.get(frameIndex);
        int entrySlotCapacity = contentFrame.getInt(offsetInContentFrame);
        int entryUsedCountInSlot = contentFrame.getInt(offsetInContentFrame + 1);
        boolean frameIndexChanged = false;
        if (entryUsedCountInSlot < entrySlotCapacity) {
            // The slot has at least one space to accommodate this tuple pointer.
            // Increase used count.
            contentFrame.writeInt(offsetInContentFrame + 1, entryUsedCountInSlot + 1);
            // Calculate the first empty spot in the slot.
            // +2: (capacity, # of used entry count)
            // *2: each tuplePointer's occupation (frame index + offset in that frame)
            int startOffsetInContentFrame = offsetInContentFrame + 2 + entryUsedCountInSlot * 2;
            while (startOffsetInContentFrame >= frameCapacity) {
                ++frameIndex;
                startOffsetInContentFrame -= frameCapacity;
                frameIndexChanged = true;
            }
            // We don't have to read content frame again if the frame index has not been changed.
            if (frameIndexChanged) {
                contentFrame = contents.get(frameIndex);
            }
            contentFrame.writeInt(startOffsetInContentFrame, pointer.getFrameIndex());
            contentFrame.writeInt(startOffsetInContentFrame + 1, pointer.getTupleIndex());
        } else {
            // There is no enough space in this slot.We need to increase the slot size and
            // migrate the current entries in it.

            // New capacity: double the original capacity
            int capacity = (entrySlotCapacity + 1) * 2;
            // Temporarily set the header as -1 for the slot.
            header.writeInt(offsetInHeaderFrame, -1);
            header.writeInt(offsetInHeaderFrame + 1, -1);
            // Get the location of the initial entry.
            int fIndex = contentFrame.getInt(offsetInContentFrame + 2);
            int tIndex = contentFrame.getInt(offsetInContentFrame + 3);
            tempTuplePointer.reset(fIndex, tIndex);
            // Create a new double-sized slot for the current entries and
            // migrate the initial entry in the slot to the new slot.
            this.insertNewEntry(header, offsetInHeaderFrame, capacity, tempTuplePointer);
            int newFrameIndex = header.getInt(offsetInHeaderFrame);
            int newTupleIndex = header.getInt(offsetInHeaderFrame + 1);
            boolean firstIterInTheLoop = true;

            // Migrate the existing entries (from 2nd to the last).
            for (int i = 1; i < entryUsedCountInSlot; i++) {
                int startOffsetInContentFrame = offsetInContentFrame + 2 + i * 2;
                int startFrameIndex = frameIndex;
                while (startOffsetInContentFrame >= frameCapacity) {
                    ++startFrameIndex;
                    startOffsetInContentFrame -= frameCapacity;
                }
                if (firstIterInTheLoop || startFrameIndex != frameIndex) {
                    // Only read content frame in case frameINdex is changed or if this is the first iteration.
                    contentFrame = contents.get(startFrameIndex);
                    firstIterInTheLoop = false;
                }
                fIndex = contentFrame.getInt(startOffsetInContentFrame);
                tIndex = contentFrame.getInt(startOffsetInContentFrame + 1);
                tempTuplePointer.reset(fIndex, tIndex);
                insertNonFirstTuple(header, offsetInHeaderFrame, newFrameIndex, newTupleIndex, tempTuplePointer);
            }
            // Now, insert the new entry that caused an overflow to the old bucket.
            insertNonFirstTuple(header, offsetInHeaderFrame, newFrameIndex, newTupleIndex, pointer);
        }
    }

    private void resetFrame(IntSerDeBuffer frame) {
        for (int i = 0; i < frameCapacity; i++)
            frame.writeInt(i, -1);
    }

    private int getHeaderFrameIndex(int entry) {
        int frameIndex = entry * 2 / frameCapacity;
        return frameIndex;
    }

    private int getHeaderFrameOffset(int entry) {
        int offset = entry * 2 % frameCapacity;
        return offset;
    }

    private static class IntSerDeBuffer {

        private byte[] bytes;

        public IntSerDeBuffer(byte[] data) {
            this.bytes = data;
        }

        public int getInt(int pos) {
            int offset = pos * 4;
            return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16)
                    + ((bytes[offset + 2] & 0xff) << 8) + ((bytes[offset + 3] & 0xff) << 0);
        }

        public void writeInt(int pos, int value) {
            int offset = pos * 4;
            bytes[offset++] = (byte) (value >> 24);
            bytes[offset++] = (byte) (value >> 16);
            bytes[offset++] = (byte) (value >> 8);
            bytes[offset++] = (byte) (value);
        }

        public int capacity() {
            return bytes.length / 4;
        }
    }
}
