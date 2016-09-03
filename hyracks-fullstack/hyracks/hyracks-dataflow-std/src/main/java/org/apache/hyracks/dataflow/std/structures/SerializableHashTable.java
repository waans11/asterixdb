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
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;

/**
 * This table consists of header frames and content frames.
 * .
 * Header indicates the first entry slot location for the given integer value.
 * A header slot consists of [content frame number], [offset in that frame] to get
 * the first tuple's pointer information that shares the same hash value.
 * .
 * An entry slot in the content frame is as follows.
 * [capacity of the slot], [# of occupied elements], {[frameIndex], [tupleIndex]}+;
 * <fIndex, tIndex> forms a tuple pointer
 */
public class SerializableHashTable implements ISerializableTable {

    // unit size: int
    private static final int INT_SIZE = 4;
    // Initial entry slot size
    private static final int INIT_ENTRY_SIZE = 4;
    private static double garbageCollectionThreshold = 0.10;
    private static int INVALID_VALUE = -1;

    // Header frame array
    private IntSerDeBuffer[] headers;
    // Content frame list
    private List<IntSerDeBuffer> contents = new ArrayList<>();
    private List<Integer> currentOffsetInEachFrameList = new ArrayList<>();
    private final IHyracksFrameMgrContext ctx;
    private final int frameCapacity;
    private int currentLargestFrameNumber = 0;
    private int tupleCount = 0;
    // The number of total frames that are allocated to the headers and contents
    private int totalFrameCount = 0;
    private TuplePointer tempTuplePointer = new TuplePointer();
    // Keep track of wasted spaces due to the migration of an entry slot in a content frame
    private int wastedIntSpaceCount = 0;
    private int tableSize;

    // For garbage collection
    int currentReadPageForGC = 0;
    int currentReadIntOffsetInPageForGC = 0;
    int nextSlotIntPosInPageForGC = 0;
    int currentGCWritePageForGC = 0;
    int currentWriteIntOffsetInPageForGC = 0;
    IntSerDeBuffer currentReadContentFrameForGC;
    IntSerDeBuffer currentWriteContentFrameForGC;

    public SerializableHashTable(int tableSize, final IHyracksFrameMgrContext ctx) throws HyracksDataException {
        this.ctx = ctx;
        int frameSize = ctx.getInitialFrameSize();

        int residual = tableSize * INT_SIZE * 2 % frameSize == 0 ? 0 : 1;
        int headerSize = tableSize * INT_SIZE * 2 / frameSize + residual;
        headers = new IntSerDeBuffer[headerSize];

        IntSerDeBuffer frame = new IntSerDeBuffer(ctx.allocateFrame().array());
        frameCapacity = frame.capacity();
        resetFrame(frame);
        contents.add(frame);
        totalFrameCount++;
        currentOffsetInEachFrameList.add(0);
        this.tableSize = tableSize;
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
            // Since the initial value of index and offset is -1, this means that the slot for
            // this entry is not created yet. So, create the entry slot and insert first tuple into that slot.
            // OR, the previous slot becomes full and the newly double-sized slot is about to be created.
            insertNewEntry(headerFrame, offsetInHeaderFrame, INIT_ENTRY_SIZE, pointer);
        } else {
            // The entry slot already exists. Insert non-first tuple into the entry slot
            int offsetInContentFrame = headerFrame.getInt(offsetInHeaderFrame + 1);
            insertNonFirstTuple(headerFrame, offsetInHeaderFrame, contentFrameIndex, offsetInContentFrame, pointer);
        }
        tupleCount++;
    }

    @Override
    /**
     * Reset the slot information for the entry. The connection (pointer) between header frame and
     * content frame will be also lost. Specifically, we reset the number of used count in the slot as -1
     * so that the space could be reclaimed.
     */
    public void delete(int entry) {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer header = headers[headerFrameIndex];
        if (header != null) {
            int contentFrameIndex = header.getInt(offsetInHeaderFrame);
            int offsetInContentFrame = header.getInt(offsetInHeaderFrame + 1);
            if (contentFrameIndex >= 0) {
                IntSerDeBuffer frame = contents.get(contentFrameIndex);
                int entrySlotCapacity = frame.getInt(offsetInContentFrame);
                int entryUsedCountInSlot = frame.getInt(offsetInContentFrame + 1);
                // Set used count as -1 in the slot so that the slot space could be reclaimed.
                frame.writeInt(offsetInContentFrame + 1, INVALID_VALUE);
                // Also reset the header to content frame pointer.
                header.writeInt(offsetInHeaderFrame, INVALID_VALUE);
                header.writeInt(offsetInHeaderFrame + 1, INVALID_VALUE);

                tupleCount -= entryUsedCountInSlot;
                wastedIntSpaceCount += ((entrySlotCapacity + 1) * 2);
            }
        }
    }

    @Override
    /**
     * For the given integer value, get the n-th (n = offsetInSlot) tuple pointer in the corresponding slot.
     */
    public boolean getTuplePointer(int entry, int offsetInSlot, TuplePointer dataPointer) {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer header = headers[headerFrameIndex];
        if (header == null) {
            dataPointer.reset(INVALID_VALUE, INVALID_VALUE);
            return false;
        }
        int contentFrameIndex = header.getInt(offsetInHeaderFrame);
        int offsetInContentFrame = header.getInt(offsetInHeaderFrame + 1);
        if (contentFrameIndex < 0) {
            dataPointer.reset(INVALID_VALUE, INVALID_VALUE);
            return false;
        }
        IntSerDeBuffer frame = contents.get(contentFrameIndex);
        int entryUsedCountInSlot = frame.getInt(offsetInContentFrame + 1);
        if (offsetInSlot > entryUsedCountInSlot - 1) {
            dataPointer.reset(INVALID_VALUE, INVALID_VALUE);
            return false;
        }
        int startOffsetInContentFrame = offsetInContentFrame + 2 + offsetInSlot * 2;
        while (startOffsetInContentFrame >= frameCapacity) {
            ++contentFrameIndex;
            startOffsetInContentFrame -= frameCapacity;
        }
        frame = contents.get(contentFrameIndex);
        dataPointer.reset(frame.getInt(startOffsetInContentFrame), frame.getInt(startOffsetInContentFrame + 1));
        return true;
    }

    @Override
    public void reset() {
        for (IntSerDeBuffer frame : headers)
            if (frame != null)
                resetFrame(frame);

        currentOffsetInEachFrameList.clear();
        for (int i = 0; i < contents.size(); i++) {
            currentOffsetInEachFrameList.add(0);
        }

        currentLargestFrameNumber = 0;
        tupleCount = 0;
        totalFrameCount = 0;
        wastedIntSpaceCount = 0;
        currentReadPageForGC = 0;
        currentReadIntOffsetInPageForGC = 0;
        nextSlotIntPosInPageForGC = 0;
        currentGCWritePageForGC = 0;
        currentWriteIntOffsetInPageForGC = 0;
        currentReadContentFrameForGC = null;
        currentWriteContentFrameForGC = null;
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
        currentOffsetInEachFrameList.clear();
        tupleCount = 0;
        totalFrameCount = 0;
        currentLargestFrameNumber = 0;
        ctx.deallocateFrames(nFrames);
    }

    private void insertNewEntry(IntSerDeBuffer header, int offsetInHeaderFrame, int entryCapacity, TuplePointer pointer)
            throws HyracksDataException {
        IntSerDeBuffer lastContentFrame = contents.get(currentLargestFrameNumber);
        int lastOffsetInCurrentFrame = currentOffsetInEachFrameList.get(currentLargestFrameNumber);
        int requiredIntCapacity = entryCapacity * 2;
        int currentFrameNumber = currentLargestFrameNumber;
        boolean currentFrameNumberChanged = false;

        if (lastOffsetInCurrentFrame + requiredIntCapacity >= frameCapacity) {
            IntSerDeBuffer newContentFrame;
            // At least we need to have the mata-data (slot capacity and used count) and
            // one tuplePointer in the same frame (4 INT_SIZE).
            // So, if there is not enough space for this, we just move on to the next page.
            if ((lastOffsetInCurrentFrame + 4) > frameCapacity) {
                // Swipe the region that can't be used.
                for (int i = 0; i < frameCapacity - lastOffsetInCurrentFrame; i++) {
                    lastContentFrame.writeInt(lastOffsetInCurrentFrame + i, INVALID_VALUE);
                }
                currentFrameNumber++;
                lastOffsetInCurrentFrame = 0;
                currentFrameNumberChanged = true;
            }
            do {
                if (currentLargestFrameNumber >= contents.size() - 1) {
                    newContentFrame = new IntSerDeBuffer(ctx.allocateFrame().array());
                    resetFrame(newContentFrame);
                    currentLargestFrameNumber++;
                    contents.add(newContentFrame);
                    totalFrameCount++;
                    currentOffsetInEachFrameList.add(0);
                } else {
                    currentLargestFrameNumber++;
                    currentOffsetInEachFrameList.set(currentLargestFrameNumber, 0);
                }
                requiredIntCapacity -= frameCapacity;
            } while (requiredIntCapacity > 0);
        }

        if (currentFrameNumberChanged) {
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
        currentOffsetInEachFrameList.set(currentFrameNumber, newLastOffsetInContentFrame);

        requiredIntCapacity = entryCapacity * 2 - (frameCapacity - lastOffsetInCurrentFrame);
        while (requiredIntCapacity > 0) {
            currentFrameNumber++;
            requiredIntCapacity -= frameCapacity;
            newLastOffsetInContentFrame = requiredIntCapacity < 0 ? requiredIntCapacity + frameCapacity
                    : frameCapacity - 1;
            currentOffsetInEachFrameList.set(currentFrameNumber, newLastOffsetInContentFrame);
        }
    }

    private void insertNonFirstTuple(IntSerDeBuffer header, int offsetInHeaderFrame, int contentFrameIndex,
            int offsetInContentFrame, TuplePointer pointer) throws HyracksDataException {
        int frameIndex = contentFrameIndex;
        IntSerDeBuffer contentFrame = contents.get(frameIndex);
        int entrySlotCapacity = contentFrame.getInt(offsetInContentFrame);
        int entryUsedCountInSlot = contentFrame.getInt(offsetInContentFrame + 1);
        boolean frameIndexChanged = false;
        if (entryUsedCountInSlot < entrySlotCapacity) {
            // The slot has at least one space to accommodate this tuple pointer.
            // Increase the used count by 1.
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
            // There is no enough space in this slot. We need to increase the slot size and
            // migrate the current entries in it.

            // New capacity: double the original capacity
            int capacity = (entrySlotCapacity + 1) * 2;
            // Temporarily set the header as -1 for the slot.
            header.writeInt(offsetInHeaderFrame, INVALID_VALUE);
            header.writeInt(offsetInHeaderFrame + 1, INVALID_VALUE);
            // Mark the old slot as obsolete - set the used count as -1 so that its space can be reclaimed
            // when a garbage collection is executed.
            contentFrame.writeInt(offsetInContentFrame + 1, INVALID_VALUE);
            // Get the location of the initial entry.
            int fIndex = contentFrame.getInt(offsetInContentFrame + 2);
            int tIndex = contentFrame.getInt(offsetInContentFrame + 3);
            tempTuplePointer.reset(fIndex, tIndex);
            // Create a new double-sized slot for the current entries and
            // migrate the initial entry in the slot to the new slot.
            this.insertNewEntry(header, offsetInHeaderFrame, capacity, tempTuplePointer);
            int newFrameIndex = header.getInt(offsetInHeaderFrame);
            int newTupleIndex = header.getInt(offsetInHeaderFrame + 1);

            // Migrate the existing entries (from 2nd to the last).
            for (int i = 1; i < entryUsedCountInSlot; i++) {
                int startOffsetInContentFrame = offsetInContentFrame + 2 + i * 2;
                int startFrameIndex = frameIndex;
                while (startOffsetInContentFrame >= frameCapacity) {
                    ++startFrameIndex;
                    startOffsetInContentFrame -= frameCapacity;
                }
                contentFrame = contents.get(startFrameIndex);
                fIndex = contentFrame.getInt(startOffsetInContentFrame);
                tIndex = contentFrame.getInt(startOffsetInContentFrame + 1);
                tempTuplePointer.reset(fIndex, tIndex);
                insertNonFirstTuple(header, offsetInHeaderFrame, newFrameIndex, newTupleIndex, tempTuplePointer);
            }
            // Now, insert the new entry that caused an overflow to the old bucket.
            insertNonFirstTuple(header, offsetInHeaderFrame, newFrameIndex, newTupleIndex, pointer);
            wastedIntSpaceCount += capacity;
        }
    }

    private void resetFrame(IntSerDeBuffer frame) {
        for (int i = 0; i < frameCapacity; i++)
            frame.writeInt(i, INVALID_VALUE);
    }

    private int getHeaderFrameIndex(int entry) {
        int frameIndex = entry * 2 / frameCapacity;
        return frameIndex;
    }

    private int getHeaderFrameOffset(int entry) {
        int offset = entry * 2 % frameCapacity;
        return offset;
    }

    public static int getUnitSize() {
        return INT_SIZE;
    }

    public static int getNumberOfEntryInSlot() {
        return INIT_ENTRY_SIZE;
    }

    public static int getExpectedByteSizePerHashValue() {
        // first constant 2: capacity, # of used count
        // second constant 2: tuple pointer (frameIndex, offset)
        return getUnitSize() * (2 + getNumberOfEntryInSlot() * 2);
    }

    public static int getExpectedByteSizeOfHashTable(int tableSize, int frameSize) {
        int numberOfHeaderFrame = (int) Math.ceil(tableSize * 2 / frameSize);
        int numberOfContentFrame = (int) Math
                .ceil((getNumberOfEntryInSlot() * 2 * getUnitSize() * tableSize) / frameSize);
        return (numberOfHeaderFrame + numberOfContentFrame) * frameSize;
    }

    @Override
    public boolean isGarbageCollectioNeeded() {
        return wastedIntSpaceCount >= frameCapacity * (currentLargestFrameNumber + 1) * garbageCollectionThreshold;
    }

    /**
     * Conduct a garbage collection. The steps are as follows.
     * #1. Initialize the Reader and Writer.
     * #2. Read a content frame. Find and read a slot data. Check the number of used count for the slot.
     * If it's not -1 (meaning that it is being used now), we move it to to the
     * current offset of the Writer frame. Update the corresponding h() value pointer for this location
     * in the header frame. We can find the h() value of the slot using a first tuple pointer in the slot.
     * If the number is -1 (meaning that it is migrated to a new place due to an overflow or deleted),
     * just reclaim the space.
     * #3. Once a Reader reaches the end of a frame, read next frame. This applies to the Writer, too.
     * #4. Repeat #1 ~ #3 until all frames are read.
     */
    @Override
    public int executeGarbageCollection(ITuplePointerAccessor bufferAccessor, ITuplePartitionComputer tpc)
            throws HyracksDataException {
        // For calculating the original hash value
        TuplePointer tmpTuplePointerForGC = new TuplePointer();

        // Initialize the reader
        currentReadPageForGC = 0;
        currentReadIntOffsetInPageForGC = 0;
        int slotCapacity = 0;
        int slotUsedCount = 0;
        int capacityInIntCount = 0;
        nextSlotIntPosInPageForGC = 0;
        currentReadContentFrameForGC = contents.get(currentReadPageForGC);

        // Initialize the writer
        currentGCWritePageForGC = 0;
        currentWriteIntOffsetInPageForGC = 0;
        currentWriteContentFrameForGC = contents.get(currentGCWritePageForGC);

        // Step #1. Read a content frame until it reaches the end of content frames.
        while (currentReadPageForGC <= currentLargestFrameNumber) {

            currentReadIntOffsetInPageForGC = 0;
            nextSlotIntPosInPageForGC = INVALID_VALUE;
            currentReadContentFrameForGC = contents.get(currentReadPageForGC);
            slotCapacity = INVALID_VALUE;
            slotUsedCount = INVALID_VALUE;

            // Step #2. Advance the reader until it hits the end of the given frame.
            while (currentReadIntOffsetInPageForGC < frameCapacity) {
                nextSlotIntPosInPageForGC = findNextSlotInPage(currentReadContentFrameForGC,
                        currentReadIntOffsetInPageForGC);

                // Found a valid slot?
                if (nextSlotIntPosInPageForGC != INVALID_VALUE) {
                    // Read the given slot information
                    slotCapacity = currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC);
                    slotUsedCount = currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC + 1);
                    capacityInIntCount = (slotCapacity + 1) * 2;

                    // Used count should not be -1 (spilled to the disk or migrated).
                    if (slotUsedCount != INVALID_VALUE) {

                        // To prepare hash pointer update, read the first tuple pointer in the old slot.
                        tmpTuplePointerForGC.reset(currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC + 2),
                                currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC + 3));

                        // Check whether there is at least some space to put some part of the slot.
                        // If not, advance the write pointer to the next page.
                        if ((currentWriteIntOffsetInPageForGC + 4) > frameCapacity
                                && currentGCWritePageForGC < currentLargestFrameNumber) {
                            // Swipe the region that can't be used.
                            for (int i = 0; i < frameCapacity - currentWriteIntOffsetInPageForGC; i++) {
                                currentWriteContentFrameForGC.writeInt(currentWriteIntOffsetInPageForGC + i,
                                        INVALID_VALUE);
                            }
                            currentGCWritePageForGC++;
                            currentWriteContentFrameForGC = contents.get(currentGCWritePageForGC);
                            currentWriteIntOffsetInPageForGC = 0;
                        }

                        // Migrate this slot to the current offset in Writer's Frame if possible.
                        MigrateSlot(bufferAccessor, tpc, capacityInIntCount, tmpTuplePointerForGC);
                    } else {
                        // A useless slot (either migrated or deleted) is found. Reset the space
                        // so it will be occupied by the next valid slot.
                        reclaimSlotSpace(nextSlotIntPosInPageForGC, capacityInIntCount);
                    }
                } else {
                    // There isn't a valid slot in the page. Exit the loop #2 and read the next frame.
                    break;
                }
            }

            // We reach the end of a frame. Advance the Reader.
            if (currentReadPageForGC == currentLargestFrameNumber) {
                break;
            }
            currentReadPageForGC++;
        }

        // Done reading all frames. So, deallocate unnecessary frames.
        int numberOfFramesToBeDeallocated = currentReadPageForGC - currentGCWritePageForGC;

        if (numberOfFramesToBeDeallocated >= 1) {
            for (int i = 0; i < numberOfFramesToBeDeallocated; i++) {
                contents.remove(currentGCWritePageForGC + 1);
                currentOffsetInEachFrameList.remove(currentGCWritePageForGC + 1);
            }
            totalFrameCount -= numberOfFramesToBeDeallocated;
        }

        // Reset the current offset in the final page so that the future insertions will work without an issue.
        currentLargestFrameNumber = currentGCWritePageForGC;
        currentOffsetInEachFrameList.set(currentGCWritePageForGC, currentWriteIntOffsetInPageForGC);

        return numberOfFramesToBeDeallocated;
    }

    /**
     * Migrate the current slot to the designated place and reset the current space using INVALID_VALUE.
     */
    private void MigrateSlot(ITuplePointerAccessor bufferAccessor, ITuplePartitionComputer tpc, int capacityInIntCount,
            TuplePointer tempTuplePointer) throws HyracksDataException {
        // If the reader and writer indicate the same slot location, a move is not required.
        if (currentReadPageForGC == currentGCWritePageForGC
                && currentWriteIntOffsetInPageForGC == currentReadIntOffsetInPageForGC) {

            int intToRead = capacityInIntCount;
            int intReadAtThisTime;
            currentReadIntOffsetInPageForGC = nextSlotIntPosInPageForGC;
            while (intToRead > 0) {
                intReadAtThisTime = Math.min(intToRead, (frameCapacity - currentReadIntOffsetInPageForGC));
                currentReadIntOffsetInPageForGC += intReadAtThisTime;
                if (currentReadIntOffsetInPageForGC >= frameCapacity
                        && currentReadPageForGC < currentLargestFrameNumber) {
                    currentReadPageForGC++;
                    currentReadIntOffsetInPageForGC = 0;
                }
                intToRead -= intReadAtThisTime;
            }

            currentReadContentFrameForGC = contents.get(currentReadPageForGC);
            currentGCWritePageForGC = currentReadPageForGC;
            currentWriteIntOffsetInPageForGC = currentReadIntOffsetInPageForGC;
            currentWriteContentFrameForGC = contents.get(currentGCWritePageForGC);

            return;
        }

        int tempWriteIntPosInPage = currentWriteIntOffsetInPageForGC;
        int tempReadIntPosInPage = nextSlotIntPosInPageForGC;
        int chunksToMove = capacityInIntCount;
        int chunksToMoveAtThisTime;
        // To keep the original writing page that is going to be used for updating the header to content frame,
        // we declare a local variable.
        int tempWritePage = currentGCWritePageForGC;

        // Keeps the maximum INT chunks that writer/reader can write in the current page.
        int oneTimeIntCapacityForWriter;
        int oneTimeIntCapacityForReader;

        while (chunksToMove > 0) {
            oneTimeIntCapacityForWriter = Math.min(chunksToMove, (frameCapacity - tempWriteIntPosInPage));
            oneTimeIntCapacityForReader = Math.min(chunksToMove, (frameCapacity - tempReadIntPosInPage));

            chunksToMoveAtThisTime = Math.min(oneTimeIntCapacityForWriter, oneTimeIntCapacityForReader);

            // Moves a part of the slot from the Reader to Writer
            System.arraycopy(currentReadContentFrameForGC.bytes, tempReadIntPosInPage * INT_SIZE,
                    currentWriteContentFrameForGC.bytes, tempWriteIntPosInPage * INT_SIZE,
                    chunksToMoveAtThisTime * INT_SIZE);

            // Clear that part in the Reader
            for (int i = 0; i < chunksToMoveAtThisTime; i++) {
                // Do not blindly put -1 since there might be overlapping between writer and reader.
                if ((currentReadPageForGC != tempWritePage)
                        || (tempReadIntPosInPage + i >= tempWriteIntPosInPage + chunksToMoveAtThisTime)) {
                    currentReadContentFrameForGC.writeInt(tempReadIntPosInPage + i, INVALID_VALUE);
                }
            }

            // Advance the pointer
            tempWriteIntPosInPage += chunksToMoveAtThisTime;
            tempReadIntPosInPage += chunksToMoveAtThisTime;

            // Once the writer pointer hits the end of the page, we move to the next content page.
            if (tempWriteIntPosInPage >= frameCapacity && tempWritePage < currentLargestFrameNumber) {
                tempWritePage++;
                currentWriteContentFrameForGC = contents.get(tempWritePage);
                tempWriteIntPosInPage = 0;
            }

            // Once the reader pointer hits the end of the page, we move to the next content page.
            if (tempReadIntPosInPage >= frameCapacity && currentReadPageForGC < currentLargestFrameNumber) {
                currentReadPageForGC++;
                currentReadContentFrameForGC = contents.get(currentReadPageForGC);
                tempReadIntPosInPage = 0;
            }

            chunksToMove -= chunksToMoveAtThisTime;
        }

        updateHeaderToContentPointerInHeaderFrame(bufferAccessor, tpc, tempTuplePointer, currentGCWritePageForGC,
                currentWriteIntOffsetInPageForGC);

        currentGCWritePageForGC = tempWritePage;
        currentWriteIntOffsetInPageForGC = tempWriteIntPosInPage;
        currentReadIntOffsetInPageForGC = tempReadIntPosInPage;

    }

    /**
     * Completely remove the slot in the given content frame(s) and reclaim the space.
     * For this method, we assume that this slot is not moved to somewhere else.
     */
    private void reclaimSlotSpace(int slotIntPos, int capacityInIntCount) {
        int tempReadIntPosInPage = slotIntPos;
        int chunksToDelete = capacityInIntCount;
        int chunksToDeleteAtThisTime;

        while (chunksToDelete > 0) {
            chunksToDeleteAtThisTime = Math.min(chunksToDelete, (frameCapacity - tempReadIntPosInPage));

            // Clear that part in the Reader
            for (int i = 0; i < chunksToDeleteAtThisTime; i++) {
                currentReadContentFrameForGC.writeInt(tempReadIntPosInPage + i, INVALID_VALUE);
            }

            // Advance the pointer
            tempReadIntPosInPage += chunksToDeleteAtThisTime;

            // Once the reader pointer hits the end of the page, we move to the next content page.
            if (tempReadIntPosInPage >= frameCapacity && currentReadPageForGC < currentLargestFrameNumber) {
                currentReadPageForGC++;
                currentReadContentFrameForGC = contents.get(currentReadPageForGC);
                tempReadIntPosInPage = 0;
            }

            chunksToDelete -= chunksToDeleteAtThisTime;
        }

        currentReadIntOffsetInPageForGC = tempReadIntPosInPage;
    }

    /**
     * Update the given Header to Content Frame Pointer after calculating the corresponding hash value from the
     * given tuple pointer.
     */
    private void updateHeaderToContentPointerInHeaderFrame(ITuplePointerAccessor bufferAccessor,
            ITuplePartitionComputer tpc, TuplePointer hashedTuple, int newContentFrame,
            int newOffsetInContentFrame) throws HyracksDataException {
        // Find the original hash value. We assume that bufferAccessor and tpc is already assigned.
        bufferAccessor.reset(hashedTuple);
        int entry = tpc.partition(bufferAccessor, hashedTuple.getTupleIndex(), tableSize);

        // Find the location of the hash value in the header frame arrays.
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer headerFrame = headers[headerFrameIndex];

        // Update the hash value.
        headerFrame.writeInt(offsetInHeaderFrame, newContentFrame);
        headerFrame.writeInt(offsetInHeaderFrame + 1, newOffsetInContentFrame);
    }


    /**
     * Try to find a next valid slot in the given content frame.
     */
    private int findNextSlotInPage(IntSerDeBuffer frame, int readIntPosAtPage) {
        // Sanity check
        if (readIntPosAtPage >= frameCapacity) {
            return INVALID_VALUE;
        }
        int intOffset = readIntPosAtPage;
        while (frame.getInt(intOffset) == INVALID_VALUE) {
            intOffset++;
            if (intOffset >= frameCapacity) {
                // Couldn't find the next slot in the given page.
                return INVALID_VALUE;
            }
        }
        return intOffset;
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
