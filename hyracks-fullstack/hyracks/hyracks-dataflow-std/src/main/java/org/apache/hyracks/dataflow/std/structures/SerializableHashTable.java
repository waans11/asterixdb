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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
    private ITuplePointerAccessor bufferAccessor;
    private ITuplePartitionComputer tpc;

    public SerializableHashTable(int tableSize, final IHyracksFrameMgrContext ctx) throws HyracksDataException {
        this.ctx = ctx;
        int frameSize = ctx.getInitialFrameSize();

        int residual = tableSize * INT_SIZE * 2 % frameSize == 0 ? 0 : 1;
        int headerSize = tableSize * INT_SIZE * 2 / frameSize + residual;
        headers = new IntSerDeBuffer[headerSize];

        IntSerDeBuffer frame = new IntSerDeBuffer(ctx.allocateFrame().array());
        contents.add(frame);
        totalFrameCount++;
        currentOffsetInEachFrameList.add(0);
        frameCapacity = frame.capacity();
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
                int entrySlotCapacity = frame.getInt(offsetInContentFrame);
                int entryUsedCountInSlot = frame.getInt(offsetInContentFrame + 1);
                // Set used count as 0 in the slot.
                frame.writeInt(offsetInContentFrame + 1, 0);
                tupleCount -= entryUsedCountInSlot;
                wastedIntSpaceCount += ((entrySlotCapacity + 1) * 2);
            }
        }
    }

    @Override
    public boolean getTuplePointer(int entry, int offset, TuplePointer dataPointer) {
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
        if (offset > entryUsedCountInSlot - 1) {
            dataPointer.reset(INVALID_VALUE, INVALID_VALUE);
            return false;
        }
        int startOffsetInContentFrame = offsetInContentFrame + 2 + offset * 2;
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

        if (lastOffsetInCurrentFrame + requiredIntCapacity >= frameCapacity) {
            IntSerDeBuffer newContentFrame;
            currentFrameNumber++;
            do {
                if (currentLargestFrameNumber >= contents.size() - 1) {
                    newContentFrame = new IntSerDeBuffer(ctx.allocateFrame().array());
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
            header.writeInt(offsetInHeaderFrame, INVALID_VALUE);
            header.writeInt(offsetInHeaderFrame + 1, INVALID_VALUE);
            // Make the old slot as obsolete - set the used count as -1 so that its space can be reclaimed
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

    public static int getExpectedByteSizeOfHashTable(int tableSize) {
        return getExpectedByteSizePerHashValue() * tableSize;
    }

    @Override
    public boolean isGarbageCollectioNeeded() {
        return wastedIntSpaceCount >= frameCapacity * contents.size() * garbageCollectionThreshold;
    }

    @Override
    public void executeGarbageCollection(ITuplePointerAccessor bufferAccessor, ITuplePartitionComputer tpc)
            throws HyracksDataException {
        /*
         * #1. Read a content frame.
         * #2. Read a slot information. Check the number of used count for the slot.
         * If it's greater than zero (meaning that it is being used now), we keep it. But, move it towards to the
         * beginning of the frame. Update the corresponding h() value pointer for this location in a header frame.
         * We can find the h() value of the slot using a first tuple pointer in the slot.
         * If the number is zero, reset the corresponding h() value pointer for this location to the initial value
         * (-1,-1) in the header frame, again using the first tuple pointer in the slot. This slot space will be
         * filled by a next valid slot.
         * #3. Once a content frame is read fully, keep the remaining free space in a list.
         * #4. Read the next frame. If a slot can be moved to a free space of another page, then move it to that page.
         * If not, move it towards to the beginning of the page.
         * Note that in case where a slot size is bigger than a frame, the codebase assumes that
         * it starts at offset 0 of a frame. So, we can't move this slot to a previous frame. Instead, the free spaces
         * in the previous frame can be reused later by more smaller slots.
         */

        // For calculating the original hash value
        this.bufferAccessor = bufferAccessor;
        this.tpc = tpc;
        TuplePointer tmpTuplePointer = new TuplePointer();

        // This map keeps the amount free space and the list of pages that has the exact the space.
        // And the keys (amount of free space) are ordered in descending order (large -> small)
        // in order to find the bigger chunk first.
        // (e.g., <4,[0,5,6]> means that the amount of free space in int is 4 (16 bytes), and the pages that
        //        have this amount of free space is 0, 5, and 6.)
        Map<Integer, ArrayList<Integer>> freeSpaceInIntToPageListMap = new TreeMap<Integer, ArrayList<Integer>>(
                new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2.compareTo(o1);
                    }
                });

        // Initialize the reader
        int currentReadPage = 0;
        int readIntPosInPage = 0;
        int slotCapacity = 0;
        int slotUsedCount = 0;
        int capacityInIntCount = 0;
        int nextSlotPosInPage = 0;
        IntSerDeBuffer readContentFrame;
        byte[] readContentFrameArray;
        byte[] writeContentFrameArray;

        // Initialize the writer
        int currentWritePage = 0;
        int writeIntPosInPage = 0;
        int writeIntPosInCurrentPage = 0;
        IntSerDeBuffer writeContentFrame;

        // Step #1. Read a content frame until it reaches the end of content frames.
        while (currentReadPage <= contents.size() - 1) {

            readIntPosInPage = 0;
            nextSlotPosInPage = 0;
            writeIntPosInCurrentPage = 0;
            slotCapacity = 0;
            slotUsedCount = 0;
            readContentFrame = contents.get(currentReadPage);
            readContentFrameArray = readContentFrame.getByteArray();

            // Step #2. Advance the reader until it hits the end of the given frame.
            while (readIntPosInPage < frameCapacity - 1) {
                nextSlotPosInPage = findNextSlotInPage(readContentFrame, readIntPosInPage);

                // Found a valid slot?
                if (nextSlotPosInPage != -1) {
                    // Read slot information
                    slotCapacity = readContentFrame.getInt(nextSlotPosInPage);
                    slotUsedCount = readContentFrame.getInt(nextSlotPosInPage + 1);
                    capacityInIntCount = (slotCapacity + 1) * 2;

                    // Check whether this slot doesn't span multiple pages.
                    if (capacityInIntCount < frameCapacity) {

                        // Slot count should not be zero (spilled to the disk) or -1 (migrated).
                        if (slotUsedCount != 0 && slotUsedCount != -1) {

                            // To prepare hash pointer update, read the first tuple pointer in the old slot.
                            tmpTuplePointer.reset(readContentFrame.getInt(readIntPosInPage + 2),
                                    readContentFrame.getInt(readIntPosInPage + 3));

                            // Find a page that can accommodate this slot.
                            int pageToWrite = findPageWithEnoughFreeSpace(freeSpaceInIntToPageListMap,
                                    capacityInIntCount);
                            if (pageToWrite != -1) {
                                currentWritePage = pageToWrite;
                                writeContentFrame = contents.get(currentWritePage);
                                writeIntPosInPage = currentOffsetInEachFrameList.get(currentWritePage);
                                writeContentFrameArray = writeContentFrame.getByteArray();

                                // Move to the new place.
                                System.arraycopy(readContentFrameArray, readIntPosInPage, writeContentFrameArray,
                                        writeIntPosInPage, capacityInIntCount * INT_SIZE);

                                // Update header to content pointer in header frame.
                                updateHeaderToContentPointerInHeaderFrame(tmpTuplePointer, currentWritePage,
                                        writeIntPosInPage);

                                // Reclaim the space.
                                for (int i = 0; i < capacityInIntCount; i++) {
                                    readContentFrame.writeInt(nextSlotPosInPage + i, INVALID_VALUE);
                                }

                                // Move pointers to the new position
                                writeIntPosInPage += capacityInIntCount;
                                readIntPosInPage += capacityInIntCount;

                                // Update the freespace information
                                currentOffsetInEachFrameList.set(currentWritePage, writeIntPosInPage);
                                int amountOfFreeSpaceInInt = frameCapacity - writeIntPosInPage;

                                // At least, the amount of free space should be greater than initial slot size.
                                if (amountOfFreeSpaceInInt >= INIT_ENTRY_SIZE * 2) {
                                    if (freeSpaceInIntToPageListMap.containsKey(amountOfFreeSpaceInInt)) {
                                        //
                                        if (!freeSpaceInIntToPageListMap.get(amountOfFreeSpaceInInt)
                                                .contains(currentWritePage)) {
                                            freeSpaceInIntToPageListMap.get(amountOfFreeSpaceInInt)
                                                    .add(currentWritePage);
                                        }
                                    } else {
                                        ArrayList<Integer> pageList = new ArrayList<>();
                                        pageList.add(currentWritePage);
                                        freeSpaceInIntToPageListMap.put(amountOfFreeSpaceInInt,
                                                new ArrayList<>(pageList));
                                    }

                                }

                                //                                if (freeSpaceInIntToPageListMap.get(frameCapacity - writeIntPosInPage))
                                //                                freeSpaceInIntToPageListMap.get(frameCapacity - writeIntPosInPage)
                                //                                        .add(currentWritePage);

                            } else {
                                // If the current page enough space, then we can use it.
                                currentWritePage = currentReadPage;
                                writeContentFrame = readContentFrame;
                                writeIntPosInPage = writeIntPosInCurrentPage;
                                writeContentFrameArray = readContentFrameArray;

                                // Compaction is possible if the readPointer is ahead of the writePointer.
                                if (readIntPosInPage > writeIntPosInPage) {

                                    // Move to the new place.
                                    System.arraycopy(readContentFrameArray, readIntPosInPage, writeContentFrameArray,
                                            writeIntPosInPage, capacityInIntCount * INT_SIZE);

                                    // Remove the original slot
                                    for (int i = 0; i < capacityInIntCount; i++) {
                                        // Do not blindly put -1 since there might be overlapping between old and new region.
                                        if (readIntPosInPage + i >= writeIntPosInPage + capacityInIntCount) {
                                            writeContentFrame.writeInt(readIntPosInPage + i, INVALID_VALUE);
                                        }
                                    }

                                    // Update header to content pointer in header frame.
                                    updateHeaderToContentPointerInHeaderFrame(tmpTuplePointer, currentReadPage,
                                            writeIntPosInPage);

                                    // Move pointers to the new position
                                    writeIntPosInCurrentPage += capacityInIntCount;
                                    readIntPosInPage += capacityInIntCount;
                                }
                            }
                        } else {
                            // A useless slot (either migrated or deleted) is found.
                            if (slotUsedCount == 0) {
                                // This is a deleted slot. So, we make sure that the header to content pointer
                                // will not indicate to this location and reclaim the space.
                                tmpTuplePointer.reset(readContentFrame.getInt(nextSlotPosInPage + 2),
                                        readContentFrame.getInt(nextSlotPosInPage + 3));

                                updateHeaderToContentPointerInHeaderFrame(tmpTuplePointer, INVALID_VALUE,
                                        INVALID_VALUE);
                            }

                            // Reclaim the space.
                            for (int i = 0; i < capacityInIntCount; i++) {
                                readContentFrame.writeInt(nextSlotPosInPage + i, INVALID_VALUE);
                            }

                            readIntPosInPage = nextSlotPosInPage + capacityInIntCount;
                        }
                    } else {
                        // Since a slot always starts at the offset 0 of the frame if it spans multiple pages,
                        // we just progress to the next slot.
                        readIntPosInPage += capacityInIntCount;

                        while (readIntPosInPage >= frameCapacity) {
                            currentReadPage++;
                            readIntPosInPage -= frameCapacity;
                        }

                        readContentFrame = contents.get(currentReadPage);
                        // TODO: EDIT THIS
                    }
                } else {
                    // There isn't a valid slot. Exit the loop #2.
                    currentOffsetInEachFrameList.set(currentReadPage, readIntPosInPage);
                    break;
                }
            } // end of while: readIntPosInPage < frameCapacity - 1

            // We reach the end of this page. Advance the reader.
            currentReadPage++;
        }

    }

    private int findPageWithEnoughFreeSpace(Map<Integer, ArrayList<Integer>> freeSpaceInIntToPageListMap,
            int requiredSpaceInInt) {
        for (Map.Entry<Integer, ArrayList<Integer>> entry : freeSpaceInIntToPageListMap.entrySet()) {
            int amountOfFreeSpaceInInt = entry.getKey();
            if (amountOfFreeSpaceInInt >= requiredSpaceInInt) {
                ArrayList<Integer> pages = entry.getValue();
                if (!pages.isEmpty()) {
                    int pageToReturn = pages.get(0);
                    // The amount of free space will be reduced. So, this page needs to be removed in this list.
                    pages.remove(0);
                    // Return the first page
                    return pageToReturn;
                }
            } else {
                // We don't need to keep traversing the map since it is sorted in descending order.
                return -1;
            }
        }
        // No suitable page was found.
        return -1;
    }

    private void updateHeaderToContentPointerInHeaderFrame(TuplePointer hashedTuple, int newContentFrame, int newOffsetInContentFrame)
            throws HyracksDataException {
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

    private int findNextSlotInPage(IntSerDeBuffer frame, int readIntPosAtPage) {
        int intOffset = readIntPosAtPage;
        while (frame.getInt(intOffset) == INVALID_VALUE && intOffset < frameCapacity - 1) {
            intOffset++;
        }
        if (intOffset == frameCapacity - 1) {
            // Couldn't find the next slot in the page.
            return -1;
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

        public byte[] getByteArray() {
            return bytes;
        }
    }
}
