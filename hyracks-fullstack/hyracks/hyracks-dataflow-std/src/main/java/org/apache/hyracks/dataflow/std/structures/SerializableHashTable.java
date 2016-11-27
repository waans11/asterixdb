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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;

/**
 * This is an extension of SimpleSerializableHashTable class.
 * A buffer manager needs to be assigned to allocate/release frames for this table so that
 * the maximum memory usage can be bounded.
 */
public class SerializableHashTable extends SimpleSerializableHashTable {

    protected double garbageCollectionThreshold;
    protected int wastedIntSpaceCount = 0;
    protected ISimpleFrameBufferManager bufferManager;

    public SerializableHashTable(int tableSize, final IHyracksFrameMgrContext ctx,
            ISimpleFrameBufferManager bufferManager) throws HyracksDataException {
        this(tableSize, ctx, bufferManager, 0.1);
    }

    public SerializableHashTable(int tableSize, final IHyracksFrameMgrContext ctx,
            ISimpleFrameBufferManager bufferManager, double garbageCollectionThreshold)
            throws HyracksDataException {
        super(tableSize, ctx, false);
        this.bufferManager = bufferManager;

        ByteBuffer newFrame = getFrame(frameSize);
        if (newFrame == null) {
            throw new HyracksDataException("Can't allocate a frame for Hash Table. Please allocate more budget.");
        }
        IntSerDeBuffer frame = new IntSerDeBuffer(newFrame);
        frameCapacity = frame.capacity();
        contents.add(frame);
        currentOffsetInEachFrameList.add(0);
        this.garbageCollectionThreshold = garbageCollectionThreshold;
    }

    @Override
    ByteBuffer getFrame(int size) throws HyracksDataException {
        ByteBuffer newFrame = bufferManager.acquireFrame(size);
        if (newFrame != null) {
            currentByteSize += size;
        }
        return newFrame;
    }

    @Override
    void increaseWastedSpace(int size) {
        wastedIntSpaceCount += size;
    }

    @Override
    public void reset() {
        super.reset();
        currentByteSize = 0;
    }

    @Override
    public void close() {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] != null) {
                bufferManager.releaseFrame(headers[i].getByteBuffer());
                headers[i] = null;
            }
        }
        for (int i = 0; i < contents.size(); i++) {
            bufferManager.releaseFrame(contents.get(i).getByteBuffer());
        }
        contents.clear();
        currentOffsetInEachFrameList.clear();
        tupleCount = 0;
        currentByteSize = 0;
        currentLargestFrameNumber = 0;
    }

    @Override
    public boolean isGarbageCollectionNeeded() {
        return wastedIntSpaceCount > frameCapacity * (currentLargestFrameNumber + 1) * garbageCollectionThreshold;
    }

    /**
     * Conduct a garbage collection. The steps are as follows.
     * #1. Initialize the Reader and Writer. The frame index is zero at this moment.
     * #2. Read a content frame. Find and read a slot data. Check the number of used count for the slot.
     * If it's not -1 (meaning that it is being used now), we move it to to the
     * current offset of the Writer frame. Update the corresponding h() value pointer for this location
     * in the header frame. We can find the h() value of the slot using a first tuple pointer in the slot.
     * If the number is -1 (meaning that it is migrated to a new place due to an overflow or deleted),
     * just reclaim the space.
     * #3. Once a Reader reaches the end of a frame, read next frame. This applies to the Writer, too.
     * #4. Repeat #1 ~ #3 until all frames are read.
     *
     * @return the number of frames that are reclaimed. The value -1 is returned when no space was reclaimed.
     */
    @Override
    public int collectGarbage(ITuplePointerAccessor bufferAccessor, ITuplePartitionComputer tpc)
            throws HyracksDataException {
        // Keeps the garbage collection related variable
        GarbageCollectionInfo gcInfo = new GarbageCollectionInfo();

        int slotCapacity;
        int slotUsedCount;
        int capacityInIntCount;
        int nextSlotIntPosInPageForGC;
        boolean currentPageChanged;
        IntSerDeBuffer currentReadContentFrameForGC;
        IntSerDeBuffer currentWriteContentFrameForGC = contents.get(gcInfo.currentGCWritePageForGC);
        int lastOffsetInLastFrame = currentOffsetInEachFrameList.get(contents.size() - 1);

        // Step #1. Read a content frame until it reaches the end of content frames.
        while (gcInfo.currentReadPageForGC <= currentLargestFrameNumber) {

            gcInfo.currentReadIntOffsetInPageForGC = 0;
            currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);

            // Step #2. Advance the reader until it hits the end of the given frame.
            while (gcInfo.currentReadIntOffsetInPageForGC < frameCapacity) {
                nextSlotIntPosInPageForGC = findNextSlotInPage(currentReadContentFrameForGC,
                        gcInfo.currentReadIntOffsetInPageForGC);

                if (nextSlotIntPosInPageForGC == INVALID_VALUE) {
                    // There isn't a valid slot in the page. Exit the loop #2 and read the next frame.
                    break;
                }

                // Valid slot found. Read the given slot information
                slotCapacity = currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC);
                slotUsedCount = currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC + 1);
                capacityInIntCount = (slotCapacity + 1) * 2;

                // Used count should not be -1 (spilled to the disk or migrated).
                if (slotUsedCount != INVALID_VALUE) {
                    // To prepare hash pointer (header -> content) update, read the first tuple pointer in the old slot.
                    tempTuplePointer.reset(currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC + 2),
                            currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC + 3));

                    // Check whether there is at least some space to put some part of the slot.
                    // If not, advance the write pointer to the next page.
                    if ((gcInfo.currentWriteIntOffsetInPageForGC + 4) > frameCapacity
                            && gcInfo.currentGCWritePageForGC < currentLargestFrameNumber) {
                        // Swipe the region that can't be used.
                        currentWriteContentFrameForGC.writeInvalidVal(gcInfo.currentWriteIntOffsetInPageForGC,
                                frameCapacity - gcInfo.currentWriteIntOffsetInPageForGC);
                        gcInfo.currentGCWritePageForGC++;
                        currentWriteContentFrameForGC = contents.get(gcInfo.currentGCWritePageForGC);
                        gcInfo.currentWriteIntOffsetInPageForGC = 0;
                    }

                    // Migrate this slot to the current offset in Writer's Frame if possible.
                    currentPageChanged = MigrateSlot(gcInfo, bufferAccessor, tpc, capacityInIntCount,
                            nextSlotIntPosInPageForGC);

                    if (currentPageChanged) {
                        currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);
                        currentWriteContentFrameForGC = contents.get(gcInfo.currentGCWritePageForGC);
                    }
                } else {
                    // A useless slot (either migrated or deleted) is found. Reset the space
                    // so it will be occupied by the next valid slot.
                    currentPageChanged = resetSlotSpace(gcInfo, nextSlotIntPosInPageForGC, capacityInIntCount);

                    if (currentPageChanged) {
                        currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);
                    }

                }
            }

            // We reach the end of a frame. Advance the Reader.
            if (gcInfo.currentReadPageForGC == currentLargestFrameNumber) {
                break;
            }
            gcInfo.currentReadPageForGC++;
        }

        // More unused frames at the end?
        int extraFrames = 0;
        if (contents.size() > (currentLargestFrameNumber + 1)) {
            extraFrames = contents.size() - (currentLargestFrameNumber + 1);
        }

        // Done reading all frames. So, release unnecessary frames.
        int numberOfFramesToBeDeallocated = gcInfo.currentReadPageForGC + extraFrames - gcInfo.currentGCWritePageForGC;

        if (numberOfFramesToBeDeallocated >= 1) {
            for (int i = 0; i < numberOfFramesToBeDeallocated; i++) {
                currentByteSize -= contents.get(gcInfo.currentGCWritePageForGC + 1).getByteCapacity();
                bufferManager.releaseFrame(contents.get(gcInfo.currentGCWritePageForGC + 1).getByteBuffer());
                contents.remove(gcInfo.currentGCWritePageForGC + 1);
                currentOffsetInEachFrameList.remove(gcInfo.currentGCWritePageForGC + 1);
            }
        } else {
            // For this case, we check whether the last offset is changed.
            // If not, we didn't get any space from the operation.
            int afterLastOffsetInLastFrame = currentOffsetInEachFrameList.get(gcInfo.currentGCWritePageForGC);
            if (lastOffsetInLastFrame == afterLastOffsetInLastFrame) {
                numberOfFramesToBeDeallocated = -1;
            }
        }

        // Reset the current offset in the final page so that the future insertions will work without an issue.
        currentLargestFrameNumber = gcInfo.currentGCWritePageForGC;
        currentOffsetInEachFrameList.set(gcInfo.currentGCWritePageForGC, gcInfo.currentWriteIntOffsetInPageForGC);

        wastedIntSpaceCount = 0;
        tempTuplePointer.reset(INVALID_VALUE, INVALID_VALUE);

        return numberOfFramesToBeDeallocated;
    }

    /**
     * Migrate the current slot to the designated place and reset the current space using INVALID_VALUE.
     *
     * @return true if the current page has been changed. false if the current page has not been changed.
     */
    private boolean MigrateSlot(GarbageCollectionInfo gcInfo, ITuplePointerAccessor bufferAccessor,
            ITuplePartitionComputer tpc, int capacityInIntCount, int nextSlotIntPosInPageForGC)
            throws HyracksDataException {
        boolean currentPageChanged = false;
        // If the reader and writer indicate the same slot location, a move is not required.
        if (gcInfo.isReaderWriterAtTheSamePos()) {
            int intToRead = capacityInIntCount;
            int intReadAtThisTime;
            gcInfo.currentReadIntOffsetInPageForGC = nextSlotIntPosInPageForGC;
            while (intToRead > 0) {
                intReadAtThisTime = Math.min(intToRead, frameCapacity - gcInfo.currentReadIntOffsetInPageForGC);
                gcInfo.currentReadIntOffsetInPageForGC += intReadAtThisTime;
                if (gcInfo.currentReadIntOffsetInPageForGC >= frameCapacity
                        && gcInfo.currentReadPageForGC < currentLargestFrameNumber) {
                    gcInfo.currentReadPageForGC++;
                    gcInfo.currentReadIntOffsetInPageForGC = 0;
                    currentPageChanged = true;
                }
                intToRead -= intReadAtThisTime;
            }

            gcInfo.currentGCWritePageForGC = gcInfo.currentReadPageForGC;
            gcInfo.currentWriteIntOffsetInPageForGC = gcInfo.currentReadIntOffsetInPageForGC;

            return currentPageChanged;
        }

        // The reader is ahead of the writer. We can migrate the given slot towards to the beginning of
        // the content frame(s).
        int tempWriteIntPosInPage = gcInfo.currentWriteIntOffsetInPageForGC;
        int tempReadIntPosInPage = nextSlotIntPosInPageForGC;
        int chunksToMove = capacityInIntCount;
        int chunksToMoveAtThisTime;

        // To keep the original writing page that is going to be used for updating the header to content frame,
        // we declare a local variable.
        int tempWritePage = gcInfo.currentGCWritePageForGC;

        // Keeps the maximum INT chunks that writer/reader can write in the current page.
        int oneTimeIntCapacityForWriter;
        int oneTimeIntCapacityForReader;

        IntSerDeBuffer currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);
        IntSerDeBuffer currentWriteContentFrameForGC = contents.get(gcInfo.currentGCWritePageForGC);

        // Move the slot.
        while (chunksToMove > 0) {
            oneTimeIntCapacityForWriter = Math.min(chunksToMove, frameCapacity - tempWriteIntPosInPage);
            oneTimeIntCapacityForReader = Math.min(chunksToMove, frameCapacity - tempReadIntPosInPage);

            // Since the location of Reader and Writer are different, we can only move a minimum chunk
            // before the current page of either Reader or Writer changes.
            chunksToMoveAtThisTime = Math.min(oneTimeIntCapacityForWriter, oneTimeIntCapacityForReader);

            // Moves a part of the slot from the Reader to Writer
            System.arraycopy(currentReadContentFrameForGC.bytes, tempReadIntPosInPage * INT_SIZE,
                    currentWriteContentFrameForGC.bytes, tempWriteIntPosInPage * INT_SIZE,
                    chunksToMoveAtThisTime * INT_SIZE);

            // Clear that part in the Reader
            for (int i = 0; i < chunksToMoveAtThisTime; i++) {
                // Do not blindly put -1 since there might be overlapping between writer and reader.
                if ((gcInfo.currentReadPageForGC != tempWritePage)
                        || (tempReadIntPosInPage + i >= tempWriteIntPosInPage + chunksToMoveAtThisTime)) {
                    currentReadContentFrameForGC.writeInvalidVal(tempReadIntPosInPage + i, chunksToMoveAtThisTime - i);
                    break;
                }
            }

            // Advance the pointer
            tempWriteIntPosInPage += chunksToMoveAtThisTime;
            tempReadIntPosInPage += chunksToMoveAtThisTime;

            // Once the writer pointer hits the end of the page, we move to the next content page.
            if (tempWriteIntPosInPage >= frameCapacity && tempWritePage < currentLargestFrameNumber) {
                tempWritePage++;
                currentPageChanged = true;
                currentWriteContentFrameForGC = contents.get(tempWritePage);
                tempWriteIntPosInPage = 0;
            }

            // Once the reader pointer hits the end of the page, we move to the next content page.
            if (tempReadIntPosInPage >= frameCapacity && gcInfo.currentReadPageForGC < currentLargestFrameNumber) {
                gcInfo.currentReadPageForGC++;
                currentPageChanged = true;
                currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);
                tempReadIntPosInPage = 0;
            }

            chunksToMove -= chunksToMoveAtThisTime;
        }

        updateHeaderToContentPointerInHeaderFrame(bufferAccessor, tpc, tempTuplePointer, gcInfo.currentGCWritePageForGC,
                gcInfo.currentWriteIntOffsetInPageForGC);

        gcInfo.currentGCWritePageForGC = tempWritePage;
        gcInfo.currentWriteIntOffsetInPageForGC = tempWriteIntPosInPage;
        gcInfo.currentReadIntOffsetInPageForGC = tempReadIntPosInPage;

        return currentPageChanged;
    }

    /**
     * Completely remove the slot in the given content frame(s) and reset the space.
     * For this method, we assume that this slot is not moved to somewhere else.
     *
     * @return true if the current page has been changed. false if the current page has not been changed.
     */
    private boolean resetSlotSpace(GarbageCollectionInfo gcInfo, int slotIntPos, int capacityInIntCount) {
        boolean currentPageChanged = false;
        int tempReadIntPosInPage = slotIntPos;
        int chunksToDelete = capacityInIntCount;
        int chunksToDeleteAtThisTime;
        IntSerDeBuffer currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);

        while (chunksToDelete > 0) {
            chunksToDeleteAtThisTime = Math.min(chunksToDelete, frameCapacity - tempReadIntPosInPage);

            // Clear that part in the Reader
            currentReadContentFrameForGC.writeInvalidVal(tempReadIntPosInPage, chunksToDeleteAtThisTime);

            // Advance the pointer
            tempReadIntPosInPage += chunksToDeleteAtThisTime;

            // Once the reader pointer hits the end of the page, we move to the next content page.
            if (tempReadIntPosInPage >= frameCapacity && gcInfo.currentReadPageForGC < currentLargestFrameNumber) {
                gcInfo.currentReadPageForGC++;
                currentPageChanged = true;
                currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);
                tempReadIntPosInPage = 0;
            }

            chunksToDelete -= chunksToDeleteAtThisTime;
        }

        gcInfo.currentReadIntOffsetInPageForGC = tempReadIntPosInPage;

        return currentPageChanged;
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
     * Try to find the next valid slot position in the given content frame from the current position.
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

    /**
     * Keeps the garbage collection related variables
     */
    private static class GarbageCollectionInfo {
        // For garbage collection
        int currentReadPageForGC;
        int currentReadIntOffsetInPageForGC;
        int currentGCWritePageForGC;
        int currentWriteIntOffsetInPageForGC;

        public GarbageCollectionInfo() {
            currentReadPageForGC = 0;
            currentReadIntOffsetInPageForGC = 0;
            currentGCWritePageForGC = 0;
            currentWriteIntOffsetInPageForGC = 0;
        }

        public boolean isReaderWriterAtTheSamePos() {
            return currentReadPageForGC == currentGCWritePageForGC
                    && currentReadIntOffsetInPageForGC == currentWriteIntOffsetInPageForGC;
        }
    }


}
