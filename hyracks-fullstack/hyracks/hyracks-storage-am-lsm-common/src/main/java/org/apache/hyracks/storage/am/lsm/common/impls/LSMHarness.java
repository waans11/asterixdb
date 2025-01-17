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

package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;

public class LSMHarness implements ILSMHarness {
    private static final Logger LOGGER = Logger.getLogger(LSMHarness.class.getName());

    protected final ILSMIndex lsmIndex;
    protected final ILSMMergePolicy mergePolicy;
    protected final ILSMOperationTracker opTracker;
    protected final AtomicBoolean fullMergeIsRequested;
    protected final boolean replicationEnabled;
    protected List<ILSMDiskComponent> componentsToBeReplicated;

    public LSMHarness(ILSMIndex lsmIndex, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            boolean replicationEnabled) {
        this.lsmIndex = lsmIndex;
        this.opTracker = opTracker;
        this.mergePolicy = mergePolicy;
        fullMergeIsRequested = new AtomicBoolean();
        //only durable indexes are replicated
        this.replicationEnabled = replicationEnabled && lsmIndex.isDurable();
        if (replicationEnabled) {
            this.componentsToBeReplicated = new ArrayList<>();
        }
    }

    protected boolean getAndEnterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType,
            boolean isTryOperation) throws HyracksDataException {
        validateOperationEnterComponentsState(ctx);
        synchronized (opTracker) {
            while (true) {
                lsmIndex.getOperationalComponents(ctx);
                // Before entering the components, prune those corner cases that indeed should not proceed.
                switch (opType) {
                    case FLUSH:
                        ILSMComponent flushingComponent = ctx.getComponentHolder().get(0);
                        if (!((AbstractLSMMemoryComponent) flushingComponent).isModified()) {
                            //The mutable component has not been modified by any writer. There is nothing to flush.
                            //since the component is empty, set its state back to READABLE_WRITABLE
                            if (((AbstractLSMIndex) lsmIndex)
                                    .getCurrentMutableComponentState() == ComponentState.READABLE_UNWRITABLE) {
                                ((AbstractLSMIndex) lsmIndex)
                                        .setCurrentMutableComponentState(ComponentState.READABLE_WRITABLE);
                            }
                            return false;
                        }
                        if (((AbstractLSMMemoryComponent) flushingComponent).getWriterCount() > 0) {
                            /*
                             * This case is a case where even though FLUSH log was flushed to disk and scheduleFlush is triggered,
                             * the current in-memory component (whose state was changed to READABLE_WRITABLE (RW)
                             * from READABLE_UNWRITABLE(RU) before FLUSH log was written to log tail (which is memory buffer of log file)
                             * and then the state was changed back to RW (as shown in the following scenario)) can have writers
                             * based on the current code base/design.
                             * Thus, the writer count of the component may be greater than 0.
                             * if this happens, intead of throwing exception, scheduleFlush() deal with this situation by not flushing
                             * the component.
                             * Please see issue 884 for more detail information:
                             * https://code.google.com/p/asterixdb/issues/detail?id=884&q=owner%3Akisskys%40gmail.com&colspec=ID%20Type%20Status%20Priority%20Milestone%20Owner%20Summary%20ETA%20Severity
                             *
                             */
                            return false;
                        }
                        break;
                    case MERGE:
                        if (ctx.getComponentHolder().size() < 2) {
                            // There is only a single component. There is nothing to merge.
                            return false;
                        }
                    default:
                        break;
                }
                if (enterComponents(ctx, opType)) {
                    return true;
                } else if (isTryOperation) {
                    return false;
                }
                try {
                    // Flush and merge operations should never reach this wait call, because they are always try operations.
                    // If they fail to enter the components, then it means that there are an ongoing flush/merge operation on
                    // the same components, so they should not proceed.
                    opTracker.wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    protected boolean enterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType)
            throws HyracksDataException {
        validateOperationEnterComponentsState(ctx);
        List<ILSMComponent> components = ctx.getComponentHolder();
        int numEntered = 0;
        boolean entranceSuccessful = false;
        try {
            for (ILSMComponent c : components) {
                boolean isMutableComponent = numEntered == 0 && c.getType() == LSMComponentType.MEMORY ? true : false;
                if (!c.threadEnter(opType, isMutableComponent)) {
                    break;
                }
                numEntered++;
            }
            entranceSuccessful = numEntered == components.size();
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (!entranceSuccessful) {
                int i = 0;
                for (ILSMComponent c : components) {
                    if (numEntered == 0) {
                        break;
                    }
                    boolean isMutableComponent = i == 0 && c.getType() == LSMComponentType.MEMORY ? true : false;
                    c.threadExit(opType, true, isMutableComponent);
                    i++;
                    numEntered--;
                }
                return false;
            }
            ctx.setAccessingComponents(true);
        }
        // Check if there is any action that is needed to be taken based on the operation type
        switch (opType) {
            case FLUSH:
                lsmIndex.getIOOperationCallback().beforeOperation(LSMOperationType.FLUSH);
                // Changing the flush status should *always* precede changing the mutable component.
                lsmIndex.changeFlushStatusForCurrentMutableCompoent(false);
                lsmIndex.changeMutableComponent();
                // Notify all waiting threads whenever a flush has been scheduled since they will check
                // again if they can grab and enter the mutable component.
                opTracker.notifyAll();
                break;
            case MERGE:
                lsmIndex.getIOOperationCallback().beforeOperation(LSMOperationType.MERGE);
            default:
                break;
        }
        opTracker.beforeOperation(lsmIndex, opType, ctx.getSearchOperationCallback(), ctx.getModificationCallback());
        return true;
    }

    private void exitComponents(ILSMIndexOperationContext ctx, LSMOperationType opType, ILSMDiskComponent newComponent,
            boolean failedOperation) throws HyracksDataException, IndexException {
        /**
         * FLUSH and MERGE operations should always exit the components
         * to notify waiting threads.
         */
        if (!ctx.isAccessingComponents() && opType != LSMOperationType.FLUSH && opType != LSMOperationType.MERGE) {
            return;
        }
        List<ILSMDiskComponent> inactiveDiskComponents = null;
        List<ILSMDiskComponent> inactiveDiskComponentsToBeDeleted = null;
        try {
            synchronized (opTracker) {
                try {

                    /**
                     * [flow control]
                     * If merge operations are lagged according to the merge policy,
                     * flushing in-memory components are hold until the merge operation catches up.
                     * See PrefixMergePolicy.isMergeLagging() for more details.
                     */
                    if (opType == LSMOperationType.FLUSH) {
                        while (mergePolicy.isMergeLagging(lsmIndex)) {
                            try {
                                opTracker.wait();
                            } catch (InterruptedException e) {
                                //ignore
                            }
                        }
                    } else if (opType == LSMOperationType.MERGE) {
                        opTracker.notifyAll();
                    }

                    int i = 0;
                    // First check if there is any action that is needed to be taken based on the state of each component.
                    for (ILSMComponent c : ctx.getComponentHolder()) {
                        boolean isMutableComponent = i == 0 && c.getType() == LSMComponentType.MEMORY ? true : false;
                        c.threadExit(opType, failedOperation, isMutableComponent);
                        if (c.getType() == LSMComponentType.MEMORY) {
                            switch (c.getState()) {
                                case READABLE_UNWRITABLE:
                                    if (isMutableComponent && (opType == LSMOperationType.MODIFICATION
                                            || opType == LSMOperationType.FORCE_MODIFICATION)) {
                                        lsmIndex.changeFlushStatusForCurrentMutableCompoent(true);
                                    }
                                    break;
                                case INACTIVE:
                                    ((AbstractLSMMemoryComponent) c).reset();
                                    // Notify all waiting threads whenever the mutable component's has change to inactive. This is important because
                                    // even though we switched the mutable components, it is possible that the component that we just switched
                                    // to is still busy flushing its data to disk. Thus, the notification that was issued upon scheduling the flush
                                    // is not enough.
                                    opTracker.notifyAll();
                                    break;
                                default:
                                    break;
                            }
                        } else {
                            switch (c.getState()) {
                                case INACTIVE:
                                    lsmIndex.addInactiveDiskComponent((AbstractLSMDiskComponent) c);
                                    break;
                                default:
                                    break;
                            }
                        }
                        i++;
                    }
                    ctx.setAccessingComponents(false);
                    // Then, perform any action that is needed to be taken based on the operation type.
                    switch (opType) {
                        case FLUSH:
                            // newComponent is null if the flush op. was not performed.
                            if (newComponent != null) {
                                lsmIndex.addDiskComponent(newComponent);
                                if (replicationEnabled) {
                                    componentsToBeReplicated.clear();
                                    componentsToBeReplicated.add(newComponent);
                                    triggerReplication(componentsToBeReplicated, false, opType);
                                }
                                mergePolicy.diskComponentAdded(lsmIndex, false);
                            }
                            break;
                        case MERGE:
                            // newComponent is null if the merge op. was not performed.
                            if (newComponent != null) {
                                lsmIndex.subsumeMergedComponents(newComponent, ctx.getComponentHolder());
                                if (replicationEnabled) {
                                    componentsToBeReplicated.clear();
                                    componentsToBeReplicated.add(newComponent);
                                    triggerReplication(componentsToBeReplicated, false, opType);
                                }
                                mergePolicy.diskComponentAdded(lsmIndex, fullMergeIsRequested.get());
                            }
                            break;
                        default:
                            break;
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    throw e;
                } finally {
                    if (failedOperation && (opType == LSMOperationType.MODIFICATION
                            || opType == LSMOperationType.FORCE_MODIFICATION)) {
                        //When the operation failed, completeOperation() method must be called
                        //in order to decrement active operation count which was incremented in beforeOperation() method.
                        opTracker.completeOperation(lsmIndex, opType, ctx.getSearchOperationCallback(),
                                ctx.getModificationCallback());
                    } else {
                        opTracker.afterOperation(lsmIndex, opType, ctx.getSearchOperationCallback(),
                                ctx.getModificationCallback());
                    }

                    /*
                     * = Inactive disk components lazy cleanup if any =
                     * Prepare to cleanup inactive diskComponents which were old merged components
                     * and not anymore accessed.
                     * This cleanup is done outside of optracker synchronized block.
                     */
                    inactiveDiskComponents = lsmIndex.getInactiveDiskComponents();
                    if (!inactiveDiskComponents.isEmpty()) {
                        for (ILSMDiskComponent inactiveComp : inactiveDiskComponents) {
                            if (inactiveComp.getFileReferenceCount() == 1) {
                                if (inactiveDiskComponentsToBeDeleted == null) {
                                    inactiveDiskComponentsToBeDeleted = new LinkedList<>();
                                }
                                inactiveDiskComponentsToBeDeleted.add(inactiveComp);
                            }
                        }
                        if (inactiveDiskComponentsToBeDeleted != null) {
                            inactiveDiskComponents.removeAll(inactiveDiskComponentsToBeDeleted);
                        }
                    }
                }
            }
        } finally {
            /*
             * cleanup inactive disk components if any
             */
            if (inactiveDiskComponentsToBeDeleted != null) {
                try {
                    //schedule a replication job to delete these inactive disk components from replicas
                    if (replicationEnabled) {
                        lsmIndex.scheduleReplication(null, inactiveDiskComponentsToBeDeleted, false,
                                ReplicationOperation.DELETE, opType);
                    }

                    for (ILSMComponent c : inactiveDiskComponentsToBeDeleted) {
                        ((AbstractLSMDiskComponent) c).destroy();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        }

    }

    @Override
    public void forceModify(ILSMIndexOperationContext ctx, ITupleReference tuple)
            throws HyracksDataException, IndexException {
        LSMOperationType opType = LSMOperationType.FORCE_MODIFICATION;
        modify(ctx, false, tuple, opType);
    }

    @Override
    public boolean modify(ILSMIndexOperationContext ctx, boolean tryOperation, ITupleReference tuple)
            throws HyracksDataException, IndexException {
        LSMOperationType opType = LSMOperationType.MODIFICATION;
        return modify(ctx, tryOperation, tuple, opType);
    }

    private boolean modify(ILSMIndexOperationContext ctx, boolean tryOperation, ITupleReference tuple,
            LSMOperationType opType) throws HyracksDataException, IndexException {
        if (!lsmIndex.isMemoryComponentsAllocated()) {
            lsmIndex.allocateMemoryComponents();
        }
        boolean failedOperation = false;
        if (!getAndEnterComponents(ctx, opType, tryOperation)) {
            return false;
        }
        try {
            lsmIndex.modify(ctx, tuple);
            // The mutable component is always in the first index.
            AbstractLSMMemoryComponent mutableComponent = (AbstractLSMMemoryComponent) ctx.getComponentHolder().get(0);
            mutableComponent.setModified();
        } catch (Exception e) {
            failedOperation = true;
            throw e;
        } finally {
            exitComponents(ctx, opType, null, failedOperation);
        }
        return true;
    }

    @Override
    public void search(ILSMIndexOperationContext ctx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        LSMOperationType opType = LSMOperationType.SEARCH;
        ctx.setSearchPredicate(pred);
        getAndEnterComponents(ctx, opType, false);
        try {
            ctx.getSearchOperationCallback().before(pred.getLowKey());
            lsmIndex.search(ctx, cursor, pred);
        } catch (HyracksDataException | IndexException e) {
            exitComponents(ctx, opType, null, true);
            throw e;
        }
    }

    @Override
    public void endSearch(ILSMIndexOperationContext ctx) throws HyracksDataException {
        if (ctx.getOperation() == IndexOperation.SEARCH) {
            try {
                exitComponents(ctx, LSMOperationType.SEARCH, null, false);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        if (!getAndEnterComponents(ctx, LSMOperationType.FLUSH, true)) {
            callback.afterFinalize(LSMOperationType.FLUSH, null);
            return;
        }
        lsmIndex.scheduleFlush(ctx, callback);
    }

    @Override
    public void flush(ILSMIndexOperationContext ctx, ILSMIOOperation operation)
            throws HyracksDataException, IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Started a flush operation for index: " + lsmIndex + " ...");
        }

        ILSMDiskComponent newComponent = null;
        try {
            newComponent = lsmIndex.flush(operation);
            operation.getCallback().afterOperation(LSMOperationType.FLUSH, null, newComponent);
            lsmIndex.markAsValid(newComponent);
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            exitComponents(ctx, LSMOperationType.FLUSH, newComponent, false);
            operation.getCallback().afterFinalize(LSMOperationType.FLUSH, newComponent);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Finished the flush operation for index: " + lsmIndex);
        }
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        if (!getAndEnterComponents(ctx, LSMOperationType.MERGE, true)) {
            callback.afterFinalize(LSMOperationType.MERGE, null);
            return;
        }
        lsmIndex.scheduleMerge(ctx, callback);
    }

    @Override
    public void scheduleFullMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        fullMergeIsRequested.set(true);
        if (!getAndEnterComponents(ctx, LSMOperationType.MERGE, true)) {
            // If the merge cannot be scheduled because there is already an ongoing merge on subset/all of the components, then
            // whenever the current merge has finished, it will schedule the full merge again.
            callback.afterFinalize(LSMOperationType.MERGE, null);
            return;
        }
        fullMergeIsRequested.set(false);
        lsmIndex.scheduleMerge(ctx, callback);
    }

    @Override
    public void merge(ILSMIndexOperationContext ctx, ILSMIOOperation operation)
            throws HyracksDataException, IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Started a merge operation for index: " + lsmIndex + " ...");
        }

        ILSMDiskComponent newComponent = null;
        try {
            newComponent = lsmIndex.merge(operation);
            operation.getCallback().afterOperation(LSMOperationType.MERGE, ctx.getComponentHolder(), newComponent);
            lsmIndex.markAsValid(newComponent);
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            exitComponents(ctx, LSMOperationType.MERGE, newComponent, false);
            operation.getCallback().afterFinalize(LSMOperationType.MERGE, newComponent);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Finished the merge operation for index: " + lsmIndex);
        }
    }

    @Override
    public void addBulkLoadedComponent(ILSMDiskComponent c) throws HyracksDataException, IndexException {
        lsmIndex.markAsValid(c);
        synchronized (opTracker) {
            lsmIndex.addDiskComponent(c);
            if (replicationEnabled) {
                componentsToBeReplicated.clear();
                componentsToBeReplicated.add(c);
                triggerReplication(componentsToBeReplicated, true, LSMOperationType.MERGE);
            }
            mergePolicy.diskComponentAdded(lsmIndex, false);
        }
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return opTracker;
    }

    protected void triggerReplication(List<ILSMDiskComponent> lsmComponents, boolean bulkload, LSMOperationType opType)
            throws HyracksDataException {
        ILSMIndexAccessor accessor =
                lsmIndex.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        accessor.scheduleReplication(lsmComponents, bulkload, opType);
    }

    @Override
    public void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMDiskComponent> lsmComponents,
            boolean bulkload, LSMOperationType opType) throws HyracksDataException {

        //enter the LSM components to be replicated to prevent them from being deleted until they are replicated
        if (!getAndEnterComponents(ctx, LSMOperationType.REPLICATE, false)) {
            return;
        }

        lsmIndex.scheduleReplication(ctx, lsmComponents, bulkload, ReplicationOperation.REPLICATE, opType);
    }

    @Override
    public void endReplication(ILSMIndexOperationContext ctx) throws HyracksDataException {
        try {
            exitComponents(ctx, LSMOperationType.REPLICATE, null, false);
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }

    protected void validateOperationEnterComponentsState(ILSMIndexOperationContext ctx) throws HyracksDataException {
        if (ctx.isAccessingComponents()) {
            throw new HyracksDataException("Opeartion already has access to components of index " + lsmIndex);
        }
    }
}
