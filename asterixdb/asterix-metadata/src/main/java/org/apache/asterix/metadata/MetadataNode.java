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

package org.apache.asterix.metadata;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.IPropertiesProvider;
import org.apache.asterix.common.dataflow.LSMIndexUtil;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.transactions.AbstractOperationCallback;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.IRecoveryManager.ResourceType;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.api.ExtensionMetadataDataset;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;
import org.apache.asterix.metadata.api.IExtensionMetadataSearchKey;
import org.apache.asterix.metadata.api.IMetadataEntityTupleTranslator;
import org.apache.asterix.metadata.api.IMetadataExtension;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.api.IMetadataNode;
import org.apache.asterix.metadata.api.IValueExtractor;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.entitytupletranslators.CompactionPolicyTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatasetTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatasourceAdapterTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatatypeTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DataverseTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.ExternalFileTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FeedConnectionTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FeedPolicyTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FeedTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FunctionTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.IndexTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.LibraryTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.MetadataTupleTranslatorProvider;
import org.apache.asterix.metadata.entitytupletranslators.NodeGroupTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.NodeTupleTranslator;
import org.apache.asterix.metadata.valueextractors.MetadataEntityValueExtractor;
import org.apache.asterix.metadata.valueextractors.TupleCopyValueExtractor;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractComplexType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexModificationOperationCallback;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;

public class MetadataNode implements IMetadataNode {
    private static final long serialVersionUID = 1L;

    private static final DatasetId METADATA_DATASET_ID = new DatasetId(
            MetadataPrimaryIndexes.PROPERTIES_METADATA.getDatasetId());

    // shared between core and extension
    private IDatasetLifecycleManager datasetLifecycleManager;
    private ITransactionSubsystem transactionSubsystem;
    private int metadataStoragePartition;
    // core only
    private transient MetadataTupleTranslatorProvider tupleTranslatorProvider;
    // extension only
    private Map<ExtensionMetadataDatasetId, ExtensionMetadataDataset<?>> extensionDatasets;

    public static final MetadataNode INSTANCE = new MetadataNode();

    private MetadataNode() {
        super();
    }

    public void initialize(IAppRuntimeContext runtimeContext,
            MetadataTupleTranslatorProvider tupleTranslatorProvider, List<IMetadataExtension> metadataExtensions) {
        this.tupleTranslatorProvider = tupleTranslatorProvider;
        this.transactionSubsystem = runtimeContext.getTransactionSubsystem();
        this.datasetLifecycleManager = runtimeContext.getDatasetLifecycleManager();
        this.metadataStoragePartition = ((IPropertiesProvider) runtimeContext).getMetadataProperties()
                .getMetadataPartition().getPartitionId();
        if (metadataExtensions != null) {
            extensionDatasets = new HashMap<>();
            for (IMetadataExtension metadataExtension : metadataExtensions) {
                for (ExtensionMetadataDataset<?> extensionIndex : metadataExtension.getExtensionIndexes()) {
                    extensionDatasets.put(extensionIndex.getId(), extensionIndex);
                }
            }
        }
    }

    @Override
    public void beginTransaction(JobId transactionId) throws ACIDException, RemoteException {
        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().beginTransaction(transactionId);
        txnCtx.setMetadataTransaction(true);
    }

    @Override
    public void commitTransaction(JobId jobId) throws RemoteException, ACIDException {
        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId, false);
        transactionSubsystem.getTransactionManager().commitTransaction(txnCtx, new DatasetId(-1), -1);
    }

    @Override
    public void abortTransaction(JobId jobId) throws RemoteException, ACIDException {
        try {
            ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId,
                    false);
            transactionSubsystem.getTransactionManager().abortTransaction(txnCtx, new DatasetId(-1), -1);
        } catch (ACIDException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void lock(JobId jobId, byte lockMode) throws ACIDException, RemoteException {
        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId, false);
        transactionSubsystem.getLockManager().lock(METADATA_DATASET_ID, -1, lockMode, txnCtx);
    }

    @Override
    public void unlock(JobId jobId, byte lockMode) throws ACIDException, RemoteException {
        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId, false);
        transactionSubsystem.getLockManager().unlock(METADATA_DATASET_ID, -1, lockMode, txnCtx);
    }

    // TODO(amoudi): make all metadata operations go through the generic methods
    /**
     * Add entity to index
     *
     * @param jobId
     * @param entity
     * @param tupleTranslator
     * @param index
     * @throws MetadataException
     */
    private <T> void addEntity(JobId jobId, T entity, IMetadataEntityTupleTranslator<T> tupleTranslator,
            IMetadataIndex index) throws MetadataException {
        try {
            ITupleReference tuple = tupleTranslator.getTupleFromMetadataEntity(entity);
            insertTupleIntoIndex(jobId, index, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException(entity.toString() + " already exists.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    /**
     * Delete entity from index
     *
     * @param jobId
     * @param entity
     * @param tupleTranslator
     * @param index
     * @throws MetadataException
     */
    private <T> void deleteEntity(JobId jobId, T entity, IMetadataEntityTupleTranslator<T> tupleTranslator,
            IMetadataIndex index) throws MetadataException {
        try {
            ITupleReference tuple = tupleTranslator.getTupleFromMetadataEntity(entity);
            deleteTupleFromIndex(jobId, index, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException(entity.toString() + " already exists.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    /**
     * retrieve all entities that matches the searchKey
     *
     * @param jobId
     * @param searchKey
     * @param tupleTranslator
     * @param index
     * @return
     * @throws MetadataException
     */
    private <T> List<T> getEntities(JobId jobId, ITupleReference searchKey,
            IMetadataEntityTupleTranslator<T> tupleTranslator, IMetadataIndex index) throws MetadataException {
        try {
            IValueExtractor<T> valueExtractor = new MetadataEntityValueExtractor<>(tupleTranslator);
            List<T> results = new ArrayList<>();
            searchIndex(jobId, index, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IExtensionMetadataEntity> void addEntity(JobId jobId, T entity)
            throws MetadataException, RemoteException {
        ExtensionMetadataDataset<T> index = (ExtensionMetadataDataset<T>) extensionDatasets.get(entity.getDatasetId());
        if (index == null) {
            throw new MetadataException("Metadata Extension Index: " + entity.getDatasetId() + " was not found");
        }
        IMetadataEntityTupleTranslator<T> tupleTranslator = index.getTupleTranslator();
        addEntity(jobId, entity, tupleTranslator, index);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IExtensionMetadataEntity> void deleteEntity(JobId jobId, T entity)
            throws MetadataException, RemoteException {
        ExtensionMetadataDataset<T> index = (ExtensionMetadataDataset<T>) extensionDatasets.get(entity.getDatasetId());
        if (index == null) {
            throw new MetadataException("Metadata Extension Index: " + entity.getDatasetId() + " was not found");
        }
        IMetadataEntityTupleTranslator<T> tupleTranslator = index.getTupleTranslator();
        deleteEntity(jobId, entity, tupleTranslator, index);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IExtensionMetadataEntity> List<T> getEntities(JobId jobId, IExtensionMetadataSearchKey searchKey)
            throws MetadataException, RemoteException {
        ExtensionMetadataDataset<T> index = (ExtensionMetadataDataset<T>) extensionDatasets
                .get(searchKey.getDatasetId());
        if (index == null) {
            throw new MetadataException("Metadata Extension Index: " + searchKey.getDatasetId() + " was not found");
        }
        IMetadataEntityTupleTranslator<T> tupleTranslator = index.getTupleTranslator();
        return getEntities(jobId, searchKey.getSearchKey(), tupleTranslator, index);
    }

    @Override
    public void addDataverse(JobId jobId, Dataverse dataverse) throws MetadataException, RemoteException {
        try {
            DataverseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataverseTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(dataverse);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.DATAVERSE_DATASET, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException(
                    "A dataverse with this name " + dataverse.getDataverseName() + " already exists.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addDataset(JobId jobId, Dataset dataset) throws MetadataException, RemoteException {
        try {
            // Insert into the 'dataset' dataset.
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(true);
            ITupleReference datasetTuple = tupleReaderWriter.getTupleFromMetadataEntity(dataset);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);

            if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                // Add the primary index for the dataset.
                InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
                Index primaryIndex = new Index(dataset.getDataverseName(), dataset.getDatasetName(),
                        dataset.getDatasetName(), IndexType.BTREE, id.getPrimaryKey(), id.getKeySourceIndicator(),
                        id.getPrimaryKeyType(), false, true, dataset.getPendingOp());

                addIndex(jobId, primaryIndex);
            }
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A dataset with this name " + dataset.getDatasetName()
                    + " already exists in dataverse '" + dataset.getDataverseName() + "'.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addIndex(JobId jobId, Index index) throws MetadataException, RemoteException {
        try {
            IndexTupleTranslator tupleWriter = tupleTranslatorProvider.getIndexTupleTranslator(jobId, this, true);
            ITupleReference tuple = tupleWriter.getTupleFromMetadataEntity(index);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.INDEX_DATASET, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("An index with name '" + index.getIndexName() + "' already exists.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addNode(JobId jobId, Node node) throws MetadataException, RemoteException {
        try {
            NodeTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getNodeTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(node);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.NODE_DATASET, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A node with name '" + node.getNodeName() + "' already exists.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addNodeGroup(JobId jobId, NodeGroup nodeGroup) throws MetadataException, RemoteException {
        try {
            NodeGroupTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getNodeGroupTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(nodeGroup);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.NODEGROUP_DATASET, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A nodegroup with name '" + nodeGroup.getNodeGroupName() + "' already exists.",
                    e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addDatatype(JobId jobId, Datatype datatype) throws MetadataException, RemoteException {
        try {
            DatatypeTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataTypeTupleTranslator(jobId, this,
                    true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(datatype);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A datatype with name '" + datatype.getDatatypeName() + "' already exists.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addFunction(JobId jobId, Function function) throws MetadataException, RemoteException {
        try {
            // Insert into the 'function' dataset.
            FunctionTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFunctionTupleTranslator(true);
            ITupleReference functionTuple = tupleReaderWriter.getTupleFromMetadataEntity(function);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET, functionTuple);

        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A function with this name " + function.getName() + " and arity "
                    + function.getArity() + " already exists in dataverse '" + function.getDataverseName() + "'.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    private void insertTupleIntoIndex(JobId jobId, IMetadataIndex metadataIndex, ITupleReference tuple)
            throws ACIDException, HyracksDataException, IndexException {
        long resourceID = metadataIndex.getResourceID();
        String resourceName = metadataIndex.getFile().getRelativePath();
        ILSMIndex lsmIndex = (ILSMIndex) datasetLifecycleManager.get(resourceName);
        try {
            datasetLifecycleManager.open(resourceName);

            // prepare a Callback for logging
            IModificationOperationCallback modCallback = createIndexModificationCallback(jobId, resourceID,
                    metadataIndex, lsmIndex, IndexOperation.INSERT);

            ILSMIndexAccessor indexAccessor = lsmIndex.createAccessor(modCallback, NoOpOperationCallback.INSTANCE);

            ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId,
                    false);
            txnCtx.setWriteTxn(true);
            txnCtx.registerIndexAndCallback(resourceID, lsmIndex, (AbstractOperationCallback) modCallback,
                    metadataIndex.isPrimaryIndex());

            LSMIndexUtil.checkAndSetFirstLSN((AbstractLSMIndex) lsmIndex, transactionSubsystem.getLogManager());

            // TODO: fix exceptions once new BTree exception model is in hyracks.
            indexAccessor.forceInsert(tuple);
        } finally {
            datasetLifecycleManager.close(resourceName);
        }
    }

    private IModificationOperationCallback createIndexModificationCallback(JobId jobId, long resourceId,
            IMetadataIndex metadataIndex, ILSMIndex lsmIndex, IndexOperation indexOp) throws ACIDException {
        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId, false);

        // Regardless of the index type (primary or secondary index), secondary index modification callback is given
        // This is still correct since metadata index operation doesn't require any lock from ConcurrentLockMgr and
        // The difference between primaryIndexModCallback and secondaryIndexModCallback is that primary index requires
        // locks and secondary index doesn't.
        return new SecondaryIndexModificationOperationCallback(metadataIndex.getDatasetId().getId(),
                metadataIndex.getPrimaryKeyIndexes(), txnCtx, transactionSubsystem.getLockManager(),
                transactionSubsystem, resourceId, metadataStoragePartition, ResourceType.LSM_BTREE, indexOp, false);
    }

    @Override
    public void dropDataverse(JobId jobId, String dataverseName) throws MetadataException, RemoteException {
        try {

            confirmDataverseCanBeDeleted(jobId, dataverseName);

            List<Dataset> dataverseDatasets;
            Dataset ds;
            dataverseDatasets = getDataverseDatasets(jobId, dataverseName);
            // Drop all datasets in this dataverse.
            for (int i = 0; i < dataverseDatasets.size(); i++) {
                ds = dataverseDatasets.get(i);
                dropDataset(jobId, dataverseName, ds.getDatasetName());
            }

            //After dropping datasets, drop datatypes
            List<Datatype> dataverseDatatypes;
            // As a side effect, acquires an S lock on the 'datatype' dataset
            // on behalf of txnId.
            dataverseDatatypes = getDataverseDatatypes(jobId, dataverseName);
            // Drop all types in this dataverse.
            for (int i = 0; i < dataverseDatatypes.size(); i++) {
                forceDropDatatype(jobId, dataverseName, dataverseDatatypes.get(i).getDatatypeName());
            }

            // As a side effect, acquires an S lock on the 'Function' dataset
            // on behalf of txnId.
            List<Function> dataverseFunctions = getDataverseFunctions(jobId, dataverseName);
            // Drop all functions in this dataverse.
            for (Function function : dataverseFunctions) {
                dropFunction(jobId, new FunctionSignature(dataverseName, function.getName(), function.getArity()));
            }

            // As a side effect, acquires an S lock on the 'Adapter' dataset
            // on behalf of txnId.
            List<DatasourceAdapter> dataverseAdapters = getDataverseAdapters(jobId, dataverseName);
            // Drop all functions in this dataverse.
            for (DatasourceAdapter adapter : dataverseAdapters) {
                dropAdapter(jobId, dataverseName, adapter.getAdapterIdentifier().getName());
            }

            List<Feed> dataverseFeeds;
            List<FeedConnection> feedConnections;
            Feed feed;
            dataverseFeeds = getDataverseFeeds(jobId, dataverseName);
            // Drop all feeds&connections in this dataverse.
            for (int i = 0; i < dataverseFeeds.size(); i++) {
                feed = dataverseFeeds.get(i);
                feedConnections = getFeedConnections(jobId, dataverseName, feed.getFeedName());
                for (FeedConnection feedConnection : feedConnections) {
                    dropFeedConnection(jobId, dataverseName, feed.getFeedName(), feedConnection.getDatasetName());
                }
                dropFeed(jobId, dataverseName, feed.getFeedName());
            }

            List<FeedPolicyEntity> feedPolicies = getDataversePolicies(jobId, dataverseName);
            if (feedPolicies != null && feedPolicies.size() > 0) {
                // Drop all feed ingestion policies in this dataverse.
                for (FeedPolicyEntity feedPolicy : feedPolicies) {
                    dropFeedPolicy(jobId, dataverseName, feedPolicy.getPolicyName());
                }
            }

            // Delete the dataverse entry from the 'dataverse' dataset.
            ITupleReference searchKey = createTuple(dataverseName);
            // As a side effect, acquires an S lock on the 'dataverse' dataset
            // on behalf of txnId.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATAVERSE_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATAVERSE_DATASET, tuple);

            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop dataverse '" + dataverseName + "' because it doesn't exist.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropDataset(JobId jobId, String dataverseName, String datasetName)
            throws MetadataException, RemoteException {
        Dataset dataset;
        try {
            dataset = getDataset(jobId, dataverseName, datasetName);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        if (dataset == null) {
            throw new MetadataException("Cannot drop dataset '" + datasetName + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'datasets' dataset.
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'dataset' dataset.
            ITupleReference datasetTuple = null;
            try {
                datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey);

                // Delete entry(s) from the 'indexes' dataset.
                List<Index> datasetIndexes = getDatasetIndexes(jobId, dataverseName, datasetName);
                if (datasetIndexes != null) {
                    for (Index index : datasetIndexes) {
                        dropIndex(jobId, dataverseName, datasetName, index.getIndexName());
                    }
                }

                if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
                    // Delete External Files
                    // As a side effect, acquires an S lock on the 'ExternalFile' dataset
                    // on behalf of txnId.
                    List<ExternalFile> datasetFiles = getExternalFiles(jobId, dataset);
                    if (datasetFiles != null && datasetFiles.size() > 0) {
                        // Drop all external files in this dataset.
                        for (ExternalFile file : datasetFiles) {
                            dropExternalFile(jobId, dataverseName, file.getDatasetName(), file.getFileNumber());
                        }
                    }
                }
            } catch (TreeIndexException tie) {
                // ignore this exception and continue deleting all relevant
                // artifacts.
            } finally {
                deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            }

        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropIndex(JobId jobId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName, indexName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'index' dataset.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.INDEX_DATASET, tuple);
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException(
                    "Cannot drop index '" + datasetName + "." + indexName + "' because it doesn't exist.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropNodegroup(JobId jobId, String nodeGroupName) throws MetadataException, RemoteException {
        List<String> datasetNames;
        try {
            datasetNames = getDatasetNamesPartitionedOnThisNodeGroup(jobId, nodeGroupName);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        if (!datasetNames.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Nodegroup '" + nodeGroupName
                    + "' cannot be dropped; it was used for partitioning these datasets:");
            for (int i = 0; i < datasetNames.size(); i++) {
                sb.append("\n" + (i + 1) + "- " + datasetNames.get(i) + ".");
            }
            throw new MetadataException(sb.toString());
        }
        try {
            ITupleReference searchKey = createTuple(nodeGroupName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'nodegroup' dataset.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.NODEGROUP_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.NODEGROUP_DATASET, tuple);
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop nodegroup '" + nodeGroupName + "' because it doesn't exist", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropDatatype(JobId jobId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {

        confirmDatatypeIsUnused(jobId, dataverseName, datatypeName);

        // Delete the datatype entry, including all it's nested anonymous types.
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'datatype' dataset.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey);
            // Get nested types
            List<String> nestedTypes = getNestedComplexDatatypeNamesForThisDatatype(jobId, dataverseName, datatypeName);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
            for (String nestedType : nestedTypes) {
                Datatype dt = getDatatype(jobId, dataverseName, nestedType);
                if (dt != null && dt.getIsAnonymous()) {
                    dropDatatype(jobId, dataverseName, dt.getDatatypeName());
                }
            }

            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop type '" + datatypeName + "' because it doesn't exist", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private void forceDropDatatype(JobId jobId, String dataverseName, String datatypeName) throws MetadataException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'datatype' dataset.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop type '" + datatypeName + "' because it doesn't exist", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    private void deleteTupleFromIndex(JobId jobId, IMetadataIndex metadataIndex, ITupleReference tuple)
            throws ACIDException, HyracksDataException, IndexException {
        long resourceID = metadataIndex.getResourceID();
        String resourceName = metadataIndex.getFile().getRelativePath();
        ILSMIndex lsmIndex = (ILSMIndex) datasetLifecycleManager.get(resourceName);
        try {
            datasetLifecycleManager.open(resourceName);
            // prepare a Callback for logging
            IModificationOperationCallback modCallback = createIndexModificationCallback(jobId, resourceID,
                    metadataIndex, lsmIndex, IndexOperation.DELETE);
            ILSMIndexAccessor indexAccessor = lsmIndex.createAccessor(modCallback, NoOpOperationCallback.INSTANCE);

            ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId,
                    false);
            txnCtx.setWriteTxn(true);
            txnCtx.registerIndexAndCallback(resourceID, lsmIndex, (AbstractOperationCallback) modCallback,
                    metadataIndex.isPrimaryIndex());

            LSMIndexUtil.checkAndSetFirstLSN((AbstractLSMIndex) lsmIndex, transactionSubsystem.getLogManager());

            indexAccessor.forceDelete(tuple);
        } finally {
            datasetLifecycleManager.close(resourceName);
        }
    }

    @Override
    public List<Dataverse> getDataverses(JobId jobId) throws MetadataException, RemoteException {
        try {
            DataverseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataverseTupleTranslator(false);
            IValueExtractor<Dataverse> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataverse> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATAVERSE_DATASET, null, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Dataverse getDataverse(JobId jobId, String dataverseName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DataverseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataverseTupleTranslator(false);
            IValueExtractor<Dataverse> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataverse> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATAVERSE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<Dataset> getDataverseDatasets(JobId jobId, String dataverseName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataset> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<Feed> getDataverseFeeds(JobId jobId, String dataverseName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(false);
            IValueExtractor<Feed> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Feed> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.FEED_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<Library> getDataverseLibraries(JobId jobId, String dataverseName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            LibraryTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getLibraryTupleTranslator(false);
            IValueExtractor<Library> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Library> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.LIBRARY_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    private List<Datatype> getDataverseDatatypes(JobId jobId, String dataverseName) throws MetadataException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatatypeTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataTypeTupleTranslator(jobId, this,
                    false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Dataset getDataset(JobId jobId, String dataverseName, String datasetName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
            List<Dataset> results = new ArrayList<>();
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    public List<Dataset> getAllDatasets(JobId jobId) throws MetadataException {
        try {
            ITupleReference searchKey = null;
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataset> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    public List<Datatype> getAllDatatypes(JobId jobId) throws MetadataException {
        try {
            ITupleReference searchKey = null;
            DatatypeTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataTypeTupleTranslator(jobId, this,
                    false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    private void confirmDataverseCanBeDeleted(JobId jobId, String dataverseName) throws MetadataException {
        //If a dataset from a DIFFERENT dataverse
        //uses a type from this dataverse
        //throw an error
        List<Dataset> datasets = getAllDatasets(jobId);
        for (Dataset set : datasets) {
            if (set.getDataverseName().equals(dataverseName)) {
                continue;
            }
            if (set.getItemTypeDataverseName().equals(dataverseName)) {
                throw new MetadataException("Cannot drop dataverse. Type " + dataverseName + "." + set.getItemTypeName()
                        + " used by dataset " + set.getDataverseName() + "." + set.getDatasetName());
            }
        }
    }

    private void confirmDatatypeIsUnused(JobId jobId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        confirmDatatypeIsUnusedByDatatypes(jobId, dataverseName, datatypeName);
        confirmDatatypeIsUnusedByDatasets(jobId, dataverseName, datatypeName);
    }

    private void confirmDatatypeIsUnusedByDatasets(JobId jobId, String dataverseName, String datatypeName)
            throws MetadataException {
        //If any dataset uses this type, throw an error
        List<Dataset> datasets = getAllDatasets(jobId);
        for (Dataset set : datasets) {
            if (set.getItemTypeName().equals(datatypeName) && set.getItemTypeDataverseName().equals(dataverseName)) {
                throw new MetadataException("Cannot drop type " + dataverseName + "." + datatypeName
                        + " being used by dataset " + set.getDataverseName() + "." + set.getDatasetName());
            }
        }
    }

    private void confirmDatatypeIsUnusedByDatatypes(JobId jobId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        //If any datatype uses this type, throw an error
        //TODO: Currently this loads all types into memory. This will need to be fixed for large numbers of types
        Datatype dataTypeToBeDropped = getDatatype(jobId, dataverseName, datatypeName);
        assert dataTypeToBeDropped != null;
        IAType typeToBeDropped = dataTypeToBeDropped.getDatatype();
        List<Datatype> datatypes = getAllDatatypes(jobId);
        for (Datatype dataType : datatypes) {
            //skip types in different dataverses as well as the type to be dropped itself
            if (!dataType.getDataverseName().equals(dataverseName)
                    || dataType.getDatatype().getTypeName().equals(datatypeName)) {
                continue;
            }

            AbstractComplexType recType = (AbstractComplexType) dataType.getDatatype();
            if (recType.containsType(typeToBeDropped)) {
                throw new MetadataException("Cannot drop type " + dataverseName + "." + datatypeName
                        + " being used by type " + dataverseName + "." + recType.getTypeName());
            }
        }
    }

    private List<String> getNestedComplexDatatypeNamesForThisDatatype(JobId jobId, String dataverseName,
            String datatypeName) throws MetadataException, RemoteException {
        //Return all field types that aren't builtin types
        Datatype parentType = getDatatype(jobId, dataverseName, datatypeName);

        List<IAType> subTypes = null;
        if (parentType.getDatatype().getTypeTag() == ATypeTag.RECORD) {
            ARecordType recType = (ARecordType) parentType.getDatatype();
            subTypes = Arrays.asList(recType.getFieldTypes());
        } else if (parentType.getDatatype().getTypeTag() == ATypeTag.UNION) {
            AUnionType recType = (AUnionType) parentType.getDatatype();
            subTypes = recType.getUnionList();
        }

        List<String> nestedTypes = new ArrayList<>();
        if (subTypes != null) {
            for (IAType subType : subTypes) {
                if (!(subType instanceof BuiltinType)) {
                    nestedTypes.add(subType.getTypeName());
                }
            }
        }
        return nestedTypes;
    }

    public List<String> getDatasetNamesPartitionedOnThisNodeGroup(JobId jobId, String nodegroup)
            throws MetadataException {
        //this needs to scan the datasets and return the datasets that use this nodegroup
        List<String> nodeGroupDatasets = new ArrayList<>();
        List<Dataset> datasets = getAllDatasets(jobId);
        for (Dataset set : datasets) {
            if (set.getNodeGroupName().equals(nodegroup)) {
                nodeGroupDatasets.add(set.getDatasetName());
            }
        }
        return nodeGroupDatasets;

    }

    @Override
    public Index getIndex(JobId jobId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName, indexName);
            IndexTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getIndexTupleTranslator(jobId, this,
                    false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Index> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<Index> getDatasetIndexes(JobId jobId, String dataverseName, String datasetName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            IndexTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getIndexTupleTranslator(jobId, this,
                    false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Index> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Datatype getDatatype(JobId jobId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            DatatypeTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataTypeTupleTranslator(jobId, this,
                    false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public NodeGroup getNodeGroup(JobId jobId, String nodeGroupName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(nodeGroupName);
            NodeGroupTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getNodeGroupTupleTranslator(false);
            IValueExtractor<NodeGroup> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<NodeGroup> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.NODEGROUP_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Function getFunction(JobId jobId, FunctionSignature functionSignature)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(functionSignature.getNamespace(), functionSignature.getName(),
                    "" + functionSignature.getArity());
            FunctionTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFunctionTupleTranslator(false);
            List<Function> results = new ArrayList<>();
            IValueExtractor<Function> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropFunction(JobId jobId, FunctionSignature functionSignature)
            throws MetadataException, RemoteException {

        Function function = getFunction(jobId, functionSignature);

        if (function == null) {
            throw new MetadataException(
                    "Cannot drop function '" + functionSignature.toString() + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'function' dataset.
            ITupleReference searchKey = createTuple(functionSignature.getNamespace(), functionSignature.getName(),
                    "" + functionSignature.getArity());
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'function' dataset.
            ITupleReference functionTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET,
                    searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET, functionTuple);

            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("There is no function with the name " + functionSignature.getName()
                    + " and arity " + functionSignature.getArity(), e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    private ITupleReference getTupleToBeDeleted(JobId jobId, IMetadataIndex metadataIndex, ITupleReference searchKey)
            throws MetadataException, IndexException, IOException {
        IValueExtractor<ITupleReference> valueExtractor = new TupleCopyValueExtractor(metadataIndex.getTypeTraits());
        List<ITupleReference> results = new ArrayList<>();
        searchIndex(jobId, metadataIndex, searchKey, valueExtractor, results);
        if (results.isEmpty()) {
            // TODO: Temporarily a TreeIndexException to make it get caught by
            // caller in the appropriate catch block.
            throw new TreeIndexException("Could not find entry to be deleted.");
        }
        // There should be exactly one result returned from the search.
        return results.get(0);
    }

    // Debugging Method
    public String printMetadata() {

        StringBuilder sb = new StringBuilder();
        try {
            IMetadataIndex index = MetadataPrimaryIndexes.DATAVERSE_DATASET;
            String resourceName = index.getFile().toString();
            IIndex indexInstance = datasetLifecycleManager.get(resourceName);
            datasetLifecycleManager.open(resourceName);
            IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            ITreeIndexCursor rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor(false);

            RangePredicate rangePred = null;
            rangePred = new RangePredicate(null, null, true, true, null, null);
            indexAccessor.search(rangeCursor, rangePred);
            try {
                while (rangeCursor.hasNext()) {
                    rangeCursor.next();
                    sb.append(TupleUtils.printTuple(rangeCursor.getTuple(),
                            new ISerializerDeserializer[] { SerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.ASTRING) }));
                }
            } finally {
                rangeCursor.close();
            }
            datasetLifecycleManager.close(resourceName);

            index = MetadataPrimaryIndexes.DATASET_DATASET;
            indexInstance = datasetLifecycleManager.get(resourceName);
            datasetLifecycleManager.open(resourceName);
            indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor(false);

            rangePred = null;
            rangePred = new RangePredicate(null, null, true, true, null, null);
            indexAccessor.search(rangeCursor, rangePred);
            try {
                while (rangeCursor.hasNext()) {
                    rangeCursor.next();
                    sb.append(TupleUtils.printTuple(rangeCursor.getTuple(),
                            new ISerializerDeserializer[] {
                                    SerializerDeserializerProvider.INSTANCE
                                            .getSerializerDeserializer(BuiltinType.ASTRING),
                                    SerializerDeserializerProvider.INSTANCE
                                            .getSerializerDeserializer(BuiltinType.ASTRING) }));
                }
            } finally {
                rangeCursor.close();
            }
            datasetLifecycleManager.close(resourceName);

            index = MetadataPrimaryIndexes.INDEX_DATASET;
            indexInstance = datasetLifecycleManager.get(resourceName);
            datasetLifecycleManager.open(resourceName);
            indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor(false);

            rangePred = null;
            rangePred = new RangePredicate(null, null, true, true, null, null);
            indexAccessor.search(rangeCursor, rangePred);
            try {
                while (rangeCursor.hasNext()) {
                    rangeCursor.next();
                    sb.append(TupleUtils.printTuple(rangeCursor.getTuple(), new ISerializerDeserializer[] {
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING),
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING),
                            SerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.ASTRING) }));
                }
            } finally {
                rangeCursor.close();
            }
            datasetLifecycleManager.close(resourceName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    private <ResultType> void searchIndex(JobId jobId, IMetadataIndex index, ITupleReference searchKey,
            IValueExtractor<ResultType> valueExtractor, List<ResultType> results)
            throws MetadataException, IndexException, IOException {
        IBinaryComparatorFactory[] comparatorFactories = index.getKeyBinaryComparatorFactory();
        if (index.getFile() == null) {
            throw new MetadataException("No file for Index " + index.getDataverseName() + "." + index.getIndexName());
        }
        String resourceName = index.getFile().getRelativePath();
        IIndex indexInstance = datasetLifecycleManager.get(resourceName);
        datasetLifecycleManager.open(resourceName);
        IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        ITreeIndexCursor rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor(false);

        IBinaryComparator[] searchCmps = null;
        MultiComparator searchCmp = null;
        RangePredicate rangePred = null;
        if (searchKey != null) {
            searchCmps = new IBinaryComparator[searchKey.getFieldCount()];
            for (int i = 0; i < searchKey.getFieldCount(); i++) {
                searchCmps[i] = comparatorFactories[i].createBinaryComparator();
            }
            searchCmp = new MultiComparator(searchCmps);
        }
        rangePred = new RangePredicate(searchKey, searchKey, true, true, searchCmp, searchCmp);
        indexAccessor.search(rangeCursor, rangePred);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ResultType result = valueExtractor.getValue(jobId, rangeCursor.getTuple());
                if (result != null) {
                    results.add(result);
                }
            }
        } finally {
            rangeCursor.close();
        }
        datasetLifecycleManager.close(resourceName);
    }

    @Override
    public void initializeDatasetIdFactory(JobId jobId) throws MetadataException, RemoteException {
        int mostRecentDatasetId = MetadataIndexImmutableProperties.FIRST_AVAILABLE_USER_DATASET_ID;
        try {
            String resourceName = MetadataPrimaryIndexes.DATASET_DATASET.getFile().getRelativePath();
            IIndex indexInstance = datasetLifecycleManager.get(resourceName);
            datasetLifecycleManager.open(resourceName);
            try {
                IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
                IIndexCursor rangeCursor = indexAccessor.createSearchCursor(false);

                DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
                IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
                RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);

                indexAccessor.search(rangeCursor, rangePred);
                int datasetId;

                try {
                    while (rangeCursor.hasNext()) {
                        rangeCursor.next();
                        final ITupleReference ref = rangeCursor.getTuple();
                        final Dataset ds = valueExtractor.getValue(jobId, ref);
                        datasetId = ds.getDatasetId();
                        if (mostRecentDatasetId < datasetId) {
                            mostRecentDatasetId = datasetId;
                        }
                    }
                } finally {
                    rangeCursor.close();
                }
            } finally {
                datasetLifecycleManager.close(resourceName);
            }

        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }

        DatasetIdFactory.initialize(mostRecentDatasetId);
    }

    // TODO: Can use Hyrack's TupleUtils for this, once we switch to a newer
    // Hyracks version.
    public static ITupleReference createTuple(String... fields) {
        @SuppressWarnings("unchecked")
        ISerializerDeserializer<AString> stringSerde = SerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);
        AMutableString aString = new AMutableString("");
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fields.length);
        for (String s : fields) {
            aString.setValue(s);
            try {
                stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            } catch (HyracksDataException e) {
                // This should never happen
                throw new IllegalStateException("Failed to create search tuple!!!! This should never happen", e);
            }
            tupleBuilder.addFieldEndOffset();
        }
        ArrayTupleReference tuple = new ArrayTupleReference();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    @Override
    public List<Function> getDataverseFunctions(JobId jobId, String dataverseName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            FunctionTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFunctionTupleTranslator(false);
            IValueExtractor<Function> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Function> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addAdapter(JobId jobId, DatasourceAdapter adapter) throws MetadataException, RemoteException {
        try {
            // Insert into the 'Adapter' dataset.
            DatasourceAdapterTupleTranslator tupleReaderWriter = tupleTranslatorProvider
                    .getAdapterTupleTranslator(true);
            ITupleReference adapterTuple = tupleReaderWriter.getTupleFromMetadataEntity(adapter);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, adapterTuple);

        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException(
                    "A adapter with this name " + adapter.getAdapterIdentifier().getName()
                            + " already exists in dataverse '" + adapter.getAdapterIdentifier().getNamespace() + "'.",
                    e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropAdapter(JobId jobId, String dataverseName, String adapterName)
            throws MetadataException, RemoteException {
        DatasourceAdapter adapter = getAdapter(jobId, dataverseName, adapterName);
        if (adapter == null) {
            throw new MetadataException("Cannot drop adapter '" + adapter + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'Adapter' dataset.
            ITupleReference searchKey = createTuple(dataverseName, adapterName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'Adapter' dataset.
            ITupleReference datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET,
                    searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, datasetTuple);

            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop adapter '" + adapterName, e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public DatasourceAdapter getAdapter(JobId jobId, String dataverseName, String adapterName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, adapterName);
            DatasourceAdapterTupleTranslator tupleReaderWriter = tupleTranslatorProvider
                    .getAdapterTupleTranslator(false);
            List<DatasourceAdapter> results = new ArrayList<>();
            IValueExtractor<DatasourceAdapter> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addCompactionPolicy(JobId jobId, CompactionPolicy compactionPolicy)
            throws MetadataException, RemoteException {
        try {
            // Insert into the 'CompactionPolicy' dataset.
            CompactionPolicyTupleTranslator tupleReaderWriter = tupleTranslatorProvider
                    .getCompactionPolicyTupleTranslator(true);
            ITupleReference compactionPolicyTuple = tupleReaderWriter.getTupleFromMetadataEntity(compactionPolicy);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.COMPACTION_POLICY_DATASET, compactionPolicyTuple);

        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A compcation policy with this name " + compactionPolicy.getPolicyName()
                    + " already exists in dataverse '" + compactionPolicy.getPolicyName() + "'.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public CompactionPolicy getCompactionPolicy(JobId jobId, String dataverse, String policyName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverse, policyName);
            CompactionPolicyTupleTranslator tupleReaderWriter = tupleTranslatorProvider
                    .getCompactionPolicyTupleTranslator(false);
            List<CompactionPolicy> results = new ArrayList<>();
            IValueExtractor<CompactionPolicy> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.COMPACTION_POLICY_DATASET, searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<DatasourceAdapter> getDataverseAdapters(JobId jobId, String dataverseName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatasourceAdapterTupleTranslator tupleReaderWriter = tupleTranslatorProvider
                    .getAdapterTupleTranslator(false);
            IValueExtractor<DatasourceAdapter> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<DatasourceAdapter> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addLibrary(JobId jobId, Library library) throws MetadataException, RemoteException {
        try {
            // Insert into the 'Library' dataset.
            LibraryTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getLibraryTupleTranslator(true);
            ITupleReference libraryTuple = tupleReaderWriter.getTupleFromMetadataEntity(library);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.LIBRARY_DATASET, libraryTuple);

        } catch (TreeIndexException e) {
            throw new MetadataException("A library with this name " + library.getDataverseName()
                    + " already exists in dataverse '" + library.getDataverseName() + "'.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropLibrary(JobId jobId, String dataverseName, String libraryName)
            throws MetadataException, RemoteException {
        Library library = getLibrary(jobId, dataverseName, libraryName);
        if (library == null) {
            throw new MetadataException("Cannot drop library '" + library + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'Library' dataset.
            ITupleReference searchKey = createTuple(dataverseName, libraryName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'Adapter' dataset.
            ITupleReference datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.LIBRARY_DATASET,
                    searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.LIBRARY_DATASET, datasetTuple);

            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop library '" + libraryName, e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public Library getLibrary(JobId jobId, String dataverseName, String libraryName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, libraryName);
            LibraryTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getLibraryTupleTranslator(false);
            List<Library> results = new ArrayList<>();
            IValueExtractor<Library> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.LIBRARY_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public int getMostRecentDatasetId() throws MetadataException, RemoteException {
        return DatasetIdFactory.getMostRecentDatasetId();
    }

    @Override
    public void addFeedPolicy(JobId jobId, FeedPolicyEntity feedPolicy) throws MetadataException, RemoteException {
        try {
            // Insert into the 'FeedPolicy' dataset.
            FeedPolicyTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedPolicyTupleTranslator(true);
            ITupleReference feedPolicyTuple = tupleReaderWriter.getTupleFromMetadataEntity(feedPolicy);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, feedPolicyTuple);

        } catch (TreeIndexException e) {
            throw new MetadataException("A feed policy with this name " + feedPolicy.getPolicyName()
                    + " already exists in dataverse '" + feedPolicy.getPolicyName() + "'.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public FeedPolicyEntity getFeedPolicy(JobId jobId, String dataverse, String policyName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverse, policyName);
            FeedPolicyTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedPolicyTupleTranslator(false);
            List<FeedPolicyEntity> results = new ArrayList<>();
            IValueExtractor<FeedPolicyEntity> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addFeedConnection(JobId jobId, FeedConnection feedConnection) throws MetadataException {
        try {
            FeedConnectionTupleTranslator tupleReaderWriter = new FeedConnectionTupleTranslator(true);
            ITupleReference feedConnTuple = tupleReaderWriter.getTupleFromMetadataEntity(feedConnection);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET, feedConnTuple);
        } catch (IndexException | ACIDException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<FeedConnection> getFeedConnections(JobId jobId, String dataverseName, String feedName)
            throws MetadataException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, feedName);
            FeedConnectionTupleTranslator tupleReaderWriter = new FeedConnectionTupleTranslator(false);
            List<FeedConnection> results = new ArrayList<>();
            IValueExtractor<FeedConnection> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public FeedConnection getFeedConnection(JobId jobId, String dataverseName, String feedName, String datasetName)
            throws MetadataException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, feedName, datasetName);
            FeedConnectionTupleTranslator tupleReaderWriter = new FeedConnectionTupleTranslator(false);
            List<FeedConnection> results = new ArrayList<>();
            IValueExtractor<FeedConnection> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET, searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropFeedConnection(JobId jobId, String dataverseName, String feedName, String datasetName)
            throws MetadataException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, feedName, datasetName);
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET,
                    searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET, tuple);
        } catch (IndexException | IOException | ACIDException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addFeed(JobId jobId, Feed feed) throws MetadataException, RemoteException {
        try {
            // Insert into the 'Feed' dataset.
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(true);
            ITupleReference feedTuple = tupleReaderWriter.getTupleFromMetadataEntity(feed);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.FEED_DATASET, feedTuple);

        } catch (TreeIndexException e) {
            throw new MetadataException("A feed with this name " + feed.getFeedName() + " already exists in dataverse '"
                    + feed.getDataverseName() + "'.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Feed getFeed(JobId jobId, String dataverse, String feedName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverse, feedName);
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(false);
            List<Feed> results = new ArrayList<>();
            IValueExtractor<Feed> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.FEED_DATASET, searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropFeed(JobId jobId, String dataverse, String feedName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverse, feedName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'nodegroup' dataset.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.FEED_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.FEED_DATASET, tuple);
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop feed '" + feedName + "' because it doesn't exist", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropFeedPolicy(JobId jobId, String dataverseName, String policyName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, policyName);
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, tuple);
        } catch (TreeIndexException e) {
            throw new MetadataException("Unknown feed policy " + policyName, e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<FeedPolicyEntity> getDataversePolicies(JobId jobId, String dataverse)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverse);
            FeedPolicyTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedPolicyTupleTranslator(false);
            IValueExtractor<FeedPolicyEntity> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<FeedPolicyEntity> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addExternalFile(JobId jobId, ExternalFile externalFile) throws MetadataException, RemoteException {
        try {
            // Insert into the 'externalFiles' dataset.
            ExternalFileTupleTranslator tupleReaderWriter = tupleTranslatorProvider
                    .getExternalFileTupleTranslator(true);
            ITupleReference externalFileTuple = tupleReaderWriter.getTupleFromMetadataEntity(externalFile);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, externalFileTuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("An externalFile with this number " + externalFile.getFileNumber()
                    + " already exists in dataset '" + externalFile.getDatasetName() + "' in dataverse '"
                    + externalFile.getDataverseName() + "'.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<ExternalFile> getExternalFiles(JobId jobId, Dataset dataset) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataset.getDataverseName(), dataset.getDatasetName());
            ExternalFileTupleTranslator tupleReaderWriter = tupleTranslatorProvider
                    .getExternalFileTupleTranslator(false);
            IValueExtractor<ExternalFile> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<ExternalFile> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropExternalFile(JobId jobId, String dataverseName, String datasetName, int fileNumber)
            throws MetadataException, RemoteException {
        try {
            // Delete entry from the 'ExternalFile' dataset.
            ITupleReference searchKey = createExternalFileSearchTuple(dataverseName, datasetName, fileNumber);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'ExternalFile' dataset.
            ITupleReference datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET,
                    searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, datasetTuple);
        } catch (TreeIndexException e) {
            throw new MetadataException("Couldn't drop externalFile.", e);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropExternalFiles(JobId jobId, Dataset dataset) throws MetadataException, RemoteException {
        List<ExternalFile> files = getExternalFiles(jobId, dataset);
        //loop through files and delete them
        for (int i = 0; i < files.size(); i++) {
            dropExternalFile(jobId, files.get(i).getDataverseName(), files.get(i).getDatasetName(),
                    files.get(i).getFileNumber());
        }
    }

    // This method is used to create a search tuple for external data file since the search tuple has an int value
    @SuppressWarnings("unchecked")
    public ITupleReference createExternalFileSearchTuple(String dataverseName, String datasetName, int fileNumber)
            throws HyracksDataException {
        ISerializerDeserializer<AString> stringSerde = SerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);
        ISerializerDeserializer<AInt32> intSerde = SerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.AINT32);

        AMutableString aString = new AMutableString("");
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);

        //dataverse field
        aString.setValue(dataverseName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        //dataset field
        aString.setValue(datasetName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        //file number field
        intSerde.serialize(new AInt32(fileNumber), tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        ArrayTupleReference tuple = new ArrayTupleReference();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    @Override
    public ExternalFile getExternalFile(JobId jobId, String dataverseName, String datasetName, Integer fileNumber)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createExternalFileSearchTuple(dataverseName, datasetName, fileNumber);
            ExternalFileTupleTranslator tupleReaderWriter = tupleTranslatorProvider
                    .getExternalFileTupleTranslator(false);
            IValueExtractor<ExternalFile> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<ExternalFile> results = new ArrayList<>();
            searchIndex(jobId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void updateDataset(JobId jobId, Dataset dataset) throws MetadataException, RemoteException {
        try {
            // This method will delete previous entry of the dataset and insert the new one
            // Delete entry from the 'datasets' dataset.
            ITupleReference searchKey;
            searchKey = createTuple(dataset.getDataverseName(), dataset.getDatasetName());
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'dataset' dataset.
            ITupleReference datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATASET_DATASET,
                    searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            // Previous tuple was deleted
            // Insert into the 'dataset' dataset.
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(true);
            datasetTuple = tupleReaderWriter.getTupleFromMetadataEntity(dataset);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
        } catch (ACIDException | IndexException | IOException e) {
            throw new MetadataException(e);
        }
    }
}
