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
package org.apache.asterix.metadata.utils;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.IPropertiesProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.transactions.IResourceFactory;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.asterix.runtime.formats.FormatUtils;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.asterix.transaction.management.resource.LSMInvertedIndexLocalResourceMetadataFactory;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexCompactOperator;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import org.apache.hyracks.storage.common.file.LocalResource;

public class SecondaryInvertedIndexOperationsHelper extends SecondaryIndexOperationsHelper {

    private IAType secondaryKeyType;
    private ITypeTraits[] invListsTypeTraits;
    private IBinaryComparatorFactory[] tokenComparatorFactories;
    private ITypeTraits[] tokenTypeTraits;
    private IBinaryTokenizerFactory tokenizerFactory;
    // For tokenization, sorting and loading. Represents <token, primary keys>.
    private int numTokenKeyPairFields;
    private IBinaryComparatorFactory[] tokenKeyPairComparatorFactories;
    private RecordDescriptor tokenKeyPairRecDesc;
    private boolean isPartitioned;
    private int[] invertedIndexFields;
    private int[] invertedIndexFieldsForNonBulkLoadOps;
    private int[] secondaryFilterFieldsForNonBulkLoadOps;

    protected SecondaryInvertedIndexOperationsHelper(Dataset dataset, Index index,
            PhysicalOptimizationConfig physOptConf, IPropertiesProvider propertiesProvider,
            MetadataProvider metadataProvider, ARecordType recType, ARecordType metaType, ARecordType enforcedType,
            ARecordType enforcedMetaType) {
        super(dataset, index, physOptConf, propertiesProvider, metadataProvider, recType, metaType, enforcedType,
                enforcedMetaType);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void setSecondaryRecDescAndComparators() throws AlgebricksException {
        int numSecondaryKeys = index.getKeyFieldNames().size();
        IndexType indexType = index.getIndexType();
        boolean isEnforcingKeyTypes = index.isEnforcingKeyFileds();
        // Sanity checks.
        if (numPrimaryKeys > 1) {
            throw new CompilationException(
                    ErrorCode.COMPILATION_ILLEGAL_INDEX_FOR_DATASET_WITH_COMPOSITE_PRIMARY_INDEX, indexType,
                    RecordUtil.toFullyQualifiedName(dataset.getDataverseName(), dataset.getDatasetName()));
        }
        if (numSecondaryKeys > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD, numSecondaryKeys,
                    indexType, 1);
        }
        if (indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
            isPartitioned = true;
        } else {
            isPartitioned = false;
        }
        // Prepare record descriptor used in the assign op, and the optional
        // select op.
        secondaryFieldAccessEvalFactories = new IScalarEvaluatorFactory[numSecondaryKeys + numFilterFields];
        ISerializerDeserializer[] secondaryRecFields =
                new ISerializerDeserializer[numPrimaryKeys + numSecondaryKeys + numFilterFields];
        ISerializerDeserializer[] enforcedRecFields =
                new ISerializerDeserializer[1 + numPrimaryKeys + numFilterFields];
        secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        ITypeTraits[] enforcedTypeTraits = new ITypeTraits[1 + numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = FormatUtils.getDefaultFormat().getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = FormatUtils.getDefaultFormat().getTypeTraitProvider();
        if (numSecondaryKeys > 0) {
            secondaryFieldAccessEvalFactories[0] = FormatUtils.getDefaultFormat().getFieldAccessEvaluatorFactory(
                    isEnforcingKeyTypes ? enforcedItemType : itemType, index.getKeyFieldNames().get(0),
                    numPrimaryKeys);
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0),
                    index.getKeyFieldNames().get(0), itemType);
            secondaryKeyType = keyTypePair.first;
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(secondaryKeyType);
            secondaryRecFields[0] = keySerde;
            secondaryTypeTraits[0] = typeTraitProvider.getTypeTrait(secondaryKeyType);
        }
        if (numFilterFields > 0) {
            secondaryFieldAccessEvalFactories[numSecondaryKeys] = FormatUtils.getDefaultFormat()
                    .getFieldAccessEvaluatorFactory(itemType, filterFieldName, numPrimaryKeys);
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableKeyFieldType(filterFieldName, itemType);
            IAType type = keyTypePair.first;
            ISerializerDeserializer serde = serdeProvider.getSerializerDeserializer(type);
            secondaryRecFields[numPrimaryKeys + numSecondaryKeys] = serde;
        }
        secondaryRecDesc = new RecordDescriptor(secondaryRecFields);
        // Comparators and type traits for tokens.
        int numTokenFields = (!isPartitioned) ? numSecondaryKeys : numSecondaryKeys + 1;
        tokenComparatorFactories = new IBinaryComparatorFactory[numTokenFields];
        tokenTypeTraits = new ITypeTraits[numTokenFields];
        tokenComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
        tokenTypeTraits[0] = NonTaggedFormatUtil.getTokenTypeTrait(secondaryKeyType);
        if (isPartitioned) {
            // The partitioning field is hardcoded to be a short *without* an Asterix type tag.
            tokenComparatorFactories[1] = PointableBinaryComparatorFactory.of(ShortPointable.FACTORY);
            tokenTypeTraits[1] = ShortPointable.TYPE_TRAITS;
        }
        // Set tokenizer factory.
        // TODO: We might want to expose the hashing option at the AQL level,
        // and add the choice to the index metadata.
        tokenizerFactory = NonTaggedFormatUtil.getBinaryTokenizerFactory(secondaryKeyType.getTypeTag(), indexType,
                index.getGramLength());
        // Type traits for inverted-list elements. Inverted lists contain
        // primary keys.
        invListsTypeTraits = new ITypeTraits[numPrimaryKeys];
        if (numPrimaryKeys > 0) {
            invListsTypeTraits[0] = primaryRecDesc.getTypeTraits()[0];
            enforcedRecFields[0] = primaryRecDesc.getFields()[0];
            enforcedTypeTraits[0] = primaryRecDesc.getTypeTraits()[0];
        }
        enforcedRecFields[numPrimaryKeys] = serdeProvider.getSerializerDeserializer(itemType);
        enforcedRecDesc = new RecordDescriptor(enforcedRecFields, enforcedTypeTraits);
        // For tokenization, sorting and loading.
        // One token (+ optional partitioning field) + primary keys.
        numTokenKeyPairFields = (!isPartitioned) ? 1 + numPrimaryKeys : 2 + numPrimaryKeys;
        ISerializerDeserializer[] tokenKeyPairFields =
                new ISerializerDeserializer[numTokenKeyPairFields + numFilterFields];
        ITypeTraits[] tokenKeyPairTypeTraits = new ITypeTraits[numTokenKeyPairFields];
        tokenKeyPairComparatorFactories = new IBinaryComparatorFactory[numTokenKeyPairFields];
        tokenKeyPairFields[0] = serdeProvider.getSerializerDeserializer(secondaryKeyType);
        tokenKeyPairTypeTraits[0] = tokenTypeTraits[0];
        tokenKeyPairComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
        int pkOff = 1;
        if (isPartitioned) {
            tokenKeyPairFields[1] = ShortSerializerDeserializer.INSTANCE;
            tokenKeyPairTypeTraits[1] = tokenTypeTraits[1];
            tokenKeyPairComparatorFactories[1] = PointableBinaryComparatorFactory.of(ShortPointable.FACTORY);
            pkOff = 2;
        }
        if (numPrimaryKeys > 0) {
            tokenKeyPairFields[pkOff] = primaryRecDesc.getFields()[0];
            tokenKeyPairTypeTraits[pkOff] = primaryRecDesc.getTypeTraits()[0];
            tokenKeyPairComparatorFactories[pkOff] = primaryComparatorFactories[0];
        }
        if (numFilterFields > 0) {
            tokenKeyPairFields[numPrimaryKeys + pkOff] = secondaryRecFields[numPrimaryKeys + numSecondaryKeys];
        }
        tokenKeyPairRecDesc = new RecordDescriptor(tokenKeyPairFields, tokenKeyPairTypeTraits);
        if (filterFieldName != null) {
            invertedIndexFields = new int[numTokenKeyPairFields];
            for (int i = 0; i < invertedIndexFields.length; i++) {
                invertedIndexFields[i] = i;
            }
            secondaryFilterFieldsForNonBulkLoadOps = new int[numFilterFields];
            secondaryFilterFieldsForNonBulkLoadOps[0] = numSecondaryKeys + numPrimaryKeys;
            invertedIndexFieldsForNonBulkLoadOps = new int[numSecondaryKeys + numPrimaryKeys];
            for (int i = 0; i < invertedIndexFieldsForNonBulkLoadOps.length; i++) {
                invertedIndexFieldsForNonBulkLoadOps[i] = i;
            }
        }
    }

    @Override
    protected int getNumSecondaryKeys() {
        return numTokenKeyPairFields - numPrimaryKeys;
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification();
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        //prepare a LocalResourceMetadata which will be stored in NC's local resource repository
        IResourceFactory localResourceMetadata = new LSMInvertedIndexLocalResourceMetadataFactory(invListsTypeTraits,
                primaryComparatorFactories, tokenTypeTraits, tokenComparatorFactories, tokenizerFactory, isPartitioned,
                dataset.getDatasetId(), mergePolicyFactory, mergePolicyFactoryProperties, filterTypeTraits,
                filterCmpFactories, invertedIndexFields, secondaryFilterFields, secondaryFilterFieldsForNonBulkLoadOps,
                invertedIndexFieldsForNonBulkLoadOps, dataset.getIndexOperationTrackerFactory(index),
                dataset.getIoOperationCallbackFactory(index),
                storageComponentProvider.getMetadataPageManagerFactory());
        ILocalResourceFactoryProvider localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(
                localResourceMetadata, LocalResource.LSMInvertedIndexResource);

        IIndexDataflowHelperFactory dataflowHelperFactory = createDataflowHelperFactory();
        LSMInvertedIndexCreateOperatorDescriptor invIndexCreateOp =
                new LSMInvertedIndexCreateOperatorDescriptor(spec, storageComponentProvider.getStorageManager(),
                        secondaryFileSplitProvider, storageComponentProvider.getIndexLifecycleManagerProvider(),
                        tokenTypeTraits, tokenComparatorFactories, invListsTypeTraits, primaryComparatorFactories,
                        tokenizerFactory, dataflowHelperFactory, localResourceFactoryProvider,
                        dataset.getModificationCallbackFactory(storageComponentProvider, index, null,
                                IndexOperation.CREATE, null),
                        storageComponentProvider.getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, invIndexCreateOp,
                secondaryPartitionConstraint);
        spec.addRoot(invIndexCreateOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification();

        // Create dummy key provider for feeding the primary index scan.
        AbstractOperatorDescriptor keyProviderOp = createDummyKeyProviderOp(spec);

        // Create primary index scan op.
        BTreeSearchOperatorDescriptor primaryScanOp = createPrimaryIndexScanOp(spec);

        AbstractOperatorDescriptor sourceOp = primaryScanOp;
        boolean isEnforcingKeyTypes = index.isEnforcingKeyFileds();
        int numSecondaryKeys = index.getKeyFieldNames().size();
        if (isEnforcingKeyTypes && !enforcedItemType.equals(itemType)) {
            sourceOp = createCastOp(spec, dataset.getDatasetType());
            spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, sourceOp, 0);
        }
        AlgebricksMetaOperatorDescriptor asterixAssignOp = createAssignOp(spec, numSecondaryKeys, secondaryRecDesc);

        // If any of the secondary fields are nullable, then add a select op
        // that filters nulls.
        AlgebricksMetaOperatorDescriptor selectOp = null;
        if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
            selectOp = createFilterNullsSelectOp(spec, numSecondaryKeys, secondaryRecDesc);
        }

        // Create a tokenizer op.
        AbstractOperatorDescriptor tokenizerOp = createTokenizerOp(spec);

        // Sort by token + primary keys.
        ExternalSortOperatorDescriptor sortOp =
                createSortOp(spec, tokenKeyPairComparatorFactories, tokenKeyPairRecDesc);

        // Create secondary inverted index bulk load op.
        LSMInvertedIndexBulkLoadOperatorDescriptor invIndexBulkLoadOp = createInvertedIndexBulkLoadOp(spec);

        AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { new SinkRuntimeFactory() }, new RecordDescriptor[] {});
        // Connect the operators.
        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, asterixAssignOp, 0);
        if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
            spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, tokenizerOp, 0);
        } else {
            spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, tokenizerOp, 0);
        }
        spec.connect(new OneToOneConnectorDescriptor(spec), tokenizerOp, 0, sortOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, invIndexBulkLoadOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), invIndexBulkLoadOp, 0, metaOp, 0);
        spec.addRoot(metaOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    private AbstractOperatorDescriptor createTokenizerOp(JobSpecification spec) throws AlgebricksException {
        int docField = 0;
        int numSecondaryKeys = index.getKeyFieldNames().size();
        int[] primaryKeyFields = new int[numPrimaryKeys + numFilterFields];
        for (int i = 0; i < primaryKeyFields.length; i++) {
            primaryKeyFields[i] = numSecondaryKeys + i;
        }
        BinaryTokenizerOperatorDescriptor tokenizerOp = new BinaryTokenizerOperatorDescriptor(spec,
                tokenKeyPairRecDesc, tokenizerFactory, docField, primaryKeyFields, isPartitioned, false);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, tokenizerOp,
                primaryPartitionConstraint);
        return tokenizerOp;
    }

    @Override
    protected ExternalSortOperatorDescriptor createSortOp(JobSpecification spec,
            IBinaryComparatorFactory[] secondaryComparatorFactories, RecordDescriptor secondaryRecDesc) {
        // Sort on token and primary keys.
        int[] sortFields = new int[numTokenKeyPairFields];
        for (int i = 0; i < numTokenKeyPairFields; i++) {
            sortFields[i] = i;
        }
        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec,
                physOptConf.getMaxFramesExternalSort(), sortFields, tokenKeyPairComparatorFactories, secondaryRecDesc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp, primaryPartitionConstraint);
        return sortOp;
    }

    private LSMInvertedIndexBulkLoadOperatorDescriptor createInvertedIndexBulkLoadOp(JobSpecification spec)
            throws AlgebricksException {
        int[] fieldPermutation = new int[numTokenKeyPairFields + numFilterFields];
        for (int i = 0; i < fieldPermutation.length; i++) {
            fieldPermutation[i] = i;
        }
        IIndexDataflowHelperFactory dataflowHelperFactory = createDataflowHelperFactory();
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        LSMInvertedIndexBulkLoadOperatorDescriptor invIndexBulkLoadOp = new LSMInvertedIndexBulkLoadOperatorDescriptor(
                spec, secondaryRecDesc, fieldPermutation, false, numElementsHint, false,
                storageComponentProvider.getStorageManager(), secondaryFileSplitProvider,
                storageComponentProvider.getIndexLifecycleManagerProvider(), tokenTypeTraits, tokenComparatorFactories,
                invListsTypeTraits, primaryComparatorFactories, tokenizerFactory, dataflowHelperFactory,
                storageComponentProvider.getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, invIndexBulkLoadOp,
                secondaryPartitionConstraint);
        return invIndexBulkLoadOp;
    }

    private IIndexDataflowHelperFactory createDataflowHelperFactory() throws AlgebricksException {
        return dataset.getIndexDataflowHelperFactory(metadataProvider, index, itemType, metaType, mergePolicyFactory,
                mergePolicyFactoryProperties);
    }

    @Override
    public JobSpecification buildCompactJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification();
        IIndexDataflowHelperFactory dataflowHelperFactory = createDataflowHelperFactory();
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        LSMInvertedIndexCompactOperator compactOp =
                new LSMInvertedIndexCompactOperator(spec, storageComponentProvider.getStorageManager(),
                        secondaryFileSplitProvider, storageComponentProvider.getIndexLifecycleManagerProvider(),
                        tokenTypeTraits, tokenComparatorFactories, invListsTypeTraits, primaryComparatorFactories,
                        tokenizerFactory, dataflowHelperFactory,
                        dataset.getModificationCallbackFactory(storageComponentProvider, index, null,
                                IndexOperation.FULL_MERGE, null),
                        storageComponentProvider.getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                secondaryPartitionConstraint);

        spec.addRoot(compactOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }
}
