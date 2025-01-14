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

package org.apache.hyracks.storage.am.lsm.invertedindex.multithread;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.AbstractIndexTestWorker;
import org.apache.hyracks.storage.am.common.TestOperationSelector;
import org.apache.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.exceptions.OccurrenceThresholdPanicException;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.ConjunctiveSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.JaccardSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;

public class LSMInvertedIndexTestWorker extends AbstractIndexTestWorker {

    protected final LSMInvertedIndex invIndex;
    protected final List<ITupleReference> documentCorpus = new ArrayList<ITupleReference>();
    protected final Random rnd = new Random(50);

    protected final IInvertedIndexSearchModifier[] TEST_SEARCH_MODIFIERS = new IInvertedIndexSearchModifier[] {
            new ConjunctiveSearchModifier(), new JaccardSearchModifier(0.8f), new JaccardSearchModifier(0.5f) };

    public LSMInvertedIndexTestWorker(DataGenThread dataGen, TestOperationSelector opSelector, IIndex index,
            int numBatches) throws HyracksDataException {
        super(dataGen, opSelector, index, numBatches);
        invIndex = (LSMInvertedIndex) index;
    }

    @Override
    public void performOp(ITupleReference tuple, TestOperation op) throws HyracksDataException, IndexException {
        LSMInvertedIndexAccessor accessor = (LSMInvertedIndexAccessor) indexAccessor;
        IIndexCursor searchCursor = accessor.createSearchCursor(false);
        IIndexCursor rangeSearchCursor = accessor.createRangeSearchCursor();
        RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
        IBinaryTokenizerFactory tokenizerFactory = invIndex.getTokenizerFactory();
        int searchModifierIndex = Math.abs(rnd.nextInt()) % TEST_SEARCH_MODIFIERS.length;
        InvertedIndexSearchPredicate searchPred = new InvertedIndexSearchPredicate(tokenizerFactory.createTokenizer(),
                TEST_SEARCH_MODIFIERS[searchModifierIndex]);

        switch (op) {
            case INSERT: {
                insert(accessor, tuple);
                break;
            }

            case DELETE: {
                // Randomly pick a document from the corpus to delete.
                if (!documentCorpus.isEmpty()) {
                    int docIndex = Math.abs(rnd.nextInt()) % documentCorpus.size();
                    ITupleReference deleteTuple = documentCorpus.get(docIndex);
                    accessor.delete(deleteTuple);
                    // Swap tupleIndex with last element.
                    documentCorpus.set(docIndex, documentCorpus.get(documentCorpus.size() - 1));
                    documentCorpus.remove(documentCorpus.size() - 1);
                } else {
                    // No existing documents to delete, treat this case as an insert.
                    insert(accessor, tuple);
                }
                break;
            }

            case POINT_SEARCH: {
                searchCursor.reset();
                searchPred.setQueryTuple(tuple);
                searchPred.setQueryFieldIndex(0);
                try {
                    accessor.search(searchCursor, searchPred);
                    consumeCursorTuples(searchCursor);
                } catch (OccurrenceThresholdPanicException e) {
                    // Ignore.
                }
                break;
            }

            case SCAN: {
                rangeSearchCursor.reset();
                accessor.rangeSearch(rangeSearchCursor, rangePred);
                consumeCursorTuples(rangeSearchCursor);
                break;
            }

            case MERGE: {
                accessor.scheduleMerge(NoOpIOOperationCallback.INSTANCE, invIndex.getImmutableComponents());
                break;
            }

            default:
                throw new HyracksDataException("Op " + op.toString() + " not supported.");
        }
    }

    private void insert(LSMInvertedIndexAccessor accessor, ITupleReference tuple) throws HyracksDataException,
            IndexException {
        // Ignore ongoing merges. Do an insert instead.
        accessor.insert(tuple);
        // Add tuple to document corpus so we can delete it.
        ITupleReference copyTuple = TupleUtils.copyTuple(tuple);
        documentCorpus.add(copyTuple);
    }
}
