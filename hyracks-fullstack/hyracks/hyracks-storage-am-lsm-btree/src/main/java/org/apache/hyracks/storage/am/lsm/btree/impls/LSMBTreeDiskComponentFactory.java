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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;

public class LSMBTreeDiskComponentFactory implements ILSMDiskComponentFactory {
    private final TreeIndexFactory<BTree> btreeFactory;
    private final BloomFilterFactory bloomFilterFactory;
    private final ILSMComponentFilterFactory filterFactory;

    public LSMBTreeDiskComponentFactory(TreeIndexFactory<BTree> btreeFactory, BloomFilterFactory bloomFilterFactory,
            ILSMComponentFilterFactory filterFactory) {
        this.btreeFactory = btreeFactory;
        this.bloomFilterFactory = bloomFilterFactory;
        this.filterFactory = filterFactory;
    }

    @Override
    public LSMBTreeDiskComponent createComponent(LSMComponentFileReferences cfr) throws HyracksDataException {
        return new LSMBTreeDiskComponent(btreeFactory.createIndexInstance(cfr.getInsertIndexFileReference()),
                bloomFilterFactory == null ? null
                        : bloomFilterFactory.createBloomFiltertInstance(cfr.getBloomFilterFileReference()),
                filterFactory == null ? null : filterFactory.createFilter());
    }

    public int[] getBloomFilterKeyFields() {
        return bloomFilterFactory == null ? null : bloomFilterFactory.getBloomFilterKeyFields();
    }
}
