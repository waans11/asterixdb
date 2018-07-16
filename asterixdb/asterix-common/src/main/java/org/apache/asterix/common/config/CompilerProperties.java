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
package org.apache.asterix.common.config;

import static org.apache.hyracks.control.common.config.OptionTypes.BOOLEAN;
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.LONG_BYTE_UNIT;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.MEGABYTE;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.util.StorageUtil;

public class CompilerProperties extends AbstractProperties {

    public enum Option implements IOption {
        COMPILER_SORTMEMORY(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(32L, MEGABYTE),
                "The memory budget (in bytes) for a sort operator instance in a partition"),
        COMPILER_JOINMEMORY(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(32L, MEGABYTE),
                "The memory budget (in bytes) for a join operator instance in a partition"),
        COMPILER_GROUPMEMORY(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(32L, MEGABYTE),
                "The memory budget (in bytes) for a group by operator instance in a partition"),
        COMPILER_TEXTSEARCHMEMORY(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(32L, MEGABYTE),
                "The memory budget (in bytes) for an inverted-index-search operator instance in a partition"),
        COMPILER_FRAMESIZE(
                INTEGER_BYTE_UNIT,
                StorageUtil.getIntSizeInBytes(32, KILOBYTE),
                "The page size (in bytes) for computation"),
        COMPILER_PARALLELISM(
                INTEGER,
                COMPILER_PARALLELISM_AS_STORAGE,
                "The degree of parallelism for query "
                        + "execution. Zero means to use the storage parallelism as the query execution parallelism, while "
                        + "other integer values dictate the number of query execution parallel partitions. The system will "
                        + "fall back to use the number of all available CPU cores in the cluster as the degree of parallelism "
                        + "if the number set by a user is too large or too small"),
        COMPILER_LIMITSORTMEMORY(
                BOOLEAN,
                true,
                "For the experiment purpose only: if set to false, the pointer array size will not be "
                        + "included in the sort memory limit."),
        COMPILER_LIMITHASHJOINMEMORY(
                BOOLEAN,
                true,
                "For the experiment purpose only: if set to false, the hash table size for each hash join "
                        + "will not be included in the hash join memory limit."),
        COMPILER_HASHTABLEGARBAGECOLLECTION(
                BOOLEAN,
                true,
                "Sets whether conducting the garbage collection for a hash table. "
                        + "If LimitHashJoinMemory or LimitHashGroupMemory is set, this should be set to false."),
        COMPILER_LIMITHASHGROUPMEMORY(
                BOOLEAN,
                true,
                "For the experiment purpose only: if set to false, the hash table size for each hash join "
                        + "will not be included in the sort memory limit."),
        COMPILER_LIMITTEXTSEARCHMEMORY(
                BOOLEAN,
                true,
                "For the experiment purpose only: if set to false, the memory size for an inverted-index search "
                        + "will not be bounded."),
        COMPILER_LIMITQUERYEXECUTION(
                BOOLEAN,
                true,
                "For the experiment purpose only: if set to false, the query will be executed without any "
                        + "budget consideration. If set to true, the required capacity will be calculated based on "
                        + "an assumption: per stage calculation."),
        COMPILER_CONSERVATIVELIMITQUERYEXECUTION(
                BOOLEAN,
                false,
                "For the experiment purpose only: if set to true, the query will be executed based on "
                        + "a budget consideration. However, the required capacity will be calculated based on "
                        + "an assumption: all operators will be executed at the same time."
                        + "If both limit query execution and this value is set, then this should be set to false."),
        COMPILER_PRINTINDEXENTRYDURINGBULKLOAD(
                BOOLEAN,
                false,
                "For the experiment purpose only: if set to true, print the index entry during the index-bulkload."),
        COMPILER_STRINGOFFSET(INTEGER, 0, "Position of a first character in a String/Binary (0 or 1)");

        private final IOptionType type;
        private final Object defaultValue;
        private final String description;

        Option(IOptionType type, Object defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public IOptionType type() {
            return type;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }

        @Override
        public boolean hidden() {
            return this == COMPILER_STRINGOFFSET;
        }
    }

    public static final String COMPILER_SORTMEMORY_KEY = Option.COMPILER_SORTMEMORY.ini();

    public static final String COMPILER_GROUPMEMORY_KEY = Option.COMPILER_GROUPMEMORY.ini();

    public static final String COMPILER_JOINMEMORY_KEY = Option.COMPILER_JOINMEMORY.ini();

    public static final String COMPILER_TEXTSEARCHMEMORY_KEY = Option.COMPILER_TEXTSEARCHMEMORY.ini();

    public static final String COMPILER_PARALLELISM_KEY = Option.COMPILER_PARALLELISM.ini();

    public static final String COMPILER_LIMITSORTMEMORY_KEY = Option.COMPILER_LIMITSORTMEMORY.ini();

    public static final String COMPILER_LIMITHASHJOINMEMORY_KEY = Option.COMPILER_LIMITHASHJOINMEMORY.ini();

    public static final String COMPILER_HASHTABLEGARBAGECOLLECTION_KEY =
            Option.COMPILER_HASHTABLEGARBAGECOLLECTION.ini();

    public static final String COMPILER_LIMITHASHGROUPMEMORY_KEY = Option.COMPILER_LIMITHASHGROUPMEMORY.ini();

    public static final String COMPILER_LIMITTEXTSEARCHMEMORY_KEY = Option.COMPILER_LIMITTEXTSEARCHMEMORY.ini();

    public static final String COMPILER_LIMITQUERYEXECUTION_KEY = Option.COMPILER_LIMITQUERYEXECUTION.ini();

    public static final String COMPILER_CONSERVATIVELIMITQUERYEXECUTION_KEY =
            Option.COMPILER_CONSERVATIVELIMITQUERYEXECUTION.ini();

    public static final String COMPILER_PRINTINDEXENTRYDURINGBULKLOAD_KEY =
            Option.COMPILER_PRINTINDEXENTRYDURINGBULKLOAD.ini();

    public static final int COMPILER_PARALLELISM_AS_STORAGE = 0;

    public CompilerProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public long getSortMemorySize() {
        return accessor.getLong(Option.COMPILER_SORTMEMORY);
    }

    public long getJoinMemorySize() {
        return accessor.getLong(Option.COMPILER_JOINMEMORY);
    }

    public long getGroupMemorySize() {
        return accessor.getLong(Option.COMPILER_GROUPMEMORY);
    }

    public long getTextSearchMemorySize() {
        return accessor.getLong(Option.COMPILER_TEXTSEARCHMEMORY);
    }

    public int getFrameSize() {
        return accessor.getInt(Option.COMPILER_FRAMESIZE);
    }

    public int getParallelism() {
        return accessor.getInt(Option.COMPILER_PARALLELISM);
    }

    public boolean getLimitSortMemory() {
        return accessor.getBoolean(Option.COMPILER_LIMITSORTMEMORY);
    }

    public boolean getLimitHashGroupMemory() {
        return accessor.getBoolean(Option.COMPILER_LIMITHASHGROUPMEMORY);
    }

    public boolean getLimitHashJoinMemory() {
        return accessor.getBoolean(Option.COMPILER_LIMITHASHJOINMEMORY);
    }

    public boolean getLimitTextSearchMemory() {
        return accessor.getBoolean(Option.COMPILER_LIMITTEXTSEARCHMEMORY);
    }

    public boolean getLimitQueryExecution() {
        return accessor.getBoolean(Option.COMPILER_LIMITQUERYEXECUTION);
    }

    public boolean getConservativeLimitQueryExecution() {
        return accessor.getBoolean(Option.COMPILER_CONSERVATIVELIMITQUERYEXECUTION);
    }

    public boolean getHashTableGarbageCollection() {
        return accessor.getBoolean(Option.COMPILER_HASHTABLEGARBAGECOLLECTION);
    }

    public boolean getPrintIndexEntryDuringBulkLoad() {
        return accessor.getBoolean(Option.COMPILER_PRINTINDEXENTRYDURINGBULKLOAD);
    }

    public int getStringOffset() {
        int value = accessor.getInt(Option.COMPILER_STRINGOFFSET);
        return value > 0 ? 1 : 0;
    }
}
