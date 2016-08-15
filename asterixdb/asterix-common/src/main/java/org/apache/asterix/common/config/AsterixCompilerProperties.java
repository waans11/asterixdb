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

public class AsterixCompilerProperties extends AbstractAsterixProperties {
    private static final String COMPILER_SORTMEMORY_KEY = "compiler.sortmemory";
    private static final long COMPILER_SORTMEMORY_DEFAULT = (32 << 20); // 32MB

    private static final String COMPILER_GROUPMEMORY_KEY = "compiler.groupmemory";
    private static final long COMPILER_GROUPMEMORY_DEFAULT = (32 << 20); // 32MB

    private static final String COMPILER_JOINMEMORY_KEY = "compiler.joinmemory";
    private static final long COMPILER_JOINMEMORY_DEFAULT = (32 << 20); // 32MB

    private static final String COMPILER_FRAMESIZE_KEY = "compiler.framesize";
    private static int COMPILER_FRAMESIZE_DEFAULT = (32 << 10); // 32KB

    private static final String COMPILER_PREGELIX_HOME = "compiler.pregelix.home";
    private static final String COMPILER_PREGELIX_HOME_DEFAULT = "~/pregelix";

    // This option defines the number of possible hash entries in an external hash group-by operator.
    // The defalut value: the smallest prime number that is greater than 500,000
    // the space occupation of maximum hash entries: 8 bytes * 500,009 = 4,000,072 bytes = 4MB
    // 32 bytes per slot * 500,009 = 15.3MB
    // So, the default size of the groupmemory is 32MB, this default can take 20MB of space if hash key is
    // well distributed.
    private static final String COMPILER_EXTERNAL_GROUP_TABLE_SIZE_KEY = "compiler.grouphashtablesize";
    private static final long COMPILER_EXTERNAL_GROUP_TABLE_SIZE_DEFAULT = 500009;

    // If the maximum expected size of the group hash table exceeds the compiler.groupmemory,
    // this options tells whether the hash table size can be adjusted or not.
    // If so, we will use the specified ration in the config file to adjust table size.
    // If not, an exception will be thrown and the query will not be executed.
    private static final String COMPILER_EXTERNAL_GROUP_TABLE_SIZE_ADJUST_KEY = "compiler.adjustgrouphashtablesize";
    private static final boolean COMPILER_EXTERNAL_GROUP_TABLE_SIZE_ADJUST_DEFAULT = false;

    // When the above adjust option is enabled, we use the ratio to allocate memory to hash table.
    // For example, if it's 0.3, 30% of compiler.groupmemory will be given to hash table.
    // and 70% of compiler.groupmemory will be given to data table.
    private static final String COMPILER_EXTERNAL_GROUP_TABLE_SIZE_ADJUST_RATIO_KEY = "compiler.adjustratioforgrouphashtablesize";
    private static final double COMPILER_EXTERNAL_GROUP_TABLE_SIZE_ADJUST_RATIO_DEFAULT = 0.5;

    public AsterixCompilerProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public long getSortMemorySize() {
        return accessor.getProperty(COMPILER_SORTMEMORY_KEY, COMPILER_SORTMEMORY_DEFAULT,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    public long getJoinMemorySize() {
        return accessor.getProperty(COMPILER_JOINMEMORY_KEY, COMPILER_JOINMEMORY_DEFAULT,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    public long getGroupMemorySize() {
        return accessor.getProperty(COMPILER_GROUPMEMORY_KEY, COMPILER_GROUPMEMORY_DEFAULT,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    public long getGroupHashTableSize() {
        return accessor.getProperty(COMPILER_EXTERNAL_GROUP_TABLE_SIZE_KEY, COMPILER_EXTERNAL_GROUP_TABLE_SIZE_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    // This method is used to set the hash table size when the COMPILER_EXTERNAL_GROUP_TABLE_SIZE_KEY set by
    // the configuration file is expected to exceed the budget.
    public long getGroupHashTableDefaultSize() {
        return COMPILER_EXTERNAL_GROUP_TABLE_SIZE_DEFAULT;
    }

    public int getFrameSize() {
        return accessor.getProperty(COMPILER_FRAMESIZE_KEY, COMPILER_FRAMESIZE_DEFAULT,
                PropertyInterpreters.getIntegerBytePropertyInterpreter());
    }

    public String getPregelixHome() {
        return accessor.getProperty(COMPILER_PREGELIX_HOME, COMPILER_PREGELIX_HOME_DEFAULT,
                PropertyInterpreters.getStringPropertyInterpreter());
    }
}
