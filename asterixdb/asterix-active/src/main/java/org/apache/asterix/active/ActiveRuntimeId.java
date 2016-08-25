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
package org.apache.asterix.active;

import java.io.Serializable;

public class ActiveRuntimeId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final EntityId entityId;
    private final String runtimeName;
    private final int partition;
    private final int hashCode;

    public ActiveRuntimeId(EntityId entityId, String runtimeName, int partition) {
        this.entityId = entityId;
        this.runtimeName = runtimeName;
        this.partition = partition;
        this.hashCode = toString().hashCode();
    }

    @Override
    public String toString() {
        return "(" + entityId + ")" + "[" + partition + "]:" + runtimeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActiveRuntimeId)) {
            return false;
        }
        ActiveRuntimeId other = (ActiveRuntimeId) o;
        return other.entityId.equals(entityId) && other.getRuntimeName().equals(runtimeName)
                && other.getPartition() == partition;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    public String getRuntimeName() {
        return runtimeName;
    }

    public int getPartition() {
        return partition;
    }

    public EntityId getEntityId() {
        return entityId;
    }
}
