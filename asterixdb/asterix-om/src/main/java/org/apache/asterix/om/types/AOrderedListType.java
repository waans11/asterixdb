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
package org.apache.asterix.om.types;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.asterix.om.base.IAObject;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AOrderedListType extends AbstractCollectionType {

    private static final long serialVersionUID = 1L;

    public static final AOrderedListType FULL_OPEN_ORDEREDLIST_TYPE = new AOrderedListType(BuiltinType.ANY, "");

    /**
     * @param itemType
     *            if null, the list is untyped
     */
    public AOrderedListType(IAType itemType, String typeName) {
        super(itemType, typeName);
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.ORDEREDLIST;
    }

    @Override
    public String getDisplayName() {
        return "AOrderedList";
    }

    @Override
    public String toString() {
        return "[ " + itemType + " ]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AOrderedListType) {
            AOrderedListType type = (AOrderedListType) obj;
            return this.itemType.equals(type.itemType);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.itemType.hashCode() * 10;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public ObjectNode toJSON()  {
        ObjectMapper om = new ObjectMapper();
        ObjectNode type = om.createObjectNode();
        type.put("type", AOrderedListType.class.getName());
        type.set("item-type", itemType.toJSON());
        return type;
    }
}
