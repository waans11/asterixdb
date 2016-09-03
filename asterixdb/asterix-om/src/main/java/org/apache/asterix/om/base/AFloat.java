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
package org.apache.asterix.om.base;

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.visitors.IOMVisitor;

public class AFloat implements IAObject {

    protected float value;

    public AFloat(float value) {
        this.value = value;
    }

    public float getFloatValue() {
        return value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AFLOAT;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AFloat)) {
            return false;
        } else {
            return Float.floatToIntBits(value) == Float.floatToIntBits(((AFloat) o).getFloatValue());
        }
    }

    @Override
    public int hashCode() {
        return Float.floatToIntBits(value);
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAFloat(this);
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
    public String toString() {
        return "AFloat: {" + value + "}";
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("AFloat", value);

        return json;
    }
}
