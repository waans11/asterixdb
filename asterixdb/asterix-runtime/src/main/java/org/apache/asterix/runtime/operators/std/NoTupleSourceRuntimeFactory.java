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
package org.apache.asterix.runtime.operators.std;

import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputSourcePushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class NoTupleSourceRuntimeFactory implements IPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "nts";
    }

    @Override
    public IPushRuntime createPushRuntime(IHyracksTaskContext ctx) {
        return new AbstractOneInputSourcePushRuntime() {

            @Override
            public void open() throws HyracksDataException {
                try {
                    writer.open();
                } finally {
                    writer.close();
                }
            }

        };
    }

}
