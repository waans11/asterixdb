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
package org.apache.asterix.app.nc.task;

import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.nc.NodeControllerService;

public class StartFailbackTask implements INCLifecycleTask {

    private static final long serialVersionUID = 1L;

    @Override
    public void perform(IControllerService cs) throws HyracksDataException {
        NodeControllerService ncs = (NodeControllerService) cs;
        IAppRuntimeContext runtimeContext = (IAppRuntimeContext) ncs.getApplicationContext().getApplicationObject();
        runtimeContext.getRemoteRecoveryManager().startFailbackProcess();
    }
}
