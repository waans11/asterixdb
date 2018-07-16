/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.runtime.job.resource;

import java.text.DecimalFormat;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.job.resource.IReadOnlyClusterCapacity;
import org.apache.hyracks.control.cc.scheduler.IResourceManager;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// To avoid the computation cost for checking the capacity constraint for each node,
// currently the admit/allocation decisions are based on the aggregated resource information.
// TODO(buyingyi): investigate partition-aware resource control.
public class JobCapacityController implements IJobCapacityController {

    private final IResourceManager resourceManager;
    private static final Logger LOGGER = LogManager.getLogger();
    private static int capacityExceedCount = 0;
    private static int queueCount = 0;
    private static int executeCount = 0;
    private static int releaseCount = 0;

    // Temp :
    private static final DecimalFormat decFormat = new DecimalFormat("#.######");
    //

    public JobCapacityController(IResourceManager resourceManager) {
        this.resourceManager = resourceManager;
    }

    @Override
    public JobSubmissionStatus allocate(JobSpecification job) throws HyracksException {
        boolean limitQueryExecution = job.getLimitQueryExecution();
        boolean capacityExceeded = false;
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();
        IReadOnlyClusterCapacity maximumCapacity = resourceManager.getMaximumCapacity();
        IClusterCapacity currentCapacity = resourceManager.getCurrentCapacity();
        long currentAggregatedMemoryByteSize = currentCapacity.getAggregatedMemoryByteSize();
        int currentAggregatedAvailableCores = currentCapacity.getAggregatedCores();
        if (!(reqAggregatedMemoryByteSize <= maximumCapacity.getAggregatedMemoryByteSize()
                && reqAggregatedNumCores <= maximumCapacity.getAggregatedCores())) {
            // Temp :
            capacityExceeded = true;
            capacityExceedCount++;
            job.setCapacityExceeded(capacityExceeded);
            if (limitQueryExecution) {
                // Temp :
                LOGGER.log(Level.INFO, this.hashCode() + "\t" + "allocate" + "\tlimitQueryExecution:\t"
                        + limitQueryExecution + "\tcapacityExceeded:\t" + capacityExceeded + "\tFAIL(EXCEED_CAPACITY)"
                        + "\trequested_memory_size(MB):\t"
                        + decFormat.format((double) reqAggregatedMemoryByteSize / 1048576) + "\tmax_memory_size(MB):\t"
                        + decFormat.format((double) maximumCapacity.getAggregatedMemoryByteSize() / 1048576)
                        + "\trequested_num_cores:\t" + reqAggregatedNumCores + "\tmax_num_cores:\t"
                        + maximumCapacity.getAggregatedCores() + "\tcurrently_available_memory_size(MB):\t"
                        + decFormat.format((double) currentAggregatedMemoryByteSize / 1048576)
                        + "\tcurrently_available_num_cores:\t" + currentAggregatedAvailableCores
                        + "\tcapacityExceedCount:\t" + capacityExceedCount + "\tqueueCount:\t" + queueCount
                        + "\texecuteCount:\t" + executeCount + "\treleaseCount:\t" + releaseCount + "\tQuery:\n"
                        + job.getOriginalQuery(true));
                //
                throw HyracksException.create(ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY, requiredCapacity.toString(),
                        maximumCapacity.toString());
            } else {
                // Temp :
                LOGGER.log(Level.INFO, this.hashCode() + "\t" + "allocate" + "\tlimitQueryExecution:\t"
                        + limitQueryExecution + "\tcapacityExceeded:\t" + capacityExceeded
                        + "\tFAIL(EXCEED_CAPACITY)-BUT-EXECUTE" + "\trequested_memory_size(MB):\t"
                        + decFormat.format((double) reqAggregatedMemoryByteSize / 1048576) + "\tmax_memory_size(MB):\t"
                        + decFormat.format((double) maximumCapacity.getAggregatedMemoryByteSize() / 1048576)
                        + "\trequested_num_cores:\t" + reqAggregatedNumCores + "\tmax_num_cores:\t"
                        + maximumCapacity.getAggregatedCores() + "\tcurrently_available_memory_size(MB):\t"
                        + decFormat.format((double) currentAggregatedMemoryByteSize / 1048576)
                        + "\tcurrently_available_num_cores:\t" + currentAggregatedAvailableCores
                        + "\tcapacityExceedCount:\t" + capacityExceedCount + "\tqueueCount:\t" + queueCount
                        + "\texecuteCount:\t" + executeCount + "\treleaseCount:\t" + releaseCount + "\tQuery:\n"
                        + job.getOriginalQuery(true));
                //
            }
        }
        // Temp :
        if (limitQueryExecution && !capacityExceeded && !(reqAggregatedMemoryByteSize <= currentAggregatedMemoryByteSize
                && reqAggregatedNumCores <= currentAggregatedAvailableCores)) {
            queueCount++;
            // Temp :
            LOGGER.log(Level.INFO,
                    this.hashCode() + "\t" + "allocate" + "\tlimitQueryExecution:\t" + limitQueryExecution
                            + "\tcapacityExceeded:\t" + capacityExceeded + "\tQUEUE" + "\trequested_memory_size(MB):\t"
                            + decFormat.format((double) reqAggregatedMemoryByteSize / 1048576)
                            + "\tmax_memory_size(MB):\t"
                            + decFormat.format((double) maximumCapacity.getAggregatedMemoryByteSize() / 1048576)
                            + "\trequested_num_cores:\t" + reqAggregatedNumCores + "\tmax_num_cores:\t"
                            + maximumCapacity.getAggregatedCores() + "\tcurrently_available_memory_size(MB):\t"
                            + decFormat.format((double) currentAggregatedMemoryByteSize / 1048576)
                            + "\tcurrently_available_num_cores:\t" + currentAggregatedAvailableCores
                            + "\tcapacityExceedCount:\t" + capacityExceedCount + "\tqueueCount:\t" + queueCount
                            + "\texecuteCount:\t" + executeCount + "\treleaseCount:\t" + releaseCount + "\tQuery:\n"
                            + job.getOriginalQuery(true));
            //
            return JobSubmissionStatus.QUEUE;
        }

        // Temp :
        executeCount++;
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "allocate" + "\tlimitQueryExecution:\t" + limitQueryExecution
                + "\tcapacityExceeded:\t" + capacityExceeded + "\tEXECUTE" + "\trequested_memory_size(MB):\t"
                + decFormat.format((double) reqAggregatedMemoryByteSize / 1048576) + "\tmax_memory_size(MB):\t"
                + decFormat.format((double) maximumCapacity.getAggregatedMemoryByteSize() / 1048576)
                + "\trequested_num_cores:\t" + reqAggregatedNumCores + "\tmax_num_cores:\t"
                + maximumCapacity.getAggregatedCores() + "\tcurrently_available_memory_size(MB):\t"
                + decFormat.format((double) currentAggregatedMemoryByteSize / 1048576)
                + "\tcurrently_available_num_cores:\t" + currentAggregatedAvailableCores + "\tcapacityExceedCount:\t"
                + capacityExceedCount + "\tqueueCount:\t" + "\texecuteCount:\t" + executeCount + "\treleaseCount:\t"
                + releaseCount + "\tQuery:\n" + job.getOriginalQuery(true));

        if (capacityExceeded) {
            currentCapacity.setAggregatedMemoryByteSize(0);
            currentCapacity.setAggregatedCores(0);
        } else {
            currentCapacity.setAggregatedMemoryByteSize(currentAggregatedMemoryByteSize - reqAggregatedMemoryByteSize);
            currentCapacity.setAggregatedCores(currentAggregatedAvailableCores - reqAggregatedNumCores);
        }

        return JobSubmissionStatus.EXECUTE;
    }

    @Override
    public void release(JobSpecification job) {
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();
        IClusterCapacity currentCapacity = resourceManager.getCurrentCapacity();
        long aggregatedMemoryByteSize = currentCapacity.getAggregatedMemoryByteSize();
        int aggregatedNumCores = currentCapacity.getAggregatedCores();
        currentCapacity.setAggregatedMemoryByteSize(aggregatedMemoryByteSize + reqAggregatedMemoryByteSize);
        currentCapacity.setAggregatedCores(aggregatedNumCores + reqAggregatedNumCores);

        // Temp :
        releaseCount++;
        boolean capacityExceeded = job.getCapacityExceeded();
        boolean limitQueryExecution = job.getLimitQueryExecution();
        IReadOnlyClusterCapacity maximumCapacity = resourceManager.getMaximumCapacity();
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "release" + "\tlimitQueryExecution:\t" + limitQueryExecution
                + "\tcapacityExceeded:\t" + capacityExceeded + "\tRELEASE" + "\trequested_memory_size(MB):\t"
                + decFormat.format((double) reqAggregatedMemoryByteSize / 1048576) + "\tmax_memory_size(MB):\t"
                + decFormat.format((double) maximumCapacity.getAggregatedMemoryByteSize() / 1048576)
                + "\trequested_num_cores:\t" + reqAggregatedNumCores + "\tmax_num_cores:\t"
                + maximumCapacity.getAggregatedCores() + "\tcurrently_available_memory_size(MB):\t"
                + decFormat.format((double) (aggregatedMemoryByteSize + reqAggregatedMemoryByteSize) / 1048576)
                + "\tcurrently_available_num_cores:\t" + (aggregatedNumCores + reqAggregatedNumCores)
                + "\tcapacityExceedCount:\t" + capacityExceedCount + "\tqueueCount:\t" + "\texecuteCount:\t"
                + executeCount + "\treleaseCount:\t" + releaseCount + "\tQuery:\n" + job.getOriginalQuery(true));
        //
    }

}
