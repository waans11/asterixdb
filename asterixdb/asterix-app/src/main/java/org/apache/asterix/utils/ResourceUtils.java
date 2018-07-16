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

package org.apache.asterix.utils;

import java.util.List;

import org.apache.asterix.app.resource.OperatorResourcesComputer;
import org.apache.asterix.app.resource.PlanStage;
import org.apache.asterix.app.resource.PlanStagesGenerator;
import org.apache.asterix.app.resource.RequiredCapacityVisitor;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.job.resource.ClusterCapacity;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourceUtils {

    // Temp :
    private static final Logger LOGGER = LogManager.getLogger();
    //

    private ResourceUtils() {
    }

    /**
     * Calculates the required cluster capacity from a given query plan, the computation locations,
     * the operator memory budgets, and frame size.
     *
     * @param plan,
     *            a given query plan.
     * @param computationLocations,
     *            the partitions for computation.
     * @param physicalOptimizationConfig,
     *            a PhysicalOptimizationConfig.
     * @return the required cluster capacity for executing the query.
     * @throws AlgebricksException
     *             if the query plan is malformed.
     */
    public static IClusterCapacity getRequiredCapacity(ILogicalPlan plan,
            AlgebricksAbsolutePartitionConstraint computationLocations,
            PhysicalOptimizationConfig physicalOptimizationConfig) throws AlgebricksException {
        final int frameSize = physicalOptimizationConfig.getFrameSize();
        final int sortFrameLimit = physicalOptimizationConfig.getMaxFramesExternalSort();
        final int groupFrameLimit = physicalOptimizationConfig.getMaxFramesForGroupBy();
        final int joinFrameLimit = physicalOptimizationConfig.getMaxFramesForJoin();
        final int textSearchFrameLimit = physicalOptimizationConfig.getMaxFramesForTextSearch();
        final List<PlanStage> planStages = getStages(plan);

        // Temp :
        IClusterCapacity conservativeRequiredCapacity =
                getConservativeRequiredCapacity(plan, computationLocations, physicalOptimizationConfig);
        IClusterCapacity stageBasedRequiredCapacity =
                getStageBasedRequiredCapacity(planStages, computationLocations.getLocations().length, sortFrameLimit,
                        groupFrameLimit, joinFrameLimit, textSearchFrameLimit, frameSize);

        StringBuilder planStagesStr = new StringBuilder();
        for (int i = 0; i < planStages.size(); i++) {
            planStagesStr.append(planStages.get(i).toString() + "\n");
        }

        LOGGER.log(Level.INFO,
                "\t" + "ResourceUtils.getRequiredCapacity" + "\tlimitQueryExecution:\t"
                        + physicalOptimizationConfig.getLimitQueryExecution() + "\tconservativeLimitQueryExecution:\t"
                        + physicalOptimizationConfig.getConservativeLimitQueryExecution() + "\t"
                        + "\tstageBasedRequiredCapacity:\t" + stageBasedRequiredCapacity.toString()
                        + "\tconservativeRequiredCapacity:\t" + conservativeRequiredCapacity.toString() + "\t#Stages:\t"
                        + planStages.size() + "\tstages:\n" + planStagesStr.toString());

        boolean conductConservativeCapacityCalculation =
                physicalOptimizationConfig.getConservativeLimitQueryExecution();
        // Conduct a naive capacity calculation?
        return conductConservativeCapacityCalculation ? conservativeRequiredCapacity : stageBasedRequiredCapacity;
        //
    }

    public static List<PlanStage> getStages(ILogicalPlan plan) throws AlgebricksException {
        // There could be only one root operator for a top-level query plan.
        final ILogicalOperator rootOp = plan.getRoots().get(0).getValue();
        final PlanStagesGenerator stagesGenerator = new PlanStagesGenerator();
        rootOp.accept(stagesGenerator, null);
        return stagesGenerator.getStages();
    }

    public static IClusterCapacity getStageBasedRequiredCapacity(List<PlanStage> stages, int computationLocations,
            int sortFrameLimit, int groupFrameLimit, int joinFrameLimit, int textSearchFrameLimit, int frameSize) {
        final OperatorResourcesComputer computer = new OperatorResourcesComputer(computationLocations, sortFrameLimit,
                groupFrameLimit, joinFrameLimit, textSearchFrameLimit, frameSize);
        final IClusterCapacity clusterCapacity = new ClusterCapacity();
        final Long maxRequiredMemory = stages.stream().mapToLong(stage -> stage.getRequiredMemory(computer)).max()
                .orElseThrow(IllegalStateException::new);
        clusterCapacity.setAggregatedMemoryByteSize(maxRequiredMemory);
        final Integer maxRequireCores = stages.stream().mapToInt(stage -> stage.getRequiredCores(computer)).max()
                .orElseThrow(IllegalStateException::new);
        clusterCapacity.setAggregatedCores(maxRequireCores);
        return clusterCapacity;
    }

    // Temp : copied and modified from the previous codebase: assumes that all operators are executed in parallel.
    public static IClusterCapacity getConservativeRequiredCapacity(ILogicalPlan plan,
            AlgebricksAbsolutePartitionConstraint computationLocations,
            PhysicalOptimizationConfig physicalOptimizationConfig) throws AlgebricksException {
        final int frameSize = physicalOptimizationConfig.getFrameSize();
        final int sortFrameLimit = physicalOptimizationConfig.getMaxFramesExternalSort();
        final int groupFrameLimit = physicalOptimizationConfig.getMaxFramesForGroupBy();
        final int joinFrameLimit = physicalOptimizationConfig.getMaxFramesForJoin();
        final int textSearchFrameLimit = physicalOptimizationConfig.getMaxFramesForTextSearch();

        // Creates a cluster capacity visitor.
        IClusterCapacity clusterCapacity = new ClusterCapacity();
        RequiredCapacityVisitor visitor = new RequiredCapacityVisitor(computationLocations.getLocations().length,
                sortFrameLimit, groupFrameLimit, joinFrameLimit, textSearchFrameLimit, frameSize, clusterCapacity);

        // There could be only one root operator for a top-level query plan.
        ILogicalOperator rootOp = plan.getRoots().get(0).getValue();
        rootOp.accept(visitor, null);
        return clusterCapacity;
    }
    //

}
