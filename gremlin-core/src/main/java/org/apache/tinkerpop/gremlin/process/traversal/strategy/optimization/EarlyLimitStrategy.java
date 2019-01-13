/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import javafx.geometry.Side;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class EarlyLimitStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy>
        implements TraversalStrategy.OptimizationStrategy {

    private static final EarlyLimitStrategy INSTANCE = new EarlyLimitStrategy();

    private EarlyLimitStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {

        final List<Step> steps = traversal.getSteps();
        Step insertAfter = null;
        boolean merge = false;
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, j = steps.size(); i < j; i++) {
            final Step step = steps.get(i);
            if (step instanceof RangeGlobalStep) {
                if (insertAfter != null) {
                    TraversalHelper.copyLabels(step, step.getPreviousStep(), true);
                    insertAfter = moveRangeStep((RangeGlobalStep) step, insertAfter, traversal, merge);
                    if (insertAfter instanceof NoneStep) {
                        // any step besides a SideEffectCapStep after a NoneStep would be pointless
                        final int noneStepIndex = TraversalHelper.stepIndex(insertAfter, traversal);
                        for (i = j - 2; i > noneStepIndex; i--) {
                            if (!(steps.get(i) instanceof SideEffectCapStep) && !(steps.get(i) instanceof ProfileSideEffectStep)) {
                                traversal.removeStep(i);
                            }
                        }
                        break;
                    }
                    j = steps.size();
                }
            } else if (
                    step instanceof Barrier || step instanceof BranchStep || step instanceof FlatMapStep ||
                    step instanceof FilterStep || step instanceof RepeatStep) {
                insertAfter = step;
                merge = true;
            } else if (step instanceof SideEffectCapable) {
                merge = false;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Step moveRangeStep(final RangeGlobalStep step, final Step insertAfter, final Traversal.Admin<?, ?> traversal,
                               final boolean merge) {
        final Step rangeStep;
        boolean remove = true;
        if (insertAfter instanceof RangeGlobalStep) {
            final RangeGlobalStep other = (RangeGlobalStep) insertAfter;
            final long low = other.getLowRange() + step.getLowRange();
            if (other.getHighRange() == -1L) {
                rangeStep = new RangeGlobalStep(traversal, low, other.getLowRange() + step.getHighRange());
            } else if (step.getHighRange() == -1L) {
                final long high = other.getHighRange() - other.getLowRange() - step.getLowRange() + low;
                if (low < high) {
                    rangeStep = new RangeGlobalStep(traversal, low, high);
                } else {
                    rangeStep = new NoneStep<>(traversal);
                }
            } else {
                final long high = other.getLowRange() + step.getHighRange();
                rangeStep = new RangeGlobalStep(traversal, low, Math.min(high, other.getHighRange()));
            }
            remove = merge;
            TraversalHelper.replaceStep(merge ? insertAfter : step, rangeStep, traversal);
        } else {
            rangeStep = step.clone();
            TraversalHelper.insertAfterStep(rangeStep, insertAfter, traversal);
        }
        if (remove) traversal.removeStep(step);
        return rangeStep;
    }

    public static EarlyLimitStrategy instance() {
        return INSTANCE;
    }
}
