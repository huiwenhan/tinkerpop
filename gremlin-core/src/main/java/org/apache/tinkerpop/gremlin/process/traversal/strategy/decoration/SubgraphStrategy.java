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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeByPathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This {@link TraversalStrategy} provides a way to limit the view of a {@link Traversal}.  By providing
 * {@link Traversal} representations that represent a form of filtering "predicate" for vertices and/or edges,
 * this strategy will inject that "predicate" into the appropriate places of a traversal thus restricting what
 * it traverses and returns.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SubgraphStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy {

    private final Traversal<Vertex, ?> vertexPredicate;
    private final Traversal<Edge, ?> edgePredicate;

    private SubgraphStrategy(final Traversal<Vertex, ?> vertexPredicate, final Traversal<Edge, ?> edgePredicate) {
        this.vertexPredicate = vertexPredicate;

        // if there is no vertex predicate there is no need to test either side of the edge
        if (null == vertexPredicate) {
            this.edgePredicate = edgePredicate;
        } else {
            final Traversal<Object, Vertex> inVertexPredicate = __.inV().has(vertexPredicate);
            final Traversal<Object, Vertex> outVertexPredicate = __.outV().has(vertexPredicate);

            // if there is a vertex predicate then there is an implied edge filter on vertices even if there is no
            // edge predicate provided by the user.
            if (null == edgePredicate)
                this.edgePredicate = __.and(inVertexPredicate.asAdmin(), outVertexPredicate.asAdmin());
            else
                this.edgePredicate = edgePredicate.asAdmin().addStep(new AndStep<>(edgePredicate.asAdmin(), inVertexPredicate.asAdmin(), outVertexPredicate.asAdmin()));
        }
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final List<GraphStep> graphSteps = TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal);
        final List<VertexStep> vertexSteps = TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal);

        if (vertexPredicate != null) {
            final List<Step> vertexStepsToInsertFilterAfter = new ArrayList<>();
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeOtherVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddVertexStartStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(graphSteps.stream().filter(s -> s.getReturnClass().equals(Vertex.class)).collect(Collectors.toList()));

            vertexStepsToInsertFilterAfter.forEach(s -> TraversalHelper.insertAfterStep(new HasTraversalStep<>(traversal, vertexPredicate.asAdmin().clone(), false), s, traversal));
        }

        if (edgePredicate != null) {
            final List<Step> edgeStepsToInsertFilterAfter = new ArrayList<>();
            edgeStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal));
            edgeStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddEdgeByPathStep.class, traversal));
            edgeStepsToInsertFilterAfter.addAll(graphSteps.stream().filter(s -> s.getReturnClass().equals(Edge.class)).collect(Collectors.toList()));
            edgeStepsToInsertFilterAfter.addAll(vertexSteps.stream().filter(s -> s.getReturnClass().equals(Edge.class)).collect(Collectors.toList()));

            edgeStepsToInsertFilterAfter.forEach(s -> TraversalHelper.insertAfterStep(new HasTraversalStep<>(traversal, edgePredicate.asAdmin().clone(), false), s, traversal));
        }

        // explode g.V().out() to g.V().outE().inV() only if there is an edge predicate otherwise
        vertexSteps.stream().filter(s -> s.getReturnClass().equals(Vertex.class)).forEach(s -> {
            if (null == edgePredicate)
                TraversalHelper.insertAfterStep(new HasTraversalStep<>(traversal, vertexPredicate.asAdmin().clone(), false), s, traversal);
            else {
                final VertexStep replacementVertexStep = new VertexStep(traversal, Edge.class, s.getDirection(), s.getEdgeLabels());
                Step intermediateFilterStep = null;
                if (s.getDirection() == Direction.BOTH)
                    intermediateFilterStep = new EdgeOtherVertexStep(traversal);
                else
                    intermediateFilterStep = new EdgeVertexStep(traversal, s.getDirection().opposite());

                TraversalHelper.replaceStep(s, replacementVertexStep, traversal);
                TraversalHelper.insertAfterStep(intermediateFilterStep, replacementVertexStep, traversal);
                TraversalHelper.insertAfterStep(new HasTraversalStep<>(traversal, edgePredicate.asAdmin().clone(), false), replacementVertexStep, traversal);

                if (vertexPredicate != null)
                    TraversalHelper.insertAfterStep(new HasTraversalStep<>(traversal, vertexPredicate.asAdmin().clone(), false), intermediateFilterStep, traversal);
            }
        });
    }

    public Traversal<Vertex, ?> getVertexPredicate() {
        return vertexPredicate;
    }

    public Traversal<Edge, ?> getEdgePredicate() {
        return edgePredicate;
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {

        private Traversal<Vertex, ?> vertexPredicate = null;
        private Traversal<Edge, ?> edgePredicate = null;

        private Builder() {}

        public Builder vertexPredicate(final Traversal<Vertex, ?> predicate) {
            vertexPredicate = predicate;
            return this;
        }

        public Builder edgePredicate(final Traversal<Edge, ?> predicate) {
            edgePredicate = predicate;
            return this;
        }

        public SubgraphStrategy create() {
            if (null == edgePredicate && null == vertexPredicate) throw new IllegalStateException("A subgraph must be filtered by an edge or vertex filter");
            return new SubgraphStrategy(vertexPredicate, edgePredicate);
        }
    }
}
