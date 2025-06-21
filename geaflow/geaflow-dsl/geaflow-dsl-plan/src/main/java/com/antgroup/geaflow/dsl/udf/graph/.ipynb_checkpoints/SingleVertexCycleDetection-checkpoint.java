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

package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Description(name = "single_vertex_cycle_detection", 
             description = "Detects if cycles exist starting from a given vertex")
public class SingleVertexCycleDetection implements AlgorithmUserFunction<Object, Boolean> {

    private AlgorithmRuntimeContext<Object, Boolean> context;
    private Object sourceVertexId;
    private Map<Object, Boolean> visitedVertices;
    private String edgeType;
    private String vertexType;
    private boolean cycleDetected;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Boolean> context, Object[] parameters) {
        this.context = context;
        this.visitedVertices = new HashMap<>();
        this.cycleDetected = false;
        
        Objects.requireNonNull(parameters[0], "Source vertex id cannot be null");
        this.sourceVertexId = TypeCastUtil.cast(parameters[0], context.getGraphSchema().getIdType());
        
        if (parameters.length >= 2) {
            this.edgeType = (String) parameters[1];
        }
        if (parameters.length >= 3) {
            this.vertexType = (String) parameters[2];
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Boolean> messages) {
        if (vertexType != null && !vertex.getLabel().equals(vertexType)) {
            return;
        }

        if (context.getCurrentIterationId() == 1L) {
            if (vertex.getId().equals(sourceVertexId)) {
                visitedVertices.put(vertex.getId(), true);
                sendMessagesToNeighbors(vertex);
            }
            return;
        }

        while (messages.hasNext()) {
            messages.next();
            
            if (visitedVertices.containsKey(vertex.getId())) {
                // 检测到环路
                cycleDetected = true;
                return;
            } else {
                visitedVertices.put(vertex.getId(), true);
                sendMessagesToNeighbors(vertex);
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> newValue) {
        if (vertex.getId().equals(sourceVertexId)) {
            // 只有源顶点输出结果
            int result = cycleDetected ? 1 : 0;
            context.take(ObjectRow.create(vertex.getId(), result));
        }
    }

    private void sendMessagesToNeighbors(RowVertex vertex) {
        for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
            if (edgeType == null || edge.getLabel().equals(edgeType)) {
                context.sendMessage(edge.getTargetId(), true);
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("has_cycle", IntegerType.INSTANCE, false)
        );
    }
}