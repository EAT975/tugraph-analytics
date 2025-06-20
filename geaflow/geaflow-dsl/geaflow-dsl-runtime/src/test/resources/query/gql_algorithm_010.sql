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
-- 创建结果表
-- 创建结果表
CREATE TABLE result_tb (
    id BIGINT,
    has_cycle INT
) WITH (
    type='file',
    geaflow.dsl.file.path='${target}'
);

-- 使用测试图
USE GRAPH cycle_test;

-- 测试1：检测存在环路的顶点 (1->2->3->1)
INSERT INTO result_tb
CALL single_vertex_cycle_detection(1) YIELD (id, has_cycle)
RETURN id, has_cycle;

-- 测试2：检测不存在环路的顶点 (4只有出边)
INSERT INTO result_tb
CALL single_vertex_cycle_detection(4) YIELD (id, has_cycle)
RETURN id, has_cycle;

-- 测试3：检测自环顶点 (5->5)
INSERT INTO result_tb
CALL single_vertex_cycle_detection(5) YIELD (id, has_cycle)
RETURN id, has_cycle;

-- 测试4：检测另一个自环顶点 (6->6)
INSERT INTO result_tb
CALL single_vertex_cycle_detection(6) YIELD (id, has_cycle)
RETURN id, has_cycle;

-- 测试5：检测不存在的顶点
INSERT INTO result_tb
CALL single_vertex_cycle_detection(100) YIELD (id, has_cycle)
RETURN id, has_cycle;