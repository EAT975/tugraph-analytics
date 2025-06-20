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
-- 创建专门用于环路测试的图
CREATE GRAPH IF NOT EXISTS cycle_test (
  Vertex person (
    id bigint ID,
    name varchar
  ),
  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    weight double
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

INSERT INTO cycle_test.person (id, name) 
VALUES 
  (1, 'Alice'), 
  (2, 'Bob'), 
  (3, 'Charlie'),
  (4, 'David'),
  (5, 'Eve'),
  (6, 'Frank');

INSERT INTO cycle_test.knows (srcId, targetId, weight) 
VALUES 
  (1, 2, 0.8),   -- 环路1: 1->2->3->1
  (2, 3, 0.7),
  (3, 1, 0.9),
  
  (3, 4, 0.6),   -- 非环路部分
  (4, 5, 0.5),   -- 非环路部分
  
  (5, 5, 1.0),   -- 自环
  (6, 6, 1.0);   -- 自环