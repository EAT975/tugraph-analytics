-- 创建结果表
CREATE TABLE result_tb (
    has_cycle int
) WITH (
    type='file',
    geaflow.dsl.file.path='${target}'
);

-- 使用modern图
USE GRAPH modern;

-- 调用算法并插入结果
INSERT INTO result_tb
CALL single_vertex_cycle_detection(1) YIELD (has_cycle)
RETURN has_cycle;