package com.consumer.join.basis;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * reinterpretAsKeyedStream 앞에서, 레코드가 "keyBy였다면 갔을 subtask"에 실제로 와 있는지 확인한다 (co-partitioning 실험).
 *
 * 어긋난 레코드를 그대로 넘기면 keyed state 접근에서 "Key group X is not in KeyGroupRange" 예외가 나고,
 * job이 재시작해도 같은 레코드에서 또 실패한다. 그래서 세고(basisWrongSubtask) 버린다.
 * 어긋남은 보통 쓰는 쪽(수집기 · flink-dedup)의 partitioner 설정, 파티션 수, 병렬도 중 하나가 바뀐 경우다.
 */
class SubtaskCheck<T> extends ProcessFunction<T, T> {

    private static final Logger log = LoggerFactory.getLogger(SubtaskCheck.class);

    private final KeySelector<T, String> symbolOf;   // SpotMid::symbol, MarkPrice::symbol
    private final String inputName;                  // spot / mark (로그 구분용)

    private transient int subtaskIndex;      // 이 subtask 번호 (0, 1, 2)
    private transient int parallelism;       // 3
    private transient int maxParallelism;    // 128
    private transient Counter wrongSubtask;

    SubtaskCheck(KeySelector<T, String> symbolOf, String inputName) {
        this.symbolOf = symbolOf;
        this.inputName = inputName;
    }

    // [호출] Flink 런타임 — 태스크 시작 시 한 번
    @Override
    public void open(OpenContext openContext) {
        TaskInfo taskInfo = getRuntimeContext().getTaskInfo();
        subtaskIndex = taskInfo.getIndexOfThisSubtask();
        parallelism = taskInfo.getNumberOfParallelSubtasks();
        maxParallelism = taskInfo.getMaxNumberOfParallelSubtasks();
        wrongSubtask = getRuntimeContext().getMetricGroup().counter("basisWrongSubtask");
    }

    // [호출] Flink 런타임 — 레코드마다 (keyBy 없이 소스 · 파서와 같은 subtask에서, chain)
    @Override
    public void processElement(T record, Context ctx, Collector<T> out) throws Exception {
        String symbol = symbolOf.getKey(record);
        if (!validate(symbol)) {
            return;
        }
        out.collect(record);
    }

    private boolean validate(String symbol) {
        int ownerSubtask = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                symbol,            // BTCUSDT
                maxParallelism,    // 128
                parallelism);      // 3
        if (ownerSubtask != subtaskIndex) {
            wrongSubtask.inc();
            log.warn("[basis] skip {}: wrong subtask (key group owner {}, here {}) — {}",
                    inputName, ownerSubtask, subtaskIndex, symbol);
            return false;
        }
        return true;
    }
}
