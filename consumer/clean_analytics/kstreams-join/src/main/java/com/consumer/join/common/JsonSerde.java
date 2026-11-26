package com.consumer.join.common;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * record ↔ JSON 바이트. Kafka Streams가 store · changelog · 출력 토픽에 값을 쓸 때 쓴다.
 *
 *   SpotMid → store latest-spot (+ changelog)    : 심볼별 최신 Spot
 *   Basis   → binance.analytics.crypto.basis     : flink-join과 같은 JSON
 *
 * Python으로 치면 json.dumps(asdict(x)).encode() / Cls(**json.loads(b)) 짝이다.
 * Jackson은 record를 생성자로 만들고, @JsonProperty가 있으면 그 이름(snake_case)으로 쓴다.
 */
public final class JsonSerde<T> implements Serde<T> {

    // ObjectMapper는 설정이 끝나면 여러 스레드가 같이 써도 안전하다
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    // [호출] Kafka Streams — store · changelog · sink에 값을 쓸 때마다 (Serializer.serialize)
    @Override
    public Serializer<T> serializer() {
        return (topic, value) -> {
            if (value == null) {
                return null; // null은 그대로 (KTable에서는 "삭제"를 뜻한다)
            }
            try {
                return MAPPER.writeValueAsBytes(value);
            } catch (Exception error) {
                throw new SerializationException("JSON으로 바꾸지 못했다: " + type.getSimpleName(), error);
            }
        };
    }

    // [호출] Kafka Streams — store에서 값을 꺼내거나 changelog로 store를 복원할 때마다 (Deserializer.deserialize)
    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, type);
            } catch (Exception error) {
                throw new SerializationException("JSON을 읽지 못했다: " + type.getSimpleName(), error);
            }
        };
    }
}
