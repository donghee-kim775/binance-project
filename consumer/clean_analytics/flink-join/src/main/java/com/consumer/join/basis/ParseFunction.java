package com.consumer.join.basis;

import java.io.Serializable;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BasisParser를 Flink 연산자로 감싼다. 파싱 결과가 null(깨진 메시지)이면 내보내지 않는다 (= 버림).
 *
 * kstreams-join은 mapValues("parse.spot") + filter("valid.spot") 두 단계로 하는 일을 여기서는 한 연산자로 한다.
 * Flink는 연산자 사이에 null을 넘길 수 없어서 map 뒤에 filter를 둘 수 없다.
 *
 * 파서는 메서드 참조로 받는다 (new ParseFunction<>(BasisParser::parseSpot)).
 * Flink가 연산자를 직렬화해 TaskManager로 보내므로, 받는 타입도 Serializable이어야 한다.
 */
class ParseFunction<T> extends ProcessFunction<String, T> {

    // = BasisParser::parseSpot · BasisParser::parseMark
    interface Parser<T> extends Serializable {
        T parse(String envelopeJson);
    }

    private final Parser<T> parser;

    ParseFunction(Parser<T> parser) {
        this.parser = parser;
    }

    // [호출] Flink 런타임 — 입력 레코드마다
    @Override
    public void processElement(String envelopeJson, Context ctx, Collector<T> out) {
        T parsed = parser.parse(envelopeJson);   // [다음] BasisParser.parseSpot / parseMark
        if (parsed != null) {
            out.collect(parsed);
        }
    }
}
