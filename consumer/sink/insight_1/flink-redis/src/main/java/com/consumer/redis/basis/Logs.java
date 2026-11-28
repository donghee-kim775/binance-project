package com.consumer.redis.basis;

/** 로그에 메시지 원문을 남길 때 앞부분만 자른다 (Python의 s[:200] + "..."). */
final class Logs {

    private Logs() {
    }

    static String truncate(String text) {
        return text.length() <= 200 ? text : text.substring(0, 200) + "...";
    }
}
