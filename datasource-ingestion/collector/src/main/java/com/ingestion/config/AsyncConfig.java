package com.ingestion.config;

import java.util.concurrent.Executor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * WebSocket 콜백 스레드에서 메시지 파싱·발행 작업을 분리하기 위한 executor.
 * 지정 안 하면 스프링이 기본으로 쓰는 SimpleAsyncTaskExecutor는 스레드를 무제한으로 찍어내서,
 * 메시지 유입량이 늘어나는 상황을 대비해 크기를 제한한 풀을 직접 등록한다.
 */
@Configuration
@EnableAsync
public class AsyncConfig {

    // [호출] 스프링 — 시작 시 1회. MarketDataIngestService.onRaw의 @Async("ingestExecutor")가 이 이름으로 찾는다
    @Bean(name = "ingestExecutor")
    public Executor ingestExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // default thread pool size 2 threads
        executor.setCorePoolSize(2);
        // scaling threadpool size maximum 4 threads
        executor.setMaxPoolSize(4);
        // queue capacity 200 tasks
        executor.setQueueCapacity(200);
        // thread name prefix
        executor.setThreadNamePrefix("ingest-async-");
        executor.initialize();
        return executor;
    }
}
