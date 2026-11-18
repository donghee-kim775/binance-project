package com.ingestion.collector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 단일 Binance WebSocket 스트림에 붙어 텍스트 메시지를 넘겨주는 최소 클라이언트.
 * JDK 내장 {@link java.net.http.HttpClient} WebSocket API만 사용해 별도 의존성이 없다.
 *
 * 재연결은 고정 지연으로만 처리한다 — 24시간 커넥션 만료 전 make-before-break, exponential
 * backoff 등은 POC 이후 core/ws 모듈로 뽑을 때 구현한다 (datasource-ingestion/README.md 참고).

WebSocket.Listener 객체 구현 시, 작성사항
 ㄴ onOpen : 연결 성공 시 1회 호출
 ㄴ onError : 연결 에러 발생 시 호출
 ㄴ onText : 메시지 수신 시 호출
 ㄴ onClose : 연결 종료 시 호출
**/
public class BinanceStreamClient implements WebSocket.Listener {

    private static final Logger log = LoggerFactory.getLogger(BinanceStreamClient.class);
    // 재연결 전 대기 시간 — 고정 지연, exponential backoff 아님
    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);

    // ── 생성자로 주입받는 연결 설정값 ──────────────────────────────
    private final String streamLabel;         // 로그 구분용 라벨 (예: "usdm-optical-bookTicker")
    private final URI uri;                   // 접속할 WebSocket 주소
    private final MessageHandler messageHandler; // 메시지 수신 시 위임할 콜백 — 이 클래스는 내용을 모름 (정의는 onText 바로 위)

    // ── 내부 상태 ──────────────────────────────────────────────
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final StringBuilder messageBuffer = new StringBuilder(); // 조각난 텍스트 프레임 조립용

    private volatile WebSocket webSocket; // onOpen에서 저장, close()에서 사용
    private volatile boolean closing = false; // close() 호출 여부 — true면 재연결하지 않음

    public BinanceStreamClient(String streamLabel, URI uri, MessageHandler messageHandler) {
        this.streamLabel = streamLabel; // 로그 구분용 라벨
        this.uri = uri; // ws 주소
        this.messageHandler = messageHandler; // 메시지 수신 시 위임할 콜백
    }

    // ── 연결 시작 ──────────────────────────────────────────────
    // * 연결을 시작한다.
    //   buildAsync()가 즉시 리턴하는 논블로킹 호출
    // * 실제 연결·수신은 HttpClient 내부 데몬 스레드가 처리
    public void connect() {
        log.info("[{}] connecting to {}", streamLabel, uri);
        httpClient.newWebSocketBuilder()
                .buildAsync(uri, this) // 논블로킹 호출 — [다음] 연결되면 JDK가 this의 onOpen · onText · onError · onClose를 부른다
                .whenComplete((webSocket, error) -> { // whenComplete는 buildAsync()가 즉시 리턴한 CompletableFuture가 완료될 때 호출되는 콜백
                    if (error != null) {
                        log.warn("[{}] connect failed: {}", streamLabel, error.toString());
                        scheduleReconnect();
                    }
                });
    }

    // ── 정상 종료 ──────────────────────────────────────────────
    // * 정상 종료 close 프레임을 보낸다. 이후 onClose 콜백이 오더라도 재연결은 하지 않는다.
    public void close() {
        closing = true;
        WebSocket current = webSocket;
        if (current != null) {
            log.info("[{}] closing", streamLabel);
            current.sendClose(WebSocket.NORMAL_CLOSURE, "shutting down");
        }
    }

    // ── WebSocket.Listener 콜백 (JDK → 이 클래스) ─────────────────
    // * JDK HttpClient가 이 클래스의 메서드를 부른다. 이 클래스가 구현한다
    // * 연결 성공 시 1회 호출 — close()에서 쓸 수 있도록 세션을 저장해둔다
    // [호출] JDK HttpClient — 연결 성공 시 1회
    // [스레드] HttpClient-N-Worker-N
    @Override
    public void onOpen(WebSocket webSocket) {
        log.info("[{}] connected", streamLabel);
        this.webSocket = webSocket;
        WebSocket.Listener.super.onOpen(webSocket);
    }

    // ── 메시지 처리 콜백 (이 클래스 → 바깥) ──
    // * 바로 아래 onText가 완성된 메시지마다 handle()을 부른다. 구현은 바깥(CollectorRunner)이 람다로 넘긴다
    // * Listener 콜백과 방향이 반대다 — 이 클래스는 메시지 내용을 모른다
    //   예: raw -> ingestService.onRaw(spec, raw, System.currentTimeMillis())
    //       → handle(message)를 부르면 이 람다 본문이 실행된다
    @FunctionalInterface
    public interface MessageHandler {
        void handle(String message);
    }

    // 메시지 수신 시 호출 — 조각(fragment)일 수 있어 last=true가 올 때까지 버퍼에 모은다
    // message -> buffer -> toString -> buffer 비우기 -> messageHandler.handle
    // [호출] JDK HttpClient — 텍스트 프레임이 올 때마다
    // [스레드] HttpClient-N-Worker-N
    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        // 조각난 텍스트 프레임을 messageBuffer에 모은다
        messageBuffer.append(data);
        // 
        if (last) {
            String message = messageBuffer.toString(); // 조각이 끝나면 버퍼를 문자열로 변환
            messageBuffer.setLength(0); // buffer 초기화
            try {
                // 파싱·처리는 이 클래스 책임이 아니라 콜백 쪽 책임
                messageHandler.handle(message); // [다음] CollectorRunner의 람다 → MarketDataIngestService.onRaw
            } catch (Exception e) {
                log.error("[{}] failed to handle message: {}", streamLabel, message, e);
            }
        }
        
        // super.onText => request(1) 호출, 다음 메시지 수신 허용
        return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    // 연결 에러 발생 시 호출 — 의도적으로 닫는 중이 아니면 재연결
    // [호출] JDK HttpClient — 연결 에러 시
    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        log.warn("[{}] connection error: {}", streamLabel, error.toString());
        if (!closing) {
            scheduleReconnect();
        }
    }

    // 연결 종료 시 호출 — 의도적으로 닫는 중(close() 호출됨)이면 조용히 로그만, 아니면 재연결
    // [호출] JDK HttpClient — 서버가 close 프레임을 보냈을 때
    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        if (closing) {
            log.info("[{}] closed (code={}, reason={})", streamLabel, statusCode, reason);
        } else {
            log.warn("[{}] closed (code={}, reason={}) — reconnecting in {}s",
                    streamLabel, statusCode, reason, RECONNECT_DELAY.toSeconds());
            scheduleReconnect();
        }
        return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    // ── 재연결 ────────────────────────────────────────────────
    // * RECONNECT_DELAY 만큼 지연 후 connect()를 다시 호출 — 논블로킹 (Thread.sleep 아님)
    private void scheduleReconnect() {
        CompletableFuture.delayedExecutor(RECONNECT_DELAY.toMillis(), TimeUnit.MILLISECONDS)
                .execute(this::connect); // [다음] connect() — [스레드] CompletableFuture.delayedExecutor (ForkJoinPool 공용 풀)
    }
}
