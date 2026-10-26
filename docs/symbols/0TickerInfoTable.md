# Ticker 정보 테이블

Collector가 수집할 심볼 목록이다. 2026-10-20에 아래 API의 `exchangeInfo`를 조회해 `TRADING` 상태인 심볼만 정리했다.

- Spot: `https://api.binance.com/api/v3/exchangeInfo`
- USD-M Futures: `https://fapi.binance.com/fapi/v1/exchangeInfo`
- COIN-M Futures: `https://dapi.binance.com/dapi/v1/exchangeInfo`

주식은 Binance Spot에 없고, **USD-M Futures의 TradFi 무기한 선물**(`contractType: TRADIFI_PERPETUAL`)로만 거래된다. 따라서 주식 심볼은 모두 `wss://fstream.binance.com` 도메인에서 수집한다. 스트림 이름은 소문자로 쓴다(예: `nvdausdt@aggTrade`).

`exchangeInfo`의 `underlyingType`으로 기초자산 종류를 구분할 수 있다.

| underlyingType | 의미 |
| --- | --- |
| `COIN` | 암호화폐 |
| `EQUITY` | 미국 주식·ETF |
| `KR_EQUITY` | 한국 주식·ETF |
| `HK_EQUITY` | 홍콩 주식 |
| `CN_EQUITY` | 중국 본토 주식 |

---

## 1. 비트코인

envelope `sector`: `sov` (코인 섹터 분류는 [cryptoSectorREADME.md](cryptoSectorREADME.md))

| 시장 | 심볼 | 종류 | 결제 자산 | WSS 도메인 |
| --- | --- | --- | --- | --- |
| Spot | `BTCUSDT` | 현물 | USDT | `stream.binance.com:9443` |
| Spot | `BTCUSDC` | 현물 | USDC | `stream.binance.com:9443` |
| Spot | `BTCFDUSD` | 현물 | FDUSD | `stream.binance.com:9443` |
| USD-M | `BTCUSDT` | 무기한 | USDT | `fstream.binance.com` |
| USD-M | `BTCUSDC` | 무기한 | USDC | `fstream.binance.com` |
| USD-M | `BTCUSD1` | 무기한 | USD1 | `fstream.binance.com` |
| USD-M | `BTCU` | 무기한 | U | `fstream.binance.com` |
| USD-M | `BTCUSDT_261225` | 분기물 (2026-12-25 만기) | USDT | `fstream.binance.com` |
| USD-M | `BTCUSDT_270326` | 분기물 (2027-03-26 만기) | USDT | `fstream.binance.com` |
| COIN-M | `BTCUSD_PERP` | 무기한 | BTC | `dstream.binance.com` |
| COIN-M | `BTCUSD_261225` | 분기물 (2026-12-25 만기) | BTC | `dstream.binance.com` |
| COIN-M | `BTCUSD_270326` | 분기물 (2027-03-26 만기) | BTC | `dstream.binance.com` |

> Spot `BTCUSDT`와 USD-M `BTCUSDT`는 심볼이 같지만 도메인이 다른 별개 시장이다. Kafka 토픽이나 키에 시장 구분(`spot`, `usdm`, `coinm`)을 함께 넣어야 섞이지 않는다.

---

## 2. 광학 섹터 (광통신·포토닉스)

모두 USD-M TradFi 무기한 선물이다.

| 심볼 | 기초자산 | 상장 시장 | 비고 |
| --- | --- | --- | --- |
| `LITEUSDT` | Lumentum | 미국 | 광트랜시버, 레이저 |
| `CRDOUSDT` | Credo | 미국 | 고속 연결 칩 (광학 인접 인터커넥트) |
| `ALABUSDT` | Astera Labs | 미국 | 데이터센터 연결 칩 (광학 인접 인터커넥트) |

---

## 3. 반도체

모두 USD-M TradFi 무기한 선물이다.

### 3-1. 미국 반도체 (설계·파운드리·IDM)

| 심볼 | 기초자산 | 분야 |
| --- | --- | --- |
| `NVDAUSDT` | NVIDIA | GPU, AI 가속기 |
| `AVGOUSDT` | Broadcom | 네트워크·맞춤형 칩 |
| `MRVLUSDT` | Marvell | 데이터 인프라 칩 |

---

## 참고

- **수집 시간:** TradFi 무기한 선물은 24시간 거래되지만, 기초자산 시장이 닫힌 시간에는 거래량이 적고 가격 움직임이 다르다. 고정 rate 그룹과 변동성 그룹을 나눌 때 함께 고려한다 ([plan.md](../../plan/ingestion/plan.md) 고려 사항 1).
- **목록 갱신:** 심볼은 계속 추가되고 상장 폐지된다. 운영 시에는 이 표를 하드코딩하지 말고, `exchangeInfo`를 주기적으로 조회해 `status`가 `TRADING`인 심볼만 구독한다.
- **분류 제외:** `AGPUUSDT`처럼 티커만으로 기초자산을 확인하지 못한 종목은 넣지 않았다.
