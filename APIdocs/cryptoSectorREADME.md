# 암호화폐 섹터 분류

주식에 업종(반도체, 광통신 등)이 있듯이 코인도 **프로젝트가 무슨 역할을 하는지**로 분류한다. 이 문서는 envelope `sector` 필드에 crypto 심볼의 값을 넣을 때 쓰는 기준이다. 결정 배경은 [topic/plan.md](../plan/topic/plan.md) 고려 사항 4를 참고한다.

## 주식 섹터와 다른 점

- **표준이 없다.** 주식에는 GICS 같은 업종 분류 표준이 있지만, 코인은 CoinGecko, CoinMarketCap, 거래소가 각자 카테고리를 붙인다. 이름과 범위가 사이트마다 다르다.
- **한 코인이 여러 분류에 속한다.** 예를 들어 ETH는 L1이면서 DeFi 생태계의 기반이다. 사이트들은 태그를 여러 개 붙인다.
- **유행에 따라 새 분류가 생긴다.** AI, RWA처럼 시기마다 주목받는 테마가 바뀐다.
- **Binance API는 분류를 주지 않는다.** `exchangeInfo`에는 `underlyingType: COIN`까지만 있고, 섹터 정보는 없다 ([0TickerInfoTable.md](0TickerInfoTable.md)). 그래서 주식 섹터처럼 직접 정해 ConfigMap에 넣는다.

## 분류와 `sector` 값

| `sector` 값 | 분류 | 뜻 | 대표 코인 |
| --- | --- | --- | --- |
| `sov` | Store of Value (SoV) / Payment | 가치 저장이나 송금이 주목적. "디지털 금" 포지션 | BTC, LTC |
| `l1` | Layer 1 | 독립된 블록체인 자체. 그 위에서 스마트 컨트랙트와 앱이 돌아가는 플랫폼 | ETH, SOL, AVAX, ADA |
| `l2` | Layer 2 | L1(주로 이더리움) 위에서 거래를 더 빠르고 싸게 처리하는 확장 체인 | ARB, OP, POL |
| `defi` | DeFi | 은행·거래소 없이 스마트 컨트랙트로 하는 금융 (탈중앙 거래소, 대출 등) | UNI(거래소), AAVE(대출) |
| `meme` | Meme | 기술·실사용보다 커뮤니티와 밈으로 가격이 움직임. 변동성이 매우 큼 | DOGE, SHIB, PEPE |
| `exchange-token` | Exchange Token | 거래소가 발행한 토큰. 수수료 할인 등에 쓰임 | BNB |
| `oracle` | Oracle / Infra | 블록체인 밖의 데이터(가격 등)를 체인 안으로 전달하는 인프라 | LINK |
| `stablecoin` | Stablecoin | 달러 등에 1:1로 가치를 고정한 코인 | USDT, USDC |
| `ai`, `gaming`, `rwa` | 테마 | AI, 게임, 실물자산(국채·부동산) 토큰화 등 시기별 테마 | 추가할 때 확인 |

> 대표 코인은 2026-10 기준 일반적인 분류다. 섹터를 실제로 추가할 때는 CoinGecko, CoinMarketCap의 카테고리 페이지에서 현재 분류를 확인한다.

## 값을 정하는 규칙

- **하나만 고른다.** envelope의 `sector`는 문자열 하나다. 여러 분류에 속하는 코인은 **가격을 주로 움직이는 이유(주된 용도)** 를 고른다. ETH는 DeFi에도 걸치지만 플랫폼 수요로 가격이 움직이므로 `l1`이다.
- **소문자 kebab-case로 쓴다.** stock 섹터(`optical`, `semi`)와 같은 형식이다.
- **stock 섹터와 겹치지 않게 정한다.** pod 이름이 `<product>-<섹터>`(`usdm-sov`, `usdm-optical`)라 자산 종류가 이름에 없기 때문이다.
- **분류 축은 "역할"로 통일한다.** `coin`, `crypto`처럼 자산 종류를 뜻하는 값은 쓰지 않는다. 자산 종류는 `asset_class`가 이미 담고 있다.
- **stablecoin은 수집 대상이 아니라 결제 자산이다.** `BTCUSDT`의 USDT처럼 심볼 뒤에 붙는 쪽이라, 지금 구조에서는 `sector` 값으로 쓸 일이 거의 없다. 분류 체계를 완성하기 위해 표에만 둔다.

## 현재 수집 심볼

| 심볼 | 시장 | `sector` | Pod |
| --- | --- | --- | --- |
| `BTCUSDT` | Spot, USD-M | `sov` | `spot-sov`, `usdm-sov`, `markprice-usdm-sov` |

## 섹터를 추가할 때 볼 것

stock을 optical, semi로 묶은 이유는 **같은 섹터 종목은 같이 움직이므로 섹터 단위로 비교한다**는 것이다. crypto도 같은 기준으로 바스켓을 만든다.

| 바스켓 | 예 | 보는 것 |
| --- | --- | --- |
| `l1` | ETH, SOL, AVAX | 플랫폼끼리 경쟁 관계 → 상관관계, 상대 강도 |
| `meme` | DOGE, SHIB, PEPE | 변동성이 커서 basis(현물-선물 가격 차)와 펀딩비가 크게 벌어짐 |

섹터 pod를 추가하는 절차는 `/add-symbol`을 따른다.
