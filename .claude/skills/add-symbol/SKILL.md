---
name: add-symbol
description: 수집 심볼이나 섹터를 추가, 삭제할 때 관련 문서와 매니페스트를 한 번에 맞춘다. "심볼 추가", "섹터 추가", "NVDA 빼줘" 같은 요청에 사용한다.
argument-hint: "<심볼 또는 섹터> [product] [asset-class] [sector]"
---

# 수집 심볼 / 섹터 변경

요청: `$ARGUMENTS`

같은 정보가 여러 파일에 흩어져 있으므로 **전부 함께** 고친다. 하나라도 빠지면 문서끼리 어긋난다.

## 1. 확인

- Binance `exchangeInfo`로 심볼이 실제로 `TRADING` 상태인지 확인한다.
  - Spot: `https://api.binance.com/api/v3/exchangeInfo?symbol=<SYMBOL>`
  - USD-M: `https://fapi.binance.com/fapi/v1/exchangeInfo` (주식은 `contractType: TRADIFI_PERPETUAL`)
- 주식은 Spot에 없고 USD-M에만 있다.
- 기존 섹터에 넣을지, 새 섹터를 만들지 정해지지 않았으면 사용자에게 묻는다.

## 2. 고칠 파일

| 파일 | 고칠 내용 |
| --- | --- |
| `APIdocs/0TickerInfoTable.md` | 심볼 표에 행 추가 (기초자산, 분야) |
| `datasource-ingestion/deploy/variable-rate/*.yaml` | 해당 pod ConfigMap의 `symbols`. 새 섹터면 파일을 새로 만든다 |
| `datasource-ingestion/deploy/fixed-rate/*.yaml` | markPrice pod의 `symbols`. 새 섹터면 파일을 새로 만든다 |
| `datasource-ingestion/deploy/kustomization.yaml` | 새 파일을 만들었으면 `resources`에 추가 |
| `plan/ingestion/plan.md` | 수집 대상 표, 구성도, pod 목록, 구독 URL, 소켓 수 합계 |
| `plan/topic/plan.md` | 파티션 수 (stock 토픽은 심볼 수에 맞춤), ERD의 symbols |
| `README.md` | "수집 대상" 표, Deployment·Pod·연결 수 |

## 3. 규칙

- 새 deploy 파일은 같은 그룹의 기존 파일을 복사해 이름, label, ConfigMap만 바꾼다. 그룹 규칙(replicas, strategy, PDB)은 `CLAUDE.md`를 따른다.
- 섹터 pod는 combined stream(`/stream?streams=a/b/c`), 심볼 1개 pod는 단일 stream(`/ws/...`)이다.
- **파티션은 줄일 수 없다.** 파티션 수를 바꿔야 하면 바로 고치지 말고 사용자에게 알린다.
- 소켓 수 합계: variable-rate는 pod 종류 × 3소켓 × 2 replicas, fixed-rate는 pod 종류 × 1소켓.

## 4. 마무리

- `kubectl kustomize datasource-ingestion/deploy`로 렌더링을 검사한다.
- 바꾼 파일 목록과, 사용자가 직접 결정해야 하는 것(파티션 수 등)을 보고한다.
