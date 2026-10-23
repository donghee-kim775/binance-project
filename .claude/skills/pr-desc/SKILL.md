---
name: pr-desc
description: 현재 브랜치와 main의 차이를 읽고 .github/pull_request_template.md 형식으로 PR 제목과 설명을 작성한다. PR 설명, PR 본문, PR 정리를 요청받았을 때 사용한다.
argument-hint: "[base 브랜치, 기본값 main]"
allowed-tools: Bash(git *), Read, Grep, Glob
---

# PR 설명 작성

base 브랜치: `$ARGUMENTS` (비어 있으면 `main`)

## 절차

1. 상태 확인
   ```bash
   git fetch -q origin
   git branch --show-current
   git status --short
   git log --oneline origin/<base>..HEAD
   git diff --stat origin/<base>..HEAD
   ```
2. **커밋 안 된 변경이 있으면 먼저 경고한다.** 푸시해도 PR에 포함되지 않기 때문이다.
3. 바뀐 파일을 실제로 읽고 무엇이 왜 바뀌었는지 파악한다. 파일 이름만 보고 추측하지 않는다.
4. `.github/pull_request_template.md`를 읽고 그 섹션 순서를 그대로 따른다.
5. 결과를 코드 블록 하나로 출력한다. 사용자가 그대로 붙여 넣을 수 있어야 한다.

## 작성 규칙

- **Change**는 추가 / 수정 / 삭제로 나누고, 파일 경로와 한 줄 설명을 함께 적는다.
- 문서만 바뀐 PR이면 Checklist의 테스트 항목에 "(해당 없음: 문서만 변경)"을 붙인다.
- **Warning Point**에는 리뷰어가 놓치기 쉬운 것을 적는다: 문서끼리 어긋나는 설계, 설정값 위험, 머지 후에야 적용되는 것.
- 비어 있는 폴더는 git에 올라가지 않으므로, 새 빈 폴더가 있으면 언급한다 (`.gitkeep` 필요).
- 제목 후보를 2~3개 함께 제시한다. 형식: `<type>: <요약>` (예: `docs: add ingestion & topic plans`).
- 커밋, 푸시, PR 생성은 하지 않는다. 설명만 만든다.
