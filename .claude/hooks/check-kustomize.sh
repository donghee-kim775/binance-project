#!/usr/bin/env bash
# deploy/ 아래 yaml을 고쳤을 때만 kustomize 렌더링을 검사한다.
# 실패하면 exit 2 → stderr 내용이 Claude에게 전달되어 바로 고치게 된다.

file=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("tool_input",{}).get("file_path",""))' 2>/dev/null)

case "$file" in
  */datasource-ingestion/deploy/*.yaml) ;;
  *) exit 0 ;;
esac

# Docker Desktop이 꺼져 있으면 kubectl 링크가 끊긴다 → 검사 생략
kubectl version --client >/dev/null 2>&1 || exit 0

if ! out=$(kubectl kustomize "$CLAUDE_PROJECT_DIR/datasource-ingestion/deploy" 2>&1 >/dev/null); then
  echo "kustomize 렌더링 실패 ($file):" >&2
  echo "$out" >&2
  exit 2
fi
