"""
JSON 직렬화 패치 모듈
사용법: engine_common.py 상단에 아래 import 추가 후
        json.dump() 호출 시 default=json_safe 인자 추가

  from json_fix_patch import json_safe, safe_json_dump
"""

import json
import numpy as np
from datetime import datetime, date


def json_safe(o):
    """numpy scalar / bool / datetime → JSON 직렬화 가능 타입 변환"""
    if isinstance(o, np.generic):          # np.bool_, np.int64, np.float64 등
        return o.item()
    if isinstance(o, (datetime, date)):    # pandas Timestamp, datetime 방어
        return o.isoformat()
    if isinstance(o, (set, frozenset)):
        return list(o)
    raise TypeError(f"Not serializable: {type(o).__name__}")


def sanitize(v):
    """중첩 dict/list 구조 전체를 재귀적으로 정제"""
    if isinstance(v, np.generic):
        return v.item()
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: sanitize(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [sanitize(x) for x in v]
    if isinstance(v, (set, frozenset)):
        return [sanitize(x) for x in v]
    return v


def safe_json_dump(data, filepath, **kwargs):
    """
    json.dump 래퍼 — numpy/datetime 타입 자동 처리
    
    사용 예:
        safe_json_dump(results, "stock_data.json")
        safe_json_dump(history, "history.json", indent=2)
    """
    kwargs.setdefault("ensure_ascii", False)
    kwargs.setdefault("indent", 2)
    kwargs.setdefault("default", json_safe)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, **kwargs)
    print(f"[저장 완료] {filepath}")


# ──────────────────────────────────────────────────────────
# engine_common.py 적용 예시 (기존 코드 → 수정 코드)
# ──────────────────────────────────────────────────────────
#
# [기존]
#   with open("stock_data.json", "w", encoding="utf-8") as f:
#       json.dump(results, f, ensure_ascii=False, indent=2)
#
# [수정 A] default 콜백만 추가 (최소 변경)
#   from json_fix_patch import json_safe
#   with open("stock_data.json", "w", encoding="utf-8") as f:
#       json.dump(results, f, default=json_safe, ensure_ascii=False, indent=2)
#
# [수정 B] 래퍼 함수로 교체 (가장 간단)
#   from json_fix_patch import safe_json_dump
#   safe_json_dump(results, "stock_data.json")
#   safe_json_dump(history, "history.json")
# ──────────────────────────────────────────────────────────
