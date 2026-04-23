"""User-facing labels for structured collection failures."""

FAILURE_LABELS = {
    "missing_credentials": "설정 오류",
    "provider_rate_limited": "API 한도 초과",
    "provider_error": "공급자 오류",
    "schema_changed": "응답 구조 변경",
    "no_data": "데이터 없음",
    "unexpected_exception": "예상치 못한 오류",
}


def get_failure_label(failure_code: str | None) -> str:
    """Return a short Korean label for one failure code."""
    if not failure_code:
        return "원인 미상"
    return FAILURE_LABELS.get(failure_code, failure_code)
