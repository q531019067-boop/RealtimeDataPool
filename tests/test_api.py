"""API 基础行为测试。"""

from rdp.api import _RateLimitMiddleware


def test_zero_rate_limit_means_disabled():
    middleware = _RateLimitMiddleware(app=lambda scope, receive, send: None, limit_per_min=0)
    assert middleware._check("203.0.113.1") == (True, 0)

