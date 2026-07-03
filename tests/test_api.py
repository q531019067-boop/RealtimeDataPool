"""API 基础行为测试。"""

from rdp.api import WEB_DIR, _RateLimitMiddleware


def test_zero_rate_limit_means_disabled():
    middleware = _RateLimitMiddleware(app=lambda scope, receive, send: None, limit_per_min=0)
    assert middleware._check("203.0.113.1") == (True, 0)


def test_web_dir_points_to_project_frontend():
    assert WEB_DIR.name == "web"
    assert (WEB_DIR / "index.html").is_file()

