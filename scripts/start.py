#!/usr/bin/env python
"""跨平台启动脚本。

用法：
    python scripts/start.py              # 启动 serve（默认）
    python scripts/start.py fetch        # 单次抓取
    python scripts/start.py refresh-pool  # 刷新股票池
    python scripts/start.py status       # 查看状态
"""

from __future__ import annotations

import sys
from pathlib import Path

# 把 src 加进路径
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from rdp.cli import main  # noqa: E402

if __name__ == "__main__":
    main()