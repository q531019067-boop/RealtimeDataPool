#!/usr/bin/env bash
# Linux/Mac 启动脚本
set -e
cd "$(dirname "$0")"
exec python scripts/start.py "$@"