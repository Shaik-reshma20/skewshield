#!/usr/bin/env bash
set -euo pipefail
python -m skewshield.cli generate --output data/generated --orders 100000 --customers 1000
python -m skewshield.cli run --input data/generated --output outputs --run-id demo-001
