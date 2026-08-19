#!/bin/bash

# ==============================================================================
# Script điều khiển chạy Seeder dữ liệu mẫu cho Postgres Source
# ==============================================================================

chmod +x ./seeder.py

python3 ./seeder.py "$@"
