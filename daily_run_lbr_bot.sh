#!/bin/bash
source /var/www/lu_collect_and_process_bot/venv/Scripts/activate
cd /var/www/lu_collect_and_process_bot/
export PATH=$PATH:/var/www/lu_collect_and_process_bot/
python3 -m src.main -DU