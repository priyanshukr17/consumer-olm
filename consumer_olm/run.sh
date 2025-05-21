#!/bin/bash

python3 manage.py shell -c "from order_updater.lat_log_update_consumer import consume; consume()"