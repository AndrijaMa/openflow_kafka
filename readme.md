python3 get_sql_connector_parameters.py --deployment test --runtime demo  --out-dir params
python3 get_connector_parameters.py --deployment test --runtime demo            # all connectors
python3 get_connector_parameters.py --deployment test --runtime demo --connector Kafka
python3 get_connector_parameters.py --list-runtimes