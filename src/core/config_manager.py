import yaml
from pathlib import Path
import sqlite3

class ConfigManager:
    @staticmethod
    def load_config(config_type: str):
        with open(Path(__file__).parent.parent.parent / f"config/{config_type}_config.yaml") as f:
            return yaml.safe_load(f)

class DeviceTableManager:
    @staticmethod
    def create_devices_table(db_path):
        conn = sqlite3.connect(db_path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                type TEXT,
                connector TEXT
            )
        ''')
        conn.commit()
        conn.close()

    @staticmethod
    def insert_devices_from_yaml(db_path, yaml_path):
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
        devices = data.get('devices', {})
        with sqlite3.connect(db_path) as conn:
            for name, info in devices.items():
                conn.execute(
                    'INSERT OR REPLACE INTO devices (name, type, connector) VALUES (?, ?, ?)',
                    (name, info.get('type', ''), info.get('connector', ''))
                )
            conn.commit()

    @staticmethod
    def create_type_tables_from_devices(db_path, yaml_path):
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
        devices = data.get('devices', {})
        types = set(info.get('type', '') for info in devices.values())
        conn = sqlite3.connect(db_path)
        for t in types:
            if t:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {t} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        device_name TEXT,
                        timestamp TEXT,
                        value REAL
                    )
                """)
        conn.commit()
        conn.close()

    # For backward compatibility, allow an optional db_path as the first argument
    @staticmethod
    def create_type_tables_from_schema(*args):
        if len(args) == 3:
            # Ignore the first argument (db_path) for compatibility
            _, schema_path, device_yaml_path = args
        elif len(args) == 2:
            schema_path, device_yaml_path = args
        else:
            raise TypeError("create_type_tables_from_schema expects 2 or 3 arguments (db_path, schema_path, device_yaml_path) or (schema_path, device_yaml_path)")
        with open(device_yaml_path, 'r') as f:
            device_data = yaml.safe_load(f)
        with open(schema_path, 'r') as f:
            schema_data = yaml.safe_load(f)
        for device_type, type_info in schema_data.items():
            if device_type == 'database':
                continue
            db_file = type_info.get('file')
            table_name = type_info.get('table')
            type_schema = type_info.get('schema', [])
            if not db_file or not table_name or not type_schema:
                continue
            columns = [f"{col['name']} {col['type']}" for col in type_schema]
            if not columns:
                continue
            
            # Check if device_name is already in the schema
            has_device_name = any(col.startswith('device_name ') for col in columns)
            
            conn = sqlite3.connect(db_file)
            if has_device_name:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        {', '.join(columns)}
                    )
                """)
            else:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        device_name TEXT,
                        {', '.join(columns)}
                    )
                """)
            conn.commit()
            conn.close()

    @staticmethod
    def create_devices_table_from_schema(schema_path):
        import yaml
        with open(schema_path, 'r') as f:
            schema_config = yaml.safe_load(f)
        devices_db = schema_config['devices_db']
        db_file = devices_db['file']
        table_name = devices_db['table']
        columns = [f"{col['name']} {col['type']}" for col in devices_db['schema']]
        with sqlite3.connect(db_file) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {', '.join(columns)}
                )
            """)
            conn.commit()

    @staticmethod
    def insert_devices_from_yaml_to_devices_db(schema_path, device_yaml_path):
        import yaml
        with open(schema_path, 'r') as f:
            schema_config = yaml.safe_load(f)
        devices_db = schema_config['devices_db']
        db_file = devices_db['file']
        table_name = devices_db['table']
        columns = [col['name'] for col in devices_db['schema']]
        with open(device_yaml_path, 'r') as f:
            device_config = yaml.safe_load(f)
        devices = device_config.get('devices', {})
        with sqlite3.connect(db_file) as conn:
            for name, info in devices.items():
                # Only insert fields that are in the schema
                values = []
                for col in columns:
                    if col == 'name':
                        values.append(name)
                    else:
                        values.append(info.get(col, ''))
                conn.execute(
                    f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?' for _ in columns])})",
                    values
                )
            conn.commit()