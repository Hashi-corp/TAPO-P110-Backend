import sqlite3
import aiosqlite
from tapo import ApiClient
from datetime import datetime
import yaml
import socket
from umodbus import conf
from umodbus.client import tcp
import struct  # Add this import for struct unpacking

class DataService:
    def __init__(self, db_config: dict, device_config_path: str, schema_config_path: str):
        self.db_config = db_config
        self.device_config_path = device_config_path
        self.schema_config_path = schema_config_path
        self.create_table(db_config)
        self.device_config = self.load_yaml(device_config_path)
        self.schema_config = self.load_yaml(schema_config_path)

    def load_yaml(self, path):
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def create_table(self, config: dict):
        # Keep all columns from schema, even if some fields are removed from data collection
        columns = []
        for col in config['database']['schema']:
            if col and 'name' in col and 'type' in col:
                columns.append(f"{col['name']} {col['type']}")
        with sqlite3.connect(config['database']['file']) as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {config['database']['table']} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {', '.join(columns)}
                )
            """)
            conn.commit()

    
    async def get_tapo_device_data(self, email: str, password: str, ip: str):
        device = await ApiClient(email, password).p110(ip)
        device_info = await device.get_device_info()
        energy_usage = await device.get_energy_usage()
        data = {}
        for field in self.db_config['database']['schema']:
            if not field or 'name' not in field:
                continue
            field_name = field['name']
            # Try to get from device_info, then energy_usage, else None
            value = getattr(device_info, field_name, None)
            if value is None:
                value = getattr(energy_usage, field_name, None)
            if field_name == 'timestamp':
                value = datetime.now().isoformat()
            data[field_name] = value
        return data
    
    async def save_data(self, data: dict, table_name: str = None):
        table = table_name if table_name else self.db_config['database']['table']
        async with aiosqlite.connect(self.db_config['database']['file']) as conn:
            schema_fields = [field['name'] for field in self.db_config['database']['schema'] if field and 'name' in field]
            insert_data = [data.get(field, None) for field in schema_fields]
            placeholders = ', '.join(['?' for _ in schema_fields])

            await conn.execute(
                f"INSERT INTO {table} ({', '.join(schema_fields)}) VALUES ({placeholders})",
                insert_data
            )
            await conn.commit()

    async def save_device_reading(self, device_name: str, device_type: str, data: dict):
        """
        Save a reading for a device into its type table, including device_name as a column.
        """
        type_schema = self.schema_config.get(device_type, [])
        schema_fields = [col['name'] for col in type_schema]
        insert_data = [device_name] + [data.get(field, None) for field in schema_fields]
        placeholders = ', '.join(['?' for _ in range(len(insert_data))])
        table_name = device_type
        async with aiosqlite.connect(self.db_config['database']['file']) as conn:
            await conn.execute(
                f"INSERT INTO {table_name} (device_name, {', '.join(schema_fields)}) VALUES ({placeholders})",
                insert_data
            )
            await conn.commit()

    async def save_all_devices(self, readings: dict):
        """
        readings: { device_name: {type: ..., data: {...}} }
        """
        for device_name, info in readings.items():
            await self.save_device_reading(device_name, info['type'], info['data'])

    def get_device_credentials(self, device_name: str):
        device = self.device_config['devices'].get(device_name, {})
        return device.get('username'), device.get('password')

    def get_modbus_device_data(self, device_name: str):
        """
        Read data from a Modbus TCP device using its IP/port from YAML and schema from schema_config.yaml.
        Returns a dict of field_name: value.
        Reads each field individually for robust mapping.
        """
        device = self.device_config['devices'].get(device_name, {})
        ip = device.get('ip')
        port = int(device.get('port', 502))
        unit_id = device.get('unit_id', 1)
        modbus_schema = self.schema_config.get('modbus', {}).get('schema', [])
        data = {}
        if not ip:
            return data
        conf.SIGNED_VALUES = True
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        try:
            sock.connect((ip, port))
            for field in modbus_schema:
                name = field.get('name')
                if name == 'timestamp':
                    continue
                address = field.get('address')
                length = field.get('length', 1)
                scale = field.get('scale', 1)
                fmt = field.get('format', '>I')
                try:
                    request = tcp.read_holding_registers(slave_id=unit_id, starting_address=address, quantity=length)
                    response = tcp.send_message(request, sock)
                    if length == 2 and fmt == '>I':
                        safe_values = [(v & 0xFFFF) for v in response]
                        byte_string = struct.pack('>HH', safe_values[0], safe_values[1])
                        value = struct.unpack(fmt, byte_string)[0]
                        value = value / scale if scale > 1 else value
                        data[name] = value
                    else:
                        data[name] = response[0] / scale if scale > 1 else response[0]
                except Exception as e:
                    data[f"{name}_error"] = str(e)
            data['timestamp'] = datetime.now().isoformat()
        except Exception as e:
            data['error'] = str(e)
        finally:
            sock.close()
        return data

    def save_modbus_device_reading(self, device_name: str, data: dict):
        """
        Save a reading for a Modbus device into the modbus_device_metrics table, including device_name as a column.
        """
        type_schema = self.schema_config.get('modbus', {}).get('schema', [])
        schema_fields = [col['name'] for col in type_schema]
        insert_data = [device_name] + [data.get(field, None) for field in schema_fields]
        placeholders = ', '.join(['?' for _ in range(len(insert_data))])
        table_name = self.schema_config.get('modbus', {}).get('table', 'modbus_device_metrics')
        db_file = self.schema_config.get('modbus', {}).get('file', self.db_config['database']['file'])
        with sqlite3.connect(db_file) as conn:
            conn.execute(
                f"INSERT INTO {table_name} (device_name, {', '.join(schema_fields)}) VALUES ({placeholders})",
                insert_data
            )
            conn.commit()
        
