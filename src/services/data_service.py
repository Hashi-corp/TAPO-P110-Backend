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
        if db_config:  # Only create table if db_config is provided
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

    
    async def get_tapo_device_data(self, email: str, password: str, ip: str, device_name: str = None):
        device = await ApiClient(email, password).p110(ip)
        device_info = await device.get_device_info()
        energy_usage = await device.get_energy_usage()
        data = {}
        
        # Add device_name if provided
        if device_name:
            data['device_name'] = device_name
        
        # Get SmartPlug schema
        smartplug_config = self.schema_config.get('SmartPlug', {})
        schema_fields = smartplug_config.get('schema', [])
        
        for field in schema_fields:
            if not field or 'name' not in field:
                continue
            field_name = field['name']
            
            # Skip device_name as we've already added it
            if field_name == 'device_name':
                continue
                
            # Try to get from device_info, then energy_usage, else None
            value = getattr(device_info, field_name, None)
            if value is None:
                value = getattr(energy_usage, field_name, None)
            if field_name == 'timestamp':
                value = datetime.now().isoformat()
            data[field_name] = value
        return data
    
    async def save_data(self, data: dict, table_name: str = None):
        # Use SmartPlug configuration for Tapo devices
        smartplug_config = self.schema_config.get('SmartPlug', {})
        table = table_name if table_name else smartplug_config.get('table', 'tapo_device_metrics')
        db_file = smartplug_config.get('file', 'tapo_data.db')
        
        async with aiosqlite.connect(db_file) as conn:
            schema_fields = [field['name'] for field in smartplug_config.get('schema', []) if field and 'name' in field]
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
        device_type = device.get('type', 'modbus')  # Get device type
        ip = device.get('ip')
        port = int(device.get('port', 502))
        slave_id = int(device.get('slave_id', 6))  # Convert to int in case it's stored as string
        
        # Get schema based on device type, fallback to modbus
        device_schema = self.schema_config.get(device_type, {}).get('schema', [])
        if not device_schema:
            device_schema = self.schema_config.get('modbus', {}).get('schema', [])
            
        data = {}
        
        # Add device_name to the data
        data['device_name'] = device_name
        
        if not ip:
            return data
            
        conf.SIGNED_VALUES = True
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1.0) 
        try:
            sock.connect((ip, port))
            for field in device_schema:
                name = field.get('name')
                # Skip device_name as we've already added it
                if name == 'device_name':
                    continue
                    
                # Handle timestamp specially
                if name == 'timestamp':
                    data['timestamp'] = datetime.now().isoformat()
                    continue
                
                address = field.get('address')
                length = field.get('length', 1)
                scale = field.get('scale', 1)
                fmt = field.get('format', '>I')
                
                try:
                    request = tcp.read_holding_registers(slave_id=slave_id, starting_address=address, quantity=length)
                    response = tcp.send_message(request, sock)
                    
                    if length == 2 and fmt == '>f':
                        safe_values = [(v & 0xFFFF) for v in response]
                        byte_string = struct.pack('>HH', safe_values[0], safe_values[1])
                        value = struct.unpack(fmt, byte_string)[0]
                        value = value / scale if scale > 1 else value
                        data[name] = value
                    else:
                        data[name] = response[0] / scale if scale > 1 else response[0]
                except Exception as e:
                    data[f"{name}_error"] = str(e)
                    
            # Make sure timestamp is always set
            if 'timestamp' not in data:
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
        modbus_config = self.schema_config.get('modbus', {})
        
        # Filter out the device_name field from schema_fields if it exists
        schema_fields = [col['name'] for col in modbus_config.get('schema', []) 
                        if col['name'] != 'device_name']
        
        # Make sure timestamp is included in the schema fields
        if 'timestamp' not in schema_fields:
            schema_fields.append('timestamp')
        
        # Ensure timestamp is set
        if 'timestamp' not in data or data['timestamp'] is None:
            data['timestamp'] = datetime.now().isoformat()
        
        # Make sure device_name is set in the data
        if 'device_name' not in data:
            data['device_name'] = device_name
        
        # Prepare data for insertion
        values = [data['device_name']]
        for field in schema_fields:
            values.append(data.get(field))
        
        placeholders = ', '.join(['?' for _ in range(len(values))])
        table_name = modbus_config.get('table', 'modbus_device_metrics')
        db_file = modbus_config.get('file', self.db_config['database']['file'])
        
        with sqlite3.connect(db_file) as conn:
            try:
                query = f"INSERT INTO {table_name} (device_name, {', '.join(schema_fields)}) VALUES ({placeholders})"
                conn.execute(query, values)
                conn.commit()
            except Exception as e:
                # Create the table if it doesn't exist
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        device_name TEXT,
                        {', '.join([f"{field} {col['type'] if 'type' in col else 'TEXT'}" for field, col in zip(schema_fields, modbus_config.get('schema', []))])}
                    )
                """)
                conn.commit()
                # Try again
                conn.execute(
                    f"INSERT INTO {table_name} (device_name, {', '.join(schema_fields)}) VALUES ({placeholders})",
                    values
                )
                conn.commit()

    def save_powersupply_device_reading(self, device_name: str, data: dict):
        """
        Save a reading for a PowerSupply device into the powersupply_device_metrics table.
        """
        powersupply_config = self.schema_config.get('PowerSupply', {})
        
        # Filter out the device_name field from schema_fields if it exists
        schema_fields = [col['name'] for col in powersupply_config.get('schema', []) 
                        if col['name'] != 'device_name']
        
        # Make sure timestamp is included in the schema fields
        if 'timestamp' not in schema_fields:
            schema_fields.append('timestamp')
        
        # Ensure timestamp is set
        if 'timestamp' not in data or data['timestamp'] is None:
            data['timestamp'] = datetime.now().isoformat()
        
        # Make sure device_name is set in the data
        if 'device_name' not in data:
            data['device_name'] = device_name
        
        # Prepare data for insertion
        values = [data['device_name']]
        for field in schema_fields:
            values.append(data.get(field))
        
        placeholders = ', '.join(['?' for _ in range(len(values))])
        table_name = powersupply_config.get('table', 'powersupply_device_metrics')
        db_file = powersupply_config.get('file', 'powersupply_data.db')
        
        with sqlite3.connect(db_file) as conn:
            try:
                query = f"INSERT INTO {table_name} (device_name, {', '.join(schema_fields)}) VALUES ({placeholders})"
                conn.execute(query, values)
                conn.commit()
            except Exception as e:
                # Create the table if it doesn't exist
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        device_name TEXT,
                        {', '.join([f"{field} {col['type'] if 'type' in col else 'TEXT'}" for field, col in zip(schema_fields, powersupply_config.get('schema', []))])}
                    )
                """)
                conn.commit()
                # Try again
                conn.execute(
                    f"INSERT INTO {table_name} (device_name, {', '.join(schema_fields)}) VALUES ({placeholders})",
                    values
                )
                conn.commit()

    def save_schneider_device_reading(self, device_name: str, data: dict):
        """
        Save a reading for a Schneider device into the schneider_device_metrics table.
        """
        schneider_config = self.schema_config.get('schneider', {})
        
        # Filter out the device_name field from schema_fields if it exists
        schema_fields = [col['name'] for col in schneider_config.get('schema', []) 
                        if col['name'] != 'device_name']
        
        # Make sure timestamp is included in the schema fields
        if 'timestamp' not in schema_fields:
            schema_fields.append('timestamp')
        
        # Ensure timestamp is set
        if 'timestamp' not in data or data['timestamp'] is None:
            data['timestamp'] = datetime.now().isoformat()
        
        # Make sure device_name is set in the data
        if 'device_name' not in data:
            data['device_name'] = device_name
        
        # Prepare data for insertion
        values = [data['device_name']]
        for field in schema_fields:
            values.append(data.get(field))
        
        placeholders = ', '.join(['?' for _ in range(len(values))])
        table_name = schneider_config.get('table', 'schneider_device_metrics')
        db_file = schneider_config.get('file', 'schneider_data.db')
        
        with sqlite3.connect(db_file) as conn:
            try:
                query = f"INSERT INTO {table_name} (device_name, {', '.join(schema_fields)}) VALUES ({placeholders})"
                conn.execute(query, values)
                conn.commit()
            except Exception as e:
                # Create the table if it doesn't exist
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        device_name TEXT,
                        {', '.join([f"{field} {col['type'] if 'type' in col else 'TEXT'}" for field, col in zip(schema_fields, schneider_config.get('schema', []))])}
                    )
                """)
                conn.commit()
                # Try again
                conn.execute(
                    f"INSERT INTO {table_name} (device_name, {', '.join(schema_fields)}) VALUES ({placeholders})",
                    values
                )
                conn.commit()