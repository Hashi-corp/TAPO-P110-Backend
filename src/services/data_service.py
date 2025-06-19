import asyncio
import sqlite3
import aiosqlite
from tapo import ApiClient
from datetime import datetime
import time

class DataService:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn = sqlite3.connect(db_config['database']['file'])
        self.create_table(db_config)
    
    def create_table(self, config: dict):
        # Keep all columns from schema, even if some fields are removed from data collection
        columns = []
        for col in config['database']['schema']:
            if col and 'name' in col and 'type' in col:
                columns.append(f"{col['name']} {col['type']}")
        
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {config['database']['table']} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {', '.join(columns)}
            )
        """)

    async def get_device_data(self, email: str, password: str, ip: str):
        device = await ApiClient(email, password).p110(ip)
        device_info = await device.get_device_info()
        energy_usage = await device.get_energy_usage()
        
        # Build data dict based on YAML schema
        data = {}
        
        for field in self.db_config['database']['schema']:
            # Skip None/empty fields
            if not field or 'name' not in field:
                continue
                
            field_name = field['name']
            field_source = field.get('source', field_name)  # Use source mapping or field name
            
            # Map data based on source
            if field_source == 'device_on':
                data[field_name] = int(device_info.device_on)
            elif field_source == 'current_power':
                data[field_name] = energy_usage.current_power
            elif field_source == 'today_energy':
                data[field_name] = energy_usage.today_energy
            elif field_source == 'month_energy':
                data[field_name] = energy_usage.month_energy
            elif field_source == 'nickname':
                data[field_name] = device_info.nickname
            elif field_source == 'signal_level':
                data[field_name] = device_info.signal_level
            elif field_source == 'timestamp':
                data[field_name] = datetime.now().isoformat()
            else:
                # Try to get from device_info first, then energy_usage
                value = getattr(device_info, field_source, None)
                if value is None:
                    value = getattr(energy_usage, field_source, None)
                data[field_name] = value
        
        return data

    async def save_data(self, data: dict):
        async with aiosqlite.connect(self.db_config['database']['file']) as conn:
            schema_fields = [field['name'] for field in self.db_config['database']['schema'] if field and 'name' in field]
            insert_data = [data.get(field, None) for field in schema_fields]
            placeholders = ', '.join(['?' for _ in schema_fields])

            await conn.execute(
                f"INSERT INTO {self.db_config['database']['table']} ({', '.join(schema_fields)}) VALUES ({placeholders})",
                insert_data
            )
            await conn.commit()