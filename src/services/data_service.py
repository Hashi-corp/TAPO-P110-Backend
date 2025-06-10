import asyncio
import sqlite3
from tapo import ApiClient
from datetime import datetime
import time

class DataService:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn = sqlite3.connect(db_config['database']['file'])
        self.table_initialized = False
        self._create_base_table(db_config)
    
    def _create_base_table(self, config: dict):
        """Create the basic table structure with just the ID column"""
        table_name = config['database']['table']
        
        # Only create basic table if it doesn't exist
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT
            )
        """)
        self.conn.commit()
    
    def _ensure_table_schema(self, config: dict):
        """Update table schema to match YAML config - called only when saving data"""
        if self.table_initialized:
            return
            
        table_name = config['database']['table']
        
        # Get existing columns
        cursor = self.conn.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {row[1]: row[2] for row in cursor.fetchall()}  # {column_name: column_type}
        
        # Add new columns from schema
        for col in config['database']['schema']:
            if col and 'name' in col and 'type' in col:
                col_name = col['name']
                col_type = col['type']
                
                # If column doesn't exist, add it
                if col_name not in existing_columns:
                    try:
                        self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
                        print(f"Added new column: {col_name} ({col_type})")
                    except sqlite3.OperationalError as e:
                        print(f"Warning: Could not add column {col_name}: {e}")
                elif existing_columns[col_name].upper() != col_type.upper():
                    # Column exists but type might be different - log warning
                    print(f"Warning: Column {col_name} exists with type {existing_columns[col_name]} but schema specifies {col_type}")
        
        self.conn.commit()
        self.table_initialized = True

    def _get_table_columns(self, table_name: str) -> list:
        """Get list of existing column names in the table"""
        cursor = self.conn.execute(f"PRAGMA table_info({table_name})")
        return [row[1] for row in cursor.fetchall()]

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
            try:
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
                elif field_source == 'rssi':
                    data[field_name] = device_info.rssi
                elif field_source == 'fw_ver':
                    data[field_name] = device_info.fw_ver
                elif field_source == 'timestamp':
                    data[field_name] = datetime.now().isoformat()
                else:
                    # Try to get from device_info first, then energy_usage
                    value = getattr(device_info, field_source, None)
                    if value is None:
                        value = getattr(energy_usage, field_source, None)
                    data[field_name] = value
            except AttributeError as e:
                print(f"Warning: Could not get value for {field_name} from {field_source}: {e}")
                data[field_name] = None
        
        return data

    def save_data(self, data: dict):
        # Ensure table schema is up to date before saving
        self._ensure_table_schema(self.db_config)
        
        table_name = self.db_config['database']['table']
        
        # Get current table columns (excluding 'id' which is auto-increment)
        existing_columns = self._get_table_columns(table_name)
        existing_columns = [col for col in existing_columns if col != 'id']
        
        # Get schema fields that actually exist in the table
        schema_fields = []
        for field in self.db_config['database']['schema']:
            if field and 'name' in field and field['name'] in existing_columns:
                schema_fields.append(field['name'])
        
        # Prepare data with NULL for missing fields
        insert_data = []
        for field in schema_fields:
            insert_data.append(data.get(field, None))  # None becomes NULL in SQLite   
        
        if schema_fields:  # Only insert if we have fields to insert
            placeholders = ', '.join(['?' for _ in schema_fields])
            
            try:
                self.conn.execute(
                    f"INSERT INTO {table_name} ({', '.join(schema_fields)}) VALUES ({placeholders})",
                    insert_data
                )
                self.conn.commit()
            except sqlite3.Error as e:
                print(f"Error inserting data: {e}")
                # Optionally re-raise or handle the error as needed
                raise
        else:
            print("Warning: No valid schema fields found for insertion")

    def close_connection(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()

    def __del__(self):
        """Ensure connection is closed when object is destroyed"""
        self.close_connection()