import os
import asyncio
import yaml
import time
from src.core.config_manager import DeviceTableManager
from src.services.data_service import DataService

if __name__ == "__main__":
    device_yaml_path = os.path.join(os.path.dirname(__file__), "config", "device_config.yaml")
    schema_yaml_path = os.path.join(os.path.dirname(__file__), "config", "schema_config.yaml")
    
    # Create and populate devices table in devices.db
    DeviceTableManager.create_devices_table_from_schema(schema_yaml_path)
    DeviceTableManager.insert_devices_from_yaml_to_devices_db(schema_yaml_path, device_yaml_path)
    
    # Create type tables for different device types
    DeviceTableManager.create_type_tables_from_schema(schema_yaml_path, device_yaml_path)

    # Load schema config
    with open(schema_yaml_path, 'r') as f:
        schema_config = yaml.safe_load(f)

    # Create data service - no specific db_config needed anymore
    data_service = DataService(
        db_config=None,  # Will handle device types dynamically
        device_config_path=device_yaml_path,
        schema_config_path=schema_yaml_path
    )
    # Get devices grouped by type
    devices_by_type = {}
    for device_name, device_info in data_service.device_config['devices'].items():
        device_type = device_info.get('type')
        if device_type not in devices_by_type:
            devices_by_type[device_type] = []
        devices_by_type[device_type].append(device_name)
    
    print("Starting data collection...")
    
    async def main_loop():
        while True:
            # Collect data from all device types
            async def collect_all_data():
                saved_devices = {}
                
                # Handle each device type
                for device_type, device_names in devices_by_type.items():
                    saved_devices[device_type] = False
                    
                    for device_name in device_names:
                        try:
                            device_info = data_service.device_config['devices'][device_name]
                            
                            if device_type == 'SmartPlug':
                                # Handle Tapo devices
                                username, password = data_service.get_device_credentials(device_name)
                                ip = device_info.get('ip')
                                if not ip:
                                    continue
                                
                                data = await asyncio.wait_for(
                                    data_service.get_tapo_device_data(username, password, ip, device_name),
                                    timeout=2.0
                                )
                                await data_service.save_data(data, table_name='tapo_device_metrics')
                                saved_devices[device_type] = True
                                
                            elif device_type == 'schneider':
                                # Handle Schneider devices (Modbus)
                                data = await asyncio.get_event_loop().run_in_executor(
                                    None, data_service.get_modbus_device_data, device_name
                                )
                                data_service.save_schneider_device_reading(device_name, data)
                                saved_devices[device_type] = True
                                
                        except asyncio.TimeoutError:
                            print(f"Timeout connecting to {device_type} device {device_name}")
                        except Exception as e:
                            print(f"Error with {device_type} device {device_name}: {str(e)[:100]}")
                
                return saved_devices
            
            # Run the async function to collect all data
            try:
                saved_devices = await collect_all_data()
                
                # Print messages for each device type that was saved
                for device_type, was_saved in saved_devices.items():
                    if was_saved:
                        device_names = devices_by_type.get(device_type, [])
                        for device_name in device_names:
                            print(f"{device_type} data saved for {device_name}")
            except Exception as e:
                print(f"Error during data collection: {e}")
            
            # Simple 5-second sleep (interruptible with Ctrl+C)
            await asyncio.sleep(5)
    
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("polling process terminated.")