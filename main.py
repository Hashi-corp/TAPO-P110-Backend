import asyncio
from src.core.auth_handler import AuthHandler
from src.core.config_manager import ConfigManager
from src.services.data_service import DataService

async def main():
    device_config = ConfigManager.load_config("device")
    db_config = ConfigManager.load_config("schema")
    service = DataService(db_config)
    
    email, password = AuthHandler.get_credentials()
    
    while True:
        try:
            data = await service.get_device_data(email, password, device_config['device']['ip'])
            service.save_data(data)
            print(f"Data saved at {data['timestamp']}")
            await asyncio.sleep(5)  # 5 second interval between readings
            
        except Exception as e:
            error_str = str(e).lower()
            
            # Check for TAPO authentication error patterns
            if "invalidcredentials" in error_str or "invalid credentials" in error_str:
                print("Authentication error. Please check credentials.")
                email, password = AuthHandler.get_credentials()  # Re-prompt immediately
            else:
                print(f"Error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())