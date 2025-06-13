import asyncio
from src.core.auth_handler import AuthHandler, AuthenticationError
from src.core.config_manager import ConfigManager
from src.services.data_service import DataService

async def authenticate_and_get_data(service, device_ip):
    """Handle authentication with retry logic and get device data"""
    try:
        # Use the retry mechanism from AuthHandler
        for email, password in AuthHandler.get_credentials_with_retry():
            try:
                data = await service.get_device_data(email, password, device_ip)
                return data, email, password  # Return successful credentials for reuse
            except Exception as e:
                error_str = str(e).lower()
                if "invalidcredentials" in error_str or "invalid credentials" in error_str:
                    print(f"Authentication failed: {e}")
                    continue  # Try next attempt
                else:
                    # Non-authentication error, re-raise
                    raise e
    except AuthenticationError as e:
        print(f"Authentication failed after 3 attempts: {e}")
        raise e

async def main():
    device_config = ConfigManager.load_config("device")
    db_config = ConfigManager.load_config("schema")
    service = DataService(db_config)
    
    # Initial authentication with retry logic
    try:
        data, email, password = await authenticate_and_get_data(service, device_config['device']['ip'])
        await service.save_data(data)
        print(f"Data saved at {data['timestamp']}")
    except AuthenticationError:
        print("Failed to authenticate. Exiting.")
        return
    
    # Main data collection loop
    while True:
        try:
            await asyncio.sleep(5)  # 5 second interval between readings
            data = await service.get_device_data(email, password, device_config['device']['ip'])
            await service.save_data(data)
            print(f"Data saved at {data['timestamp']}")
            
        except Exception as e:
            error_str = str(e).lower()
            
            # Check for TAPO authentication error patterns
            if "invalidcredentials" in error_str or "invalid credentials" in error_str:
                print("Authentication error detected. Re-authenticating...")
                try:
                    # Re-authenticate with retry logic
                    data, email, password = await authenticate_and_get_data(service, device_config['device']['ip'])
                    await service.save_data(data)
                    print(f"Re-authentication successful. Data saved at {data['timestamp']}")
                except AuthenticationError:
                    print("Re-authentication failed after 3 attempts. Exiting.")
                    break
            else:
                print(f"Error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
