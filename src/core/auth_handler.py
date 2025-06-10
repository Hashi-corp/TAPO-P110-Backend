from getpass import getpass
import os

class AuthenticationError(Exception):
    pass

class AuthHandler:
    @staticmethod
    def get_credentials():
        email = os.getenv('TAPO_EMAIL') or input("Tapo Email: ")
        password = os.getenv('TAPO_PASSWORD') or getpass("Tapo Password: ")
        return email, password
    
    @staticmethod
    def get_credentials_with_retry():
        """Get credentials with retry on wrong credentials"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            if attempt == 0:
                email, password = AuthHandler.get_credentials()
            else:
                print(f"Wrong credentials. Try again ({attempt + 1}/{max_attempts})")
                email = input("Tapo Email: ")
                password = getpass("Tapo Password: ")
            
            # Return credentials for testing
            yield email, password
        
        # If we get here, all attempts failed
        raise AuthenticationError("Authentication failed after 3 attempts")