# TAPO P110 & Schneider Electric Powermeter Data Pipeline

This project is a Python-based data pipeline that collects and stores data from TP-Link TAPO P110 smart plugs and Schneider Electric power meters. It functions as an ETL (Extract, Transform, Load) service: it extracts data from the devices, performs necessary transformations, and loads the results into a local SQLite database.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Python 3.7+
- Pip for installing Python packages

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/TAPO-P110-Backend.git
   cd TAPO-P110-Backend
   ```

2. **Create a virtual environment (recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install the required packages:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. **Configure your devices:**
   - Edit `config/device_config.yaml` to add your TAPO and Schneider device details. You will need to provide IP addresses, usernames, and passwords.
   - The `config/schema_config.yaml` defines the database schema for storing device data. You can modify it to add or remove fields.

2. **Run the application:**
   ```bash
   python main.py
   ```
   The application will start polling the devices every 5 seconds and save the data to the `tapo_device_metrics` table in the SQLite database.

## Project Structure

```
.
├── config/
│   ├── device_config.yaml  # Device configurations
│   └── schema_config.yaml  # Database schema definition
├── src/
│   ├── core/
│   │   ├── auth_handler.py     # Handles device authentication
│   │   └── config_manager.py   # Manages device and schema configurations
│   └── services/
│       └── data_service.py     # Service for fetching data from devices
├── main.py                     # Main application entry point
├── requirements.txt            # Project dependencies
└── README.md                   # This file
```

## Dependencies

The project uses the following Python libraries:

- `aiosqlite`
- `pyserial`
- `python-dotenv`
- `pyyaml`
- `ruff`
- `tapo`
- `typing-extensions`
- `umodbus`

These can be installed by running `pip install -r requirements.txt`.
