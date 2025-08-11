from multithreading_base import MultithreadingBase
import os
from dotenv import load_dotenv

load_dotenv()

class ModulationScanner(MultithreadingBase):
    def __init__(self):
        """Initialize scanner with proper configuration"""
        # Prepare configuration for both databases
        self.config = {
            'ACCESS': {  # destination database
                'HOST': os.getenv('ACCESS_HOST'),
                'DATABASE': os.getenv('ACCESS_DATABASE'),
                'USER': os.getenv('ACCESS_USER'),
                'PASSWORD': os.getenv('ACCESS_PASSWORD'),
                'POOL_SIZE': int(os.getenv('ACCESS_POOL_SIZE', 10))
            },
            'REPORTING': {  # source database
                'HOST': os.getenv('REPORTING_HOST'),
                'DATABASE': os.getenv('REPORTING_DATABASE'),
                'USER': os.getenv('REPORTING_USER'),
                'PASSWORD': os.getenv('REPORTING_PASSWORD'),
                'POOL_SIZE': int(os.getenv('REPORTING_POOL_SIZE', 5))
            }
        }
        print("ACCESS_HOST:", os.getenv('ACCESS_HOST'))
        # Initialize parent class
        super().__init__(config=self.config)

        self.logger.info("ModulationScanner initialized")
        self._active_devices_cache = None

    def get_active_devices(self):
        """Get active devices from reporting database, cache result"""
        if self._active_devices_cache is not None:
            return self._active_devices_cache
        try:
            query = """
            SELECT UPPER(a.ccap_name), IFNULL(b.alias, UPPER(a.ccap_name)) as alias 
            FROM acc_dailyreport a 
            LEFT JOIN acc_alias b ON UPPER(a.ccap_name) = UPPER(b.ccap_name) 
            WHERE a.active = '1'
            """
            results = self.execute_reporting_db_query(query)
            self._active_devices_cache = [device['alias'].upper() for device in results]
            return self._active_devices_cache
        except Exception as e:
            self.logger.error(f"Failed to fetch devices: {str(e)}")
            raise

    def process_device(self, device_name):
        """Device-specific processing logic"""
        try:
            self.logger.info(f"Processing device: {device_name}")
            # Add your device processing logic here
            # Example:
            if 'CCAP0' in device_name:
                self._process_ccap0(device_name)
            elif 'UBR' in device_name:
                self._process_ubr(device_name)
        except Exception as e:
            self.logger.error(f"Failed to process {device_name}: {str(e)}")
            raise

    def run_scan(self):
        """Main scanning workflow"""
        try:
            devices = self.get_active_devices()
            self.logger.info(f"Starting scan for {len(devices)} devices")
            self.run_threaded_tasks(self.process_device, devices)
            self.logger.info("Scan completed successfully")
        except Exception as e:
            self.logger.error(f"Scan failed: {str(e)}")
            raise

    # Device-specific methods below
    def _process_ccap0(self, device_name):
        """CCAP0-specific processing"""
        # Implement your CCAP0 logic here
        pass

    def _process_ubr(self, device_name):
        """UBR-specific processing"""
        # Implement your UBR logic here
        pass

if __name__ == "__main__":
    try:
        scanner = ModulationScanner()
        scanner.run_scan()
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        exit(1)