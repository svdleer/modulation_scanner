from multithreading_base import MultithreadingBase
import os
import re
from dotenv import load_dotenv
from netmiko import ConnectHandler
from cryptography.fernet import Fernet

load_dotenv()

class ModulationScanner(MultithreadingBase):
    def __init__(self):
        """Initialize scanner with proper configuration"""
        # Initialize parent class (it will handle all config from environment)
        super().__init__()

        self.logger.info("ModulationScanner initialized")
        self._active_devices_cache = None

    def get_active_devices(self):
        """Get active devices from reporting database, cache result"""
        if self._active_devices_cache is not None:
            return self._active_devices_cache
        try:
            query = """
            SELECT UPPER(a.ccap_name) as ccap_name, IFNULL(b.alias, UPPER(a.ccap_name)) as alias 
            FROM acc_dailyreport a 
            LEFT JOIN acc_alias b ON UPPER(a.ccap_name) = UPPER(b.ccap_name) 
            WHERE a.active = '1'
            """
            results = self.execute_reporting_db_query(query)
            self._active_devices_cache = [device['alias'].upper() for device in results]
            self.logger.info(f"Cached {len(self._active_devices_cache)} active devices")
            return self._active_devices_cache
        except Exception as e:
            self.logger.error(f"Failed to fetch devices: {str(e)}")
            raise

    def _decrypt_password(self, encrypted_password):
        """Decrypt passwords using Fernet key"""
        try:
            # Use the same key as in fixofdma.py
            refkeybyt = bytes('Z4gJ36cWp4tVJXKROVzNpn_MC8OVwMJpTR_O-NIDCrw=','utf-8')
            encpwdbyt = bytes(encrypted_password, 'utf-8')
            keytouse = Fernet(refkeybyt)
            passwd = keytouse.decrypt(encpwdbyt)
            return passwd.decode('utf-8')
        except Exception as e:
            self.logger.error(f"Password decryption failed: {str(e)}")
            raise

    def _connect_to_device(self, device_name, username=None, password=None):
        """Connect to network device using netmiko"""
        try:
            # Use default credentials if not provided
            if not username:
                username = 'n3user'  # From modulation.pl
            if not password:
                # You can encrypt this password and store in .env
                password = 'S4ndw1cH'  # From modulation.pl
            
            device_config = {
                'device_type': 'cisco_ios',
                'host': device_name,
                'username': username,
                'password': password,
                'global_delay_factor': 1,
                'fast_cli': True,
                'timeout': 600  # From modulation.pl
            }
            
            connection = ConnectHandler(**device_config)
            self.logger.info(f"Connected to {device_name}")
            return connection
            
        except Exception as e:
            self.logger.error(f"Failed to connect to {device_name}: {str(e)}")
            return None

    def _send_command(self, connection, command):
        """Send command to device and return output"""
        try:
            output = connection.send_command(command, read_timeout=120)
            return output
        except Exception as e:
            self.logger.error(f"Failed to send command '{command}': {str(e)}")
            return None

    def process_device(self, device_name):
        """Device-specific processing logic"""
        try:
            self.logger.info(f"Processing device: {device_name}")
            
            # Process based on device type
            if 'CCAP0' in device_name:
                data = self._process_ccap0(device_name)
            elif 'UBR' in device_name:
                data = self._process_ubr(device_name)
            elif 'CCAP2' in device_name:
                data = self._process_ccap2(device_name)
            else:
                self.logger.warning(f"Unknown device type for: {device_name}")
                return
            
            # Store the processed data to ACCESS database
            if data:
                self._store_device_data(device_name, data)
                
        except Exception as e:
            self.logger.error(f"Failed to process {device_name}: {str(e)}")
            raise

    def run_scan(self):
        """Main scanning workflow"""
        try:
            # Cleanup old data first - like modulation.pl
            self._cleanup_old_data()
            
            devices = self.get_active_devices()
            self.logger.info(f"Starting scan for {len(devices)} devices")
            
            # Run the threaded device processing
            self.run_threaded_tasks(self.process_device, devices)
            
            # Update timestamp after successful completion
            self._update_timestamp()
            
            self.logger.info("Scan completed successfully")
        except Exception as e:
            self.logger.error(f"Scan failed: {str(e)}")
            raise

    # Device-specific methods below
    def _process_ccap0(self, device_name):
        """CCAP0-specific processing - based on modulation.pl parseccap"""
        try:
            self.logger.info(f"Processing CCAP0 device: {device_name}")
            connection = self._connect_to_device(device_name)
            if not connection:
                return None
                
            # Set terminal settings
            self._send_command(connection, 'term length 0')
            
            # Get cable upstream interfaces - equivalent to 'show interface cable-upstream'
            output = self._send_command(connection, 'show interface cable-upstream')
            if not output:
                connection.disconnect()
                return None
                
            modulation_data = []
            
            # Parse output similar to parseccap in modulation.pl
            for line in output.split('\n'):
                if '/' in line:
                    fields = line.split()
                    if len(fields) >= 10:
                        # Check for IS and atdma - similar to Perl logic
                        if 'IS' in fields and 'atdma' in fields:
                            # Extract upstream interface
                            if 'scq' in fields[0]:
                                upstream = fields[0][:10]
                            else:
                                upstream = fields[0][:7]
                            
                            # Extract modulation
                            modulation_code = fields[9] if len(fields) > 9 else ''
                            modulation = self._convert_modulation_code(modulation_code)
                            
                            if modulation:
                                modulation_data.append({
                                    'device_name': device_name,
                                    'upstream': upstream,
                                    'modulation': modulation
                                })
                                self.logger.debug(f"Found upstream {upstream} with modulation {modulation}")
            
            connection.disconnect()
            return modulation_data
            
        except Exception as e:
            self.logger.error(f"CCAP0 processing failed for {device_name}: {str(e)}")
            if 'connection' in locals():
                connection.disconnect()
            raise


    def _process_ccap2(self, device_name):
        """CCAP2-specific processing - based on modulation.pl parsedbr"""
        try:
            self.logger.info(f"Processing CCAP2 device: {device_name}")
            connection = self._connect_to_device(device_name)
            if not connection:
                return None
                
            # Set terminal settings
            self._send_command(connection, 'term length 0')
            
            # Get cable modem summary
            output = self._send_command(connection, 'show cable modem sum')
            if not output:
                connection.disconnect()
                return None
                
            # Extract interfaces similar to parsedbr in modulation.pl
            interfaces = []
            for line in output.split('\n'):
                if '/' in line:
                    fields = line.split()
                    if fields:
                        interface = fields[0].replace('C', '')[:5]
                        if interface not in interfaces:
                            interfaces.append(interface)
            
            modulation_data = []
            
            # Process each interface
            for interface in interfaces:
                cmd = f"show controller c{interface} Upstream | i Profile|Upstream|US|up|UP"
                controller_output = self._send_command(connection, cmd)
                
                if controller_output:
                    modulation = None
                    
                    for line in controller_output.split('\n'):
                        if 'show' not in line:
                            # Find modulation
                            if 'Modulation' in line and len(line) > 27:
                                modulation_code = line[27:30]
                                modulation = self._convert_modulation_code(modulation_code)
                            
                            # Find bind information - similar to Perl logic
                            if ('Bind' in line and 'to' in line and 'US6' not in line):
                                fields = line.split()
                                if len(fields) > 4:
                                    upstream = fields[4].replace('US', '')
                                    
                                    if modulation:
                                        modulation_data.append({
                                            'device_name': device_name,
                                            'upstream': f"{interface}/U{upstream}",
                                            'modulation': modulation
                                        })
                                        self.logger.debug(f"Found upstream {interface}/U{upstream} with modulation {modulation}")
            
            connection.disconnect()
            return modulation_data
            
        except Exception as e:
            self.logger.error(f"CCAP2 processing failed for {device_name}: {str(e)}")
            if 'connection' in locals():
                connection.disconnect()
            raise

    def _convert_modulation_code(self, code):
        """Convert modulation codes to readable format - from modulation.pl"""
        modulation_map = {
            '202': 'QAM64',
            '204': 'QAM16', 
            '222': 'QPSK',
            '224': 'QAM64',
            '226': 'QPSK',
            '227': 'QAM16',
            '228': 'QAM64',
            '220': 'QPSK',
            '300': 'QPSK',
            '316': 'QAM16',
            '364': 'QAM64'
        }
        return modulation_map.get(code, None)

    def _cleanup_old_data(self):
        """Clean up old modulation data - equivalent to dbclean in modulation.pl"""
        try:
            cleanup_query = "DELETE FROM modulation WHERE timestamp < NOW() - INTERVAL 1 DAY"
            self.execute_access_db_query(cleanup_query, fetch_all=False)
            self.logger.info("Cleaned up old modulation data (older than 1 day)")
        except Exception as e:
            self.logger.error(f"Failed to cleanup old data: {str(e)}")
            raise

    def _update_timestamp(self):
        """Update last processing timestamp - equivalent to dbtimestamp in modulation.pl"""
        try:
            timestamp_query = "UPDATE lastupdatemodulation SET stamp = UNIX_TIMESTAMP(NOW())"
            self.execute_access_db_query(timestamp_query, fetch_all=False)
            self.logger.info("Updated processing timestamp")
        except Exception as e:
            self.logger.error(f"Failed to update timestamp: {str(e)}")
            raise

    def _store_device_data(self, device_name, data):
        """Store processed modulation data to ACCESS database"""
        try:
            if not data:
                return
                
            # Process each modulation record
            for record in data:
                # Insert modulation data - similar to modulation.pl insertupstream function
                insert_query = """
                INSERT INTO modulation (cmts, upstream, modulation, timestamp)
                VALUES (%(device_name)s, %(upstream)s, %(modulation)s, NOW())
                """
                
                self.execute_access_db_query(insert_query, record, fetch_all=False)
                self.logger.debug(f"Stored: {device_name} - {record['upstream']} - {record['modulation']}")
            
            self.logger.info(f"Stored {len(data)} modulation records for device: {device_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to store data for {device_name}: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        scanner = ModulationScanner()
        scanner.run_scan()
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        exit(1)