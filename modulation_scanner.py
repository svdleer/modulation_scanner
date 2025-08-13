#!/home/svdleer/python/venv/bin/python3

from multithreading_base import MultithreadingBase
import os
import re
import requests
import time
import fcntl
import sys
import signal
import atexit
from datetime import datetime, timedelta
from dotenv import load_dotenv
from netmiko import ConnectHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

class ModulationScanner(MultithreadingBase):
    def __init__(self):
        """Initialize scanner with proper configuration"""
        # Initialize parent class (it will handle all config from environment)
        super().__init__()

        self.logger.info("ModulationScanner initialized")
        self._active_devices_cache = None
        
        # Process management - Use /tmp to avoid NFS locking issues
        self.pid_file = "/tmp/modulation_scanner.pid"
        self.lock_file = None
        self.is_locked = False
        
        # Watchdog settings - Use /tmp to avoid NFS locking issues
        self.watchdog_file = "/tmp/modulation_scanner.status"
        self.last_successful_scan = None
        self.last_heartbeat = time.time()
        
        # Statistics tracking
        self.scan_statistics = {
            'total_scans': 0,
            'successful_scans': 0,
            'failed_scans': 0,
            'total_devices_processed': 0,
            'total_records_stored': 0,
            'average_scan_time': 0,
            'last_scan_time': 0,
            'started_at': datetime.now().isoformat(),
            'uptime_seconds': 0
        }
        
        # Per-device scheduling - track when each device was last scanned
        self.device_last_scan = {}
        self.device_scan_interval = 600  # 10 minutes in seconds
        
        # Debug logging configuration from environment variables
        self.debug_ccap0 = os.getenv('DEBUG_CCAP0', 'false').lower() == 'true'
        self.debug_ccap1 = os.getenv('DEBUG_CCAP1', 'false').lower() == 'true'
        self.debug_ccap2 = os.getenv('DEBUG_CCAP2', 'false').lower() == 'true'
        
        if self.debug_ccap0 or self.debug_ccap1 or self.debug_ccap2:
            self.logger.info(f"Debug logging enabled: CCAP0={self.debug_ccap0}, CCAP1={self.debug_ccap1}, CCAP2={self.debug_ccap2}")

    def _acquire_lock(self):
        """Acquire exclusive lock to prevent multiple instances"""
        try:
            # Create/open lock file
            self.lock_file = open(self.pid_file, 'w')
            
            # Try to acquire exclusive lock (non-blocking)
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # Write current PID to file
            pid = os.getpid()
            self.lock_file.write(str(pid))
            self.lock_file.flush()
            
            self.is_locked = True
            self.logger.info(f"Process lock acquired successfully (PID: {pid})")
            
            # Register cleanup handler
            atexit.register(self._release_lock)
            
            return True
            
        except IOError:
            # Lock is already held by another process
            if self.lock_file:
                self.lock_file.close()
                self.lock_file = None
            
            # Check if existing process is still running
            existing_pid = self._get_existing_pid()
            if existing_pid:
                if self._is_process_running(existing_pid):
                    self.logger.error(f"Another instance is already running (PID: {existing_pid})")
                    return False
                else:
                    self.logger.warning(f"Found stale PID file (PID: {existing_pid} not running), removing...")
                    self._cleanup_stale_lock()
                    return self._acquire_lock()  # Try again
            else:
                self.logger.error("Failed to acquire process lock")
                return False
        except Exception as e:
            self.logger.error(f"Error acquiring lock: {str(e)}")
            return False

    def _release_lock(self):
        """Release the process lock"""
        try:
            if self.is_locked and self.lock_file:
                fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
                self.lock_file.close()
                self.lock_file = None
                self.is_locked = False
                
                # Remove PID file
                if os.path.exists(self.pid_file):
                    os.remove(self.pid_file)
                
                self.logger.info("Process lock released")
        except Exception as e:
            self.logger.error(f"Error releasing lock: {str(e)}")

    def _get_existing_pid(self):
        """Get PID from existing lock file"""
        try:
            if os.path.exists(self.pid_file):
                with open(self.pid_file, 'r') as f:
                    pid_content = f.read().strip()
                    if pid_content:  # Check if content is not empty
                        return int(pid_content)
                    else:
                        self.logger.warning("PID file exists but is empty")
                        return None
        except (ValueError, IOError) as e:
            self.logger.warning(f"Error reading PID file: {str(e)}")
        return None

    def _is_process_running(self, pid):
        """Check if process with given PID is still running"""
        try:
            os.kill(pid, 0)  # Send signal 0 to check if process exists
            return True
        except OSError:
            return False

    def _cleanup_stale_lock(self):
        """Remove stale lock file"""
        try:
            if os.path.exists(self.pid_file):
                os.remove(self.pid_file)
        except OSError:
            pass

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            signal_name = {
                signal.SIGINT: 'SIGINT',
                signal.SIGTERM: 'SIGTERM'
            }.get(signum, f'Signal {signum}')
            
            self.logger.info(f"Received {signal_name}, initiating graceful shutdown...")
            self._update_watchdog("shutting_down", f"Received {signal_name}, stopping scanner")
            self._release_lock()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        self.logger.info("Signal handlers configured for graceful shutdown")

    def get_devices_ready_for_scan(self):
        """Get devices that haven't been scanned in the last 10 minutes"""
        all_devices = self.get_active_devices()
        current_time = time.time()
        ready_devices = []
        
        for device in all_devices:
            last_scan_time = self.device_last_scan.get(device, 0)
            time_since_scan = current_time - last_scan_time
            
            if time_since_scan >= self.device_scan_interval:
                ready_devices.append(device)
                self.logger.debug(f"Device {device} ready for scan (last scan: {time_since_scan:.1f}s ago)")
            else:
                next_scan_in = self.device_scan_interval - time_since_scan
                self.logger.debug(f"Device {device} not ready (next scan in {next_scan_in:.1f}s)")
        
        return ready_devices

    def get_active_devices(self):
        """Get active devices from ISW API, cache result"""
        if self._active_devices_cache is not None:
            return self._active_devices_cache
        
        try:
            # ISW API configuration
            api_url = "https://appdb.oss.local/isw/api/search?type=hostname&q=%2A"
            headers = {
                'accept': 'application/json',
                'Authorization': 'Basic aXN3OlNweWVtX090R2hlYjQ='
            }
            
            # Make API request
            self.logger.info("Fetching devices from ISW API...")
            response = requests.get(api_url, headers=headers, verify=False)  # verify=False for self-signed certs
            response.raise_for_status()
            
            api_data = response.json()
            
            if api_data.get('status') != 200:
                raise Exception(f"API returned status: {api_data.get('status')}")
            
            # Extract device hostnames and filter for CCAP devices
            devices = []
            for device in api_data.get('data', []):
                hostname = device.get('HostName', '')
                # Use regex to match CCAP0xx, CCAP1xx, CCAP2xx patterns
                if re.search(r'CCAP[012]\d{2}', hostname.upper()):
                    devices.append(hostname.upper())
            
            self._active_devices_cache = devices
            self.logger.info(f"Cached {len(self._active_devices_cache)} CCAP devices from ISW API")
            return self._active_devices_cache
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch devices from ISW API: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to process ISW API response: {str(e)}")
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
                'global_delay_factor': 0.5,  # Reduced from 1 for faster operations
                'fast_cli': True,
                'timeout': 300,  # Reduced from 600 for faster timeouts
                'session_timeout': 300,  # Add session timeout
                'keepalive': 30  # Add keepalive for connection stability
            }
            
            connection = ConnectHandler(**device_config)
            self.logger.info(f"Connected to {device_name}")
            return connection
            
        except Exception as e:
            self.logger.error(f"Failed to connect to {device_name}: {str(e)}")
            return None

    def _send_command(self, connection, command, device_name=None):
        """Send command to device and return output"""
        try:
            # Special handling for CCAP1 devices - send page-off first
            if device_name and "CCAP1" in device_name:
                connection.send_command('page-off', read_timeout=5)  # Reduced timeout
                
            output = connection.send_command(command, read_timeout=120)  # Reduced from 300
            return output
        except Exception as e:
            self.logger.error(f"Failed to send command '{command}' on {device_name}: {str(e)}")
            return None

    def process_device(self, device_name):
        """Device-specific processing logic"""
        try:
            self.logger.info(f"Processing device: {device_name}")
            scan_start_time = time.time()
            
            # Update heartbeat before device processing (in case it takes a while)
            # Note: Only update last_heartbeat timestamp, don't write full watchdog file
            # to avoid interfering with main thread's watchdog updates
            self.last_heartbeat = time.time()
            
            # Process based on device type using hostname patterns
            # Check for CCAP0xx pattern (CCAP001, CCAP002, etc.)
            if re.search(r'CCAP0\d{2}', device_name):
                self.logger.info(f"Routing {device_name} to CCAP0 processing")
                data = self._process_ccap0(device_name)
            # Check for CCAP1xx pattern (CCAP100, CCAP101, etc.)
            elif re.search(r'CCAP1\d{2}', device_name):
                self.logger.info(f"Routing {device_name} to CCAP1 processing")
                data = self._process_ccap1(device_name)
            # Check for CCAP2xx pattern (CCAP200, CCAP201, CCAP202, etc.)
            elif re.search(r'CCAP2\d{2}', device_name):
                self.logger.info(f"Routing {device_name} to CCAP2 processing")
                data = self._process_ccap2(device_name)
            else:
                self.logger.warning(f"Unknown device type for: {device_name}")
                return []
            
            # Update heartbeat after device processing completes
            self.last_heartbeat = time.time()
            
            # Store the processed data to ACCESS database
            if data:
                self.logger.info(f"Storing {len(data)} records for {device_name}")
                self._store_device_data(device_name, data)
                
            # Update device scan timestamp after successful processing
            self.device_last_scan[device_name] = time.time()
            
            return data if data else []
                
        except Exception as e:
            self.logger.error(f"Failed to process {device_name}: {str(e)}")
            return []

    def _update_watchdog(self, status="running", message="", scan_stats=None):
        """Update watchdog status file for external monitoring"""
        try:
            import json
            import os  # Move import to the top
            
            current_time = time.time()
            self.last_heartbeat = current_time
            
            # Calculate uptime more reliably
            start_time = datetime.fromisoformat(self.scan_statistics['started_at'])
            uptime_seconds = (datetime.now() - start_time).total_seconds()
            self.scan_statistics['uptime_seconds'] = int(uptime_seconds)
            
            watchdog_data = {
                'timestamp': datetime.now().isoformat(),
                'status': status,  # running, success, error, shutting_down, stopped
                'message': message,
                'last_successful_scan': self.last_successful_scan.isoformat() if self.last_successful_scan else None,
                'last_heartbeat': self.last_heartbeat,
                'pid': os.getpid(),
                'uptime_seconds': int(uptime_seconds),  # Ensure it's an integer
                'uptime_human': self._format_uptime(uptime_seconds),
                'lock_file': self.pid_file,
                'is_healthy': self._is_process_healthy(),
                'statistics': scan_stats or self.scan_statistics
            }
            
            # Write atomically to avoid corruption during reads
            temp_file = self.watchdog_file + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(watchdog_data, f, indent=2)
            
            # Atomic rename to prevent partial reads
            os.rename(temp_file, self.watchdog_file)
                
        except Exception as e:
            self.logger.error(f"Failed to update watchdog file: {str(e)}")

    def _format_uptime(self, seconds):
        """Format uptime in human readable format"""
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"

    def _is_process_healthy(self):
        """Check if process is healthy based on heartbeat and recent activity"""
        current_time = time.time()
        heartbeat_age = current_time - self.last_heartbeat
        
        # Consider process unhealthy if heartbeat is older than 5 minutes
        if heartbeat_age > 300:
            return False
            
        # Additional health checks can be added here
        return True

    def _log_statistics(self):
        """Log current scanning statistics"""
        stats = self.scan_statistics
        self.logger.info("=== SCAN STATISTICS ===")
        self.logger.info(f"Total scans completed: {stats['total_scans']}")
        self.logger.info(f"Successful scans: {stats['successful_scans']}")
        self.logger.info(f"Failed scans: {stats['failed_scans']}")
        self.logger.info(f"Success rate: {(stats['successful_scans']/max(stats['total_scans'],1)*100):.1f}%")
        self.logger.info(f"Total devices processed: {stats['total_devices_processed']}")
        self.logger.info(f"Total records stored: {stats['total_records_stored']}")
        self.logger.info(f"Average scan time: {stats['average_scan_time']:.1f} seconds")
        self.logger.info(f"Last scan time: {stats['last_scan_time']:.1f} seconds")
        self.logger.info("=======================")

    def run_continuous(self, check_interval_seconds=30):
        """Run scanner continuously with process locking and heartbeat monitoring"""
        # Acquire exclusive process lock first
        if not self._acquire_lock():
            self.logger.error("Failed to acquire process lock. Exiting.")
            sys.exit(1)
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        self.logger.info(f"Starting continuous per-device scanning (check every {check_interval_seconds}s, scan devices every {self.device_scan_interval}s)")
        self.logger.info(f"Process locked with PID: {os.getpid()}")
        
        # Initial watchdog update
        self._update_watchdog("starting", f"Starting continuous per-device scanning")
        
        try:
            while True:
                try:
                    start_time = datetime.now()
                    self.logger.info(f"Checking for ready devices at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # Update heartbeat
                    self._update_watchdog("running", f"Checking for ready devices")
                    
                    # Get devices ready for scanning
                    ready_devices = self.get_devices_ready_for_scan()
                    
                    if ready_devices:
                        self.logger.info(f"Found {len(ready_devices)} devices ready for scanning")
                        
                        # Update watchdog - scan starting
                        self._update_watchdog("scanning", f"Scanning {len(ready_devices)} ready devices")
                        
                        # Run the scan for ready devices
                        devices_count = 0
                        records_count = 0
                        try:
                            result = self.run_scan_for_devices(ready_devices)
                            devices_count = result.get('devices_processed', 0)
                            records_count = result.get('records_stored', 0)
                            scan_success = True
                        except Exception as scan_error:
                            self.logger.error(f"Scan failed: {str(scan_error)}")
                            scan_success = False
                            # Don't raise here, continue with next cycle
                        
                        # Calculate how long the scan took
                        end_time = datetime.now()
                        scan_duration = (end_time - start_time).total_seconds()
                        self.logger.info(f"Scan completed in {scan_duration:.1f} seconds")
                        
                        # Update statistics
                        self.scan_statistics['total_scans'] += 1
                        self.scan_statistics['last_scan_time'] = scan_duration
                        
                        if scan_success:
                            self.scan_statistics['successful_scans'] += 1
                            self.scan_statistics['total_devices_processed'] += devices_count
                            self.scan_statistics['total_records_stored'] += records_count
                            self.last_successful_scan = end_time
                        else:
                            self.scan_statistics['failed_scans'] += 1
                        
                        # Calculate average scan time
                        if self.scan_statistics['successful_scans'] > 0:
                            total_time = self.scan_statistics['average_scan_time'] * (self.scan_statistics['successful_scans'] - 1) + scan_duration
                            self.scan_statistics['average_scan_time'] = total_time / self.scan_statistics['successful_scans']
                        
                        # Log detailed statistics
                        self._log_statistics()
                        
                        # Update watchdog with statistics - scan successful
                        if scan_success:
                            self._update_watchdog("success", f"Scan completed successfully in {scan_duration:.1f}s, processed {devices_count} devices, stored {records_count} records")
                        else:
                            self._update_watchdog("error", f"Scan failed after {scan_duration:.1f}s")
                    else:
                        self.logger.info("No devices ready for scanning")
                        # Update watchdog - no devices ready
                        total_devices = len(self.get_active_devices())
                        self._update_watchdog("idle", f"No devices ready for scanning (0/{total_devices})")
                    
                    # Sleep for the check interval with periodic heartbeat updates
                    next_check = datetime.now() + timedelta(seconds=check_interval_seconds)
                    self.logger.info(f"Next check scheduled for {next_check.strftime('%Y-%m-%d %H:%M:%S')} (sleeping {check_interval_seconds}s)")
                    
                    # Update watchdog - sleeping with heartbeat
                    self._update_watchdog("sleeping", f"Waiting for next check at {next_check.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # Sleep with periodic heartbeat updates (every 30 seconds during long sleeps)
                    sleep_remaining = check_interval_seconds
                    heartbeat_interval = min(30, check_interval_seconds)
                    
                    while sleep_remaining > 0:
                        sleep_time = min(heartbeat_interval, sleep_remaining)
                        time.sleep(sleep_time)
                        sleep_remaining -= sleep_time
                        
                        # Update heartbeat during long sleeps
                        if sleep_remaining > 0:
                            remaining_time = int(sleep_remaining)
                            self._update_watchdog("sleeping", f"Sleeping, {remaining_time}s remaining until next check")
                        
                except KeyboardInterrupt:
                    self.logger.info("Received interrupt signal, stopping continuous scanning")
                    self._update_watchdog("stopped", "Received interrupt signal, scanner stopped")
                    break
                except Exception as e:
                    self.logger.error(f"Error in scan cycle: {str(e)}")
                    self.logger.info(f"Waiting {check_interval_seconds} seconds before retry...")
                    
                    # Update watchdog - error
                    self._update_watchdog("error", f"Scan cycle failed: {str(e)}")
                    
                    time.sleep(check_interval_seconds)
        
        finally:
            # Clean shutdown
            self.logger.info("Scanner shutting down...")
            self._update_watchdog("stopped", "Scanner stopped normally")
            self._release_lock()

    def run_scan_for_devices(self, devices_to_scan):
        """Scan specific devices that are ready for scanning"""
        devices_processed = 0
        records_stored = 0
        
        try:
            # Update heartbeat at start of scan
            self._update_watchdog("scanning", f"Starting scan of {len(devices_to_scan)} devices")
            
            # Check if ACCESS database is available before cleanup
            if hasattr(self, 'access_pool'):
                self._cleanup_old_data()
            else:
                self.logger.warning("ACCESS database not available, skipping cleanup")
            
            total_devices = len(devices_to_scan)
            devices_processed = 0
            self.logger.info(f"Starting CONCURRENT scan for {total_devices} ready devices (10 threads)")
            
            # Process devices with ThreadPoolExecutor - 10 concurrent threads
            with ThreadPoolExecutor(max_workers=10, thread_name_prefix="ModulationWorker") as executor:
                # Submit all device processing tasks
                future_to_device = {
                    executor.submit(self.process_device, device): device 
                    for device in devices_to_scan
                }
                
                # Process completed tasks as they finish
                for future in as_completed(future_to_device):
                    device = future_to_device[future]
                    devices_processed += 1
                    
                    # Update heartbeat every 5 devices processed
                    if devices_processed % 5 == 0 or devices_processed == total_devices:
                        progress_pct = int((devices_processed / total_devices) * 100)
                        self._update_watchdog("scanning", f"Processing devices: {devices_processed}/{total_devices} ({progress_pct}%)")
                    
                    try:
                        data = future.result()
                        if data and hasattr(data, '__len__'):
                            records_stored += len(data)
                        self.logger.info(f"Completed device {devices_processed}/{total_devices}: {device} ({len(data) if data else 0} records)")
                    except Exception as e:
                        self.logger.error(f"Failed to process device {device}: {str(e)}")
                        continue
            
            # Update heartbeat after scan completion
            self._update_watchdog("finishing", f"Scan completed, updating database timestamp")
            
            # Update timestamp after successful completion
            if hasattr(self, 'access_pool'):
                self._update_timestamp()
            else:
                self.logger.warning("ACCESS database not available, skipping timestamp update")
            
            self.logger.info(f"Concurrent scan completed successfully: {devices_processed} devices processed, {records_stored} records stored")
            
            return {
                'devices_processed': devices_processed,
                'records_stored': records_stored
            }
            
        except Exception as e:
            self.logger.error(f"Scan failed: {str(e)}")
            raise

    def run_scan(self):
        """Main scanning workflow with concurrent processing - scans ALL devices"""
        devices_processed = 0
        records_stored = 0
        
        try:
            devices = self.get_active_devices()
            return self.run_scan_for_devices(devices)
            
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
            self._send_command(connection, 'terminal length 0', device_name)

            # Get cable upstream interfaces - equivalent to 'show interface cable-upstream'
            output = self._send_command(connection, 'show interface cable-upstream', device_name)
            if not output:
                connection.disconnect()
                return None
                
            modulation_data = []
            
            # Parse output similar to parseccap in modulation.pl
            for line in output.split('\n'):
                line = line.strip()
                if '/' in line and line:  # Make sure line is not empty
                    fields = line.split()
                    if len(fields) >= 10:
                        # Check for IS and atdma - similar to Perl logic
                        if len(fields) > 3 and len(fields) > 4 and 'IS' in fields and 'atdma' in fields:
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
                                if self.debug_ccap0:
                                    self.logger.debug(f"CCAP0 DEBUG: Found upstream {upstream} with modulation {modulation} (code: {modulation_code})")
            
            connection.disconnect()
            return modulation_data
            
        except Exception as e:
            self.logger.error(f"CCAP0 processing failed for {device_name}: {str(e)}")
            if 'connection' in locals():
                connection.disconnect()
            raise

    def _process_ccap1(self, device_name):
        """CCAP1-specific processing (CCAP100 series) - OPTIMIZED for performance"""
        try:
            self.logger.info(f"Processing CCAP1 device: {device_name}")
            connection = self._connect_to_device(device_name)
            if not connection:
                return None
                
            # Set terminal settings for CCAP1 - uses 'page-off' like CBR
            self._send_command(connection, 'page-off', device_name)
            
            # OPTIMIZATION 1: Get ALL spectrum data in one command instead of per-interface
            # This dramatically reduces the number of commands from N interfaces to 1 command
            self.logger.info(f"Getting bulk spectrum data for {device_name}")
            bulk_cmd = "show spectrum hop-history"
            spectrum_output = self._send_command(connection, bulk_cmd, device_name)
            
            modulation_data = []
            
            # Check if we got output and process it
            if spectrum_output:
                self.logger.info(f"CCAP1 bulk command succeeded for {device_name}, processing spectrum data...")
                
                current_upstream = None
                processed_upstreams = set()  # Track which upstreams we've already processed
                
                for line in spectrum_output.split('\n'):
                    line = line.strip()
                    
                    if not line or 'show' in line or 'Port' in line:
                        continue
                        
                    # Look for lines with modulation data (lines with 'M' and timestamps)
                    if 'M' in line and len(line.split()) >= 9:
                        fields = line.split()
                        
                        # Extract upstream from the first field (like "4/15.3/0")
                        if '/' in fields[0]:
                            upstream_interface = fields[0]
                            
                            # Only process if we haven't seen this upstream yet (take first/most recent only)
                            if upstream_interface not in processed_upstreams:
                                # IMPORTANT: Take field 9 (TO modulation), not field 8 (FROM modulation)
                                # Field 8 is what it changed FROM, field 9 is what it changed TO (current state)
                                if len(fields) > 9:
                                    modulation_code = fields[9]  # Position 9 = TO modulation (current state)
                                else:
                                    modulation_code = fields[8]  # Fallback to position 8 if not enough fields
                                
                                modulation = self._convert_modulation_code(modulation_code)
                                
                                # DEBUG: Log the raw modulation codes we're seeing (only if debug enabled)
                                if self.debug_ccap1 and len(modulation_data) < 10:  # Only log first 10 to avoid spam
                                    self.logger.debug(f"CCAP1 DEBUG: {device_name} upstream {upstream_interface} - Fields[8]={fields[8] if len(fields)>8 else 'N/A'} Fields[9]={fields[9] if len(fields)>9 else 'N/A'} -> Using: '{modulation_code}' -> CONVERTED: '{modulation}'")
                                
                                if modulation:
                                    record = {
                                        'device_name': device_name,
                                        'upstream': upstream_interface,
                                        'modulation': modulation
                                    }
                                    modulation_data.append(record)
                                    processed_upstreams.add(upstream_interface)
                                else:
                                    # Log unknown codes to see what we're missing (only if debug enabled)
                                    if self.debug_ccap1:
                                        self.logger.warning(f"CCAP1 DEBUG: UNKNOWN MODULATION CODE: {device_name} upstream {upstream_interface} - code '{modulation_code}' not recognized")
                                    processed_upstreams.add(upstream_interface)  # Still mark as processed to avoid duplicates
                
                self.logger.info(f"CCAP1 bulk processing found {len(modulation_data)} modulation records for {device_name}")
            else:
                # FALLBACK: If bulk command doesn't work, use the original method but with optimizations
                self.logger.warning(f"Bulk command failed for {device_name}, falling back to individual interface processing")
                
                # Get cable modem summary - like CBR processing
                output = self._send_command(connection, 'show cable modem sum', device_name)
                if not output:
                    self.logger.warning(f"CCAP1 fallback: No cable modem summary output for {device_name}")
                    connection.disconnect()
                    return []
                else:
                    if self.debug_ccap1:
                        self.logger.debug(f"CCAP1 fallback: Cable modem summary output length: {len(output)} chars")
                    
                # Extract interfaces - CCAP1 uses longer interface names (9 chars) like CBR
                interfaces = []
                for line in output.split('\n'):
                    line = line.strip()
                    if line and '/' in line:
                        fields = line.split()
                        if fields:
                            # Remove 'C' prefix and take first 9 characters
                            interface = fields[0].replace('C', '')[:9]
                            if interface not in interfaces:
                                interfaces.append(interface)
                
                self.logger.info(f"{device_name} found {len(interfaces)} interfaces for fallback processing")
                
                # OPTIMIZATION 2: Limit the number of interfaces processed in fallback mode
                max_interfaces = 20  # Limit to prevent excessive slowdown
                if len(interfaces) > max_interfaces:
                    self.logger.warning(f"{device_name} has {len(interfaces)} interfaces, limiting to {max_interfaces} for performance")
                    interfaces = interfaces[:max_interfaces]
                
                # Process each interface with timeout optimization
                for i, interface in enumerate(interfaces):
                    if self.debug_ccap1:
                        self.logger.debug(f"CCAP1 fallback: Processing interface {i+1}/{len(interfaces)}: {interface}")
                    cmd = f"show spectrum hop-history upstream {interface}"
                    
                    # OPTIMIZATION 3: Reduce timeout for individual commands
                    try:
                        spectrum_output = connection.send_command(cmd, read_timeout=30)  # Reduced from default 120
                        
                        if spectrum_output:
                            upstream = interface  # For CCAP1, upstream = interface (like CBR)
                            
                            for line in spectrum_output.split('\n'):
                                line = line.strip()
                                # Skip 'show' commands and 'Port' lines
                                if line and 'show' not in line and 'Port' not in line:
                                    # Look for lines with 'M' (modulation marker)
                                    if 'M' in line:
                                        fields = line.split()
                                        if len(fields) > 8:
                                            modulation_code = fields[8]  # Position 8 like CBR
                                            if self.debug_ccap1:
                                                self.logger.debug(f"CCAP1 fallback: Found modulation code {modulation_code} for interface {interface}")
                                            modulation = self._convert_modulation_code(modulation_code)
                                            
                                            if modulation:
                                                modulation_data.append({
                                                    'device_name': device_name,
                                                    'upstream': upstream,
                                                    'modulation': modulation
                                                })
                                                if self.debug_ccap1:
                                                    self.logger.debug(f"CCAP1 fallback: Added modulation {modulation} for upstream {upstream}")
                                                break  # Only take first modulation entry per interface
                                            else:
                                                if self.debug_ccap1:
                                                    self.logger.warning(f"CCAP1 fallback: Unknown modulation code {modulation_code} for interface {interface}")
                    except Exception as e:
                        self.logger.warning(f"Timeout/error on interface {interface}: {str(e)}")
                        continue  # Skip this interface and continue with others
            
            connection.disconnect()
            
            # Show sample of what we're storing
            if modulation_data:
                self.logger.info(f"{device_name} CCAP1 processing completed with {len(modulation_data)} records")
                # Show first few records as examples
                sample_size = min(5, len(modulation_data))
                self.logger.info(f"Sample records for {device_name}:")
                for i, record in enumerate(modulation_data[:sample_size]):
                    self.logger.info(f"  {i+1}: {record['upstream']} -> {record['modulation']}")
                if len(modulation_data) > sample_size:
                    self.logger.info(f"  ... and {len(modulation_data) - sample_size} more records")
            else:
                self.logger.info(f"{device_name} CCAP1 processing completed with 0 records")
                
            return modulation_data
            
        except Exception as e:
            self.logger.error(f"CCAP1 processing failed for {device_name}: {str(e)}")
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
            self._send_command(connection, 'term length 0', device_name)
            
            # Get cable modem summary
            output = self._send_command(connection, 'show cable modem sum', device_name)
            if not output:
                connection.disconnect()
                return None
                
            # Extract interfaces similar to parsedbr in modulation.pl
            interfaces = []
            for line in output.split('\n'):
                line = line.strip()
                if line and '/' in line:
                    fields = line.split()
                    if fields:
                        # Remove 'C' prefix and keep full interface name (not just 5 chars)
                        interface = fields[0].replace('C', '')
                        if interface not in interfaces:
                            interfaces.append(interface)
            
            self.logger.info(f"{device_name} found {len(interfaces)} interfaces")
            
            # If no interfaces, that's normal - just return empty data
            if not interfaces:
                connection.disconnect()
                return []
            
            modulation_data = []
            
            # Get unique base interfaces (remove duplicates from /UB, /U0, /U1 variations)
            base_interfaces = []
            for interface in interfaces:
                base_interface = re.sub(r'/U[B\d]+$', '', interface)
                if base_interface not in base_interfaces:
                    base_interfaces.append(base_interface)
            
            self.logger.info(f"{device_name} found {len(base_interfaces)} unique base interfaces")
            
            # Process all unique base interfaces
            for base_interface in base_interfaces:
                cmd = f"show controller c{base_interface} Upstream | i Profile|Upstream|US|up|UP"
                
                try:
                    controller_output = self._send_command(connection, cmd, device_name)
                    
                    if controller_output:
                        current_modulation = None
                        found_modulation_line = False
                        found_bind_line = False
                        
                        for line in controller_output.split('\n'):
                            line = line.strip()
                            if line and 'show' not in line:
                                # Find modulation - only process traditional DOCSIS, ignore OFDMA (Subcarrier)
                                if 'Modulation Profile Group' in line and 'Subcarrier' not in line:
                                    found_modulation_line = True
                                    # Extract the number after "Modulation Profile Group "
                                    parts = line.split('Modulation Profile Group ')
                                    if len(parts) > 1:
                                        modulation_code = parts[1].split()[0]  # Get first word after the phrase
                                        current_modulation = self._convert_modulation_code(modulation_code)
                                elif 'Modulation Profile (ID' in line and 'Subcarrier' not in line:
                                    # Handle the other format but ignore OFDMA lines with Subcarrier
                                    found_modulation_line = True
                                    # Extract ID number
                                    match = re.search(r'ID (\d+)', line)
                                    if match:
                                        modulation_code = match.group(1)
                                        current_modulation = self._convert_modulation_code(modulation_code)
                                elif 'Modulation Profile' in line and 'Subcarrier' in line:
                                    # Skip OFDMA lines that we're ignoring
                                    pass
                                
                                # Find bind information - similar to Perl logic
                                if ('Bind' in line and 'to' in line and 'US6' not in line):
                                    found_bind_line = True
                                    fields = line.split()
                                    if len(fields) > 3:  # Changed from > 4 to > 3
                                        upstream = fields[3].replace('US', '')  # Changed from fields[4] to fields[3]
                                        
                                        # Only process US0 to US3, ignore all others
                                        if upstream in ['0', '1', '2', '3']:
                                            if current_modulation:
                                                modulation_data.append({
                                                    'device_name': device_name,
                                                    'upstream': f"{base_interface}/{upstream}",
                                                    'modulation': current_modulation
                                                })
                                                if self.debug_ccap2:
                                                    self.logger.debug(f"CCAP2 DEBUG: Added modulation {current_modulation} for upstream {base_interface}/{upstream}")
                                            else:
                                                if self.debug_ccap2:
                                                    self.logger.warning(f"CCAP2 DEBUG: {device_name} found bind but no modulation for upstream {upstream}")
                                    else:
                                        if self.debug_ccap2:
                                            self.logger.warning(f"CCAP2 DEBUG: {device_name} bind line has insufficient fields: {fields}")
                        
                        if not found_modulation_line:
                            if self.debug_ccap2:
                                self.logger.warning(f"CCAP2 DEBUG: {device_name} interface {base_interface}: No 'Modulation' lines found")
                        if not found_bind_line:
                            if self.debug_ccap2:
                                self.logger.warning(f"CCAP2 DEBUG: {device_name} interface {base_interface}: No 'Bind' lines found")
                    else:
                        if self.debug_ccap2:
                            self.logger.warning(f"CCAP2 DEBUG: {device_name} interface {base_interface}: No controller output received")
                        
                except Exception as e:
                    self.logger.error(f"{device_name} error processing interface {base_interface}: {str(e)}")
                    continue
            
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
            # Original CCAP0 codes
            '202': 'QAM64',
            '204': 'QAM16', 
            '222': 'QPSK',
            # CCAP2 DBR codes from parsedbr in modulation.pl
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
            # Enhanced cleanup with configurable retention (now 8 days instead of 7)
            retention_days = int(os.getenv('DATA_RETENTION_DAYS', '8'))
            cleanup_query = f"DELETE FROM modulation_new WHERE timestamp < NOW() - INTERVAL {retention_days} DAY"
            self.execute_access_db_query(cleanup_query, fetch_all=False)
            self.logger.info(f"Cleaned up old modulation data (older than {retention_days} days)")
            
            # Additional cleanup: Remove data older than 48 hours to limit data collection window
            # This ensures we only keep 2 days of detailed data for performance
            detailed_data_hours = 48  # 2 days = 48 hours
            detailed_cleanup_query = f"DELETE FROM modulation_new WHERE timestamp < NOW() - INTERVAL {detailed_data_hours} HOUR"
            
            # Count how many records would be affected first
            count_query = f"SELECT COUNT(*) as count FROM modulation_new WHERE timestamp < NOW() - INTERVAL {detailed_data_hours} HOUR"
            result = self.execute_access_db_query(count_query, fetch_all=False)
            records_to_delete = result.get('count', 0) if result else 0
            
            if records_to_delete > 0:
                self.execute_access_db_query(detailed_cleanup_query, fetch_all=False)
                self.logger.info(f"Cleaned up {records_to_delete} records older than {detailed_data_hours} hours for performance")
            else:
                self.logger.info("No old detailed data to clean up")
            
            # Run database maintenance check
            self._run_maintenance_check()
            
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
                
            # Check if ACCESS database is available
            if not hasattr(self, 'access_pool'):
                self.logger.warning("ACCESS database not available, data will not be stored")
                return
                
            # Batch insert for better performance
            insert_query = """
            INSERT INTO modulation_new (cmts, upstream, modulation, timestamp)
            VALUES (%(device_name)s, %(upstream)s, %(modulation)s, NOW())
            """
            
            # Execute all inserts in a single connection for better performance
            conn = None
            cursor = None
            try:
                conn = self.access_pool.get_connection()
                cursor = conn.cursor()
                
                for record in data:
                    cursor.execute(insert_query, record)
                
                conn.commit()  # Commit all at once
                self.logger.info(f"Stored {len(data)} modulation records for device: {device_name}")
                
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to store data for {device_name}: {str(e)}")
            raise

    def _run_maintenance_check(self):
        """Run database maintenance check to prevent AUTO_INCREMENT overflow"""
        try:
            # Only run maintenance check if enabled
            enable_maintenance = os.getenv('ENABLE_AUTO_CLEANUP', 'true').lower() == 'true'
            if not enable_maintenance:
                return
            
            # Import here to avoid circular imports
            from database_maintenance import DatabaseMaintenance
            
            # Run maintenance check (non-blocking)
            maintenance = DatabaseMaintenance()
            status = maintenance.get_maintenance_status()
            
            # Check if any table needs urgent attention
            for table_name, health in status.get('tables', {}).items():
                if health and health.get('health_status') == 'CRITICAL':
                    self.logger.warning(f"CRITICAL: Table {table_name} needs immediate maintenance - "
                                      f"AUTO_INCREMENT usage at {health['stats']['usage_percentage']:.1f}%")
                    
                    # Run emergency maintenance if usage is very high (>90%)
                    if health['stats']['usage_percentage'] > 90:
                        self.logger.info(f"Running emergency maintenance for {table_name}")
                        maintenance.run_maintenance(force=True)
                        
        except ImportError:
            self.logger.debug("Database maintenance module not available")
        except Exception as e:
            self.logger.warning(f"Maintenance check failed (non-critical): {str(e)}")

if __name__ == "__main__":
    try:
        scanner = ModulationScanner()
        
        # Run continuous per-device scanning (check every 30 seconds, scan each device every 10 minutes)
        scanner.run_continuous(check_interval_seconds=30)
            
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        exit(1)