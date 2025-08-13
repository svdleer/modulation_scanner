#!/home/svdleer/python/venv/bin/python3

"""
Modulation Scanner Watchdog
Monitors the modulation scanner process and restarts it if needed.
This script should be run via cron to ensure the scanner is always running.
"""

import os
import sys
import json
import time
import signal
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

class ScannerWatchdog:
    def __init__(self):
        # Use specific production path
        self.script_dir = Path("/home/svdleer/scripts/python/modulation")
        self.scanner_script = self.script_dir / "modulation_scanner.py"
        # Use /tmp for PID and status files to avoid NFS locking issues
        self.pid_file = Path("/tmp/modulation_scanner.pid")
        self.status_file = Path("/tmp/modulation_scanner.status")
        self.watchdog_log = self.script_dir / "watchdog.log"
        
        # Watchdog configuration
        self.max_heartbeat_age = 60  # 1 minute - scanner should update every 30s
        self.max_restart_attempts = 3
        self.restart_delay = 30  # seconds between restart attempts
        
    def log(self, message, level="INFO"):
        """Log message to watchdog log file"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] [{level}] {message}\n"
        
        try:
            with open(self.watchdog_log, 'a') as f:
                f.write(log_entry)
            print(f"{timestamp} [{level}] {message}")
        except Exception:
            print(f"{timestamp} [{level}] {message}")

    def get_scanner_status(self):
        """Get current status of the scanner"""
        try:
            if not self.status_file.exists():
                return {
                    'running': False,
                    'reason': 'No status file found'
                }
            
            with open(self.status_file, 'r') as f:
                status_data = json.load(f)
            
            # Check if process is running
            pid = status_data.get('pid')
            if not pid:
                return {
                    'running': False,
                    'reason': 'No PID in status file'
                }
            
            # Check if PID is actually running
            if not self._is_process_running(pid):
                return {
                    'running': False,
                    'reason': f'Process {pid} not running'
                }
            
            # Check heartbeat age
            last_heartbeat = status_data.get('last_heartbeat', 0)
            heartbeat_age = time.time() - last_heartbeat
            
            if heartbeat_age > self.max_heartbeat_age:
                return {
                    'running': False,
                    'reason': f'Heartbeat too old ({heartbeat_age:.0f}s)'
                }
            
            # Check if process is healthy
            is_healthy = status_data.get('is_healthy', True)
            if not is_healthy:
                return {
                    'running': False,
                    'reason': 'Process marked as unhealthy'
                }
            
            return {
                'running': True,
                'status': status_data.get('status', 'unknown'),
                'uptime': status_data.get('uptime_human', 'unknown'),
                'pid': pid,
                'heartbeat_age': heartbeat_age
            }
            
        except Exception as e:
            return {
                'running': False,
                'reason': f'Error reading status: {str(e)}'
            }

    def _is_process_running(self, pid):
        """Check if process is running"""
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False

    def stop_scanner(self, pid):
        """Stop the scanner gracefully"""
        try:
            self.log(f"Stopping scanner process {pid}")
            
            # First try graceful shutdown with SIGTERM
            os.kill(pid, signal.SIGTERM)
            
            # Wait up to 30 seconds for graceful shutdown
            for _ in range(30):
                if not self._is_process_running(pid):
                    self.log(f"Process {pid} stopped gracefully")
                    return True
                time.sleep(1)
            
            # If still running, force kill
            self.log(f"Process {pid} didn't stop gracefully, force killing")
            os.kill(pid, signal.SIGKILL)
            time.sleep(2)
            
            if not self._is_process_running(pid):
                self.log(f"Process {pid} force killed")
                return True
            
            self.log(f"Failed to stop process {pid}", "ERROR")
            return False
            
        except Exception as e:
            self.log(f"Error stopping process {pid}: {str(e)}", "ERROR")
            return False

    def start_scanner(self):
        """Start the scanner process"""
        try:
            self.log("Starting modulation scanner")
            
            # Use specific production Python interpreter
            python_exe = "/home/svdleer/python/venv/bin/python"
            cmd = [python_exe, str(self.scanner_script)]
            
            # Create log files for debugging startup issues
            scanner_stdout_log = self.script_dir / "scanner_startup.log"
            scanner_stderr_log = self.script_dir / "scanner_startup_errors.log"
            
            # Start process in background with logging
            process = subprocess.Popen(
                cmd,
                cwd=str(self.script_dir),
                stdout=open(scanner_stdout_log, 'w'),
                stderr=open(scanner_stderr_log, 'w'),
                start_new_session=True  # Detach from current session
            )
            
            # Store the process PID for later verification
            self.last_started_pid = process.pid
            
            # Wait a moment and check if it started
            time.sleep(3)
            
            if process.poll() is None:
                self.log(f"Scanner started successfully (PID: {process.pid})")
                
                # Check for immediate startup errors
                time.sleep(2)  # Give it a bit more time to initialize
                
                if scanner_stderr_log.exists():
                    try:
                        with open(scanner_stderr_log, 'r') as f:
                            stderr_content = f.read().strip()
                        if stderr_content:
                            self.log(f"Scanner startup warnings/errors: {stderr_content[:500]}...")
                    except Exception:
                        pass
                        
                return True
            else:
                self.log(f"Scanner failed to start (exit code: {process.returncode})", "ERROR")
                
                # Show startup errors if available
                if scanner_stderr_log.exists():
                    try:
                        with open(scanner_stderr_log, 'r') as f:
                            stderr_content = f.read().strip()
                        if stderr_content:
                            self.log(f"Scanner startup errors: {stderr_content}")
                    except Exception:
                        pass
                        
                return False
                
        except Exception as e:
            self.log(f"Error starting scanner: {str(e)}", "ERROR")
            return False

    def cleanup_stale_files(self):
        """Clean up stale PID and status files"""
        try:
            if self.pid_file.exists():
                self.pid_file.unlink()
                self.log("Removed stale PID file")
            
            if self.status_file.exists():
                # Check if status file has corrupted timestamp before removing
                try:
                    with open(self.status_file, 'r') as f:
                        status_data = json.load(f)
                    
                    last_heartbeat = status_data.get('last_heartbeat', 0)
                    current_time = time.time()
                    heartbeat_age = current_time - last_heartbeat
                    
                    # If heartbeat is impossibly old (more than 1 year), it's corrupted
                    if heartbeat_age > 31536000:  # 1 year in seconds
                        self.log(f"Status file contains corrupted timestamp ({last_heartbeat}), removing it")
                        self.status_file.unlink()
                        self.log("Removed corrupted status file")
                    else:
                        self.status_file.unlink()
                        self.log("Removed stale status file")
                        
                except (json.JSONDecodeError, KeyError):
                    self.log("Status file is corrupted (invalid JSON), removing it")
                    self.status_file.unlink()
                    self.log("Removed corrupted status file")
                
        except Exception as e:
            self.log(f"Error cleaning up files: {str(e)}", "ERROR")

    def check_and_restart(self):
        """Main watchdog logic - check status and restart if needed"""
        self.log("Watchdog check starting")
        
        status = self.get_scanner_status()
        
        # Always log the status details for debugging
        if status['running']:
            heartbeat_age = status.get('heartbeat_age', 0)
            self.log(f"Scanner status check: RUNNING (PID: {status['pid']}, uptime: {status['uptime']}, heartbeat: {heartbeat_age:.0f}s ago)")
            
            # Additional check - warn if heartbeat is getting old (> 45s)
            if heartbeat_age > 45:
                self.log(f"WARNING: Heartbeat is getting old ({heartbeat_age:.0f}s ago) - threshold is {self.max_heartbeat_age}s", "WARNING")
            
            return
        else:
            self.log(f"Scanner status check: NOT RUNNING - {status.get('reason', 'Unknown reason')}", "WARNING")
        
        # Scanner is not running properly
        self.log(f"Scanner not running: {status['reason']}", "WARNING")
        
        # Try to stop any existing process
        if self.pid_file.exists():
            try:
                with open(self.pid_file, 'r') as f:
                    old_pid = int(f.read().strip())
                
                if self._is_process_running(old_pid):
                    self.log(f"Found running process {old_pid}, stopping it")
                    self.stop_scanner(old_pid)
                    
            except Exception as e:
                self.log(f"Error handling existing process: {str(e)}", "WARNING")
        
        # Clean up stale files
        self.cleanup_stale_files()
        
        # Attempt to restart
        restart_attempts = 0
        while restart_attempts < self.max_restart_attempts:
            restart_attempts += 1
            self.log(f"Restart attempt {restart_attempts}/{self.max_restart_attempts}")
            
            if self.start_scanner():
                # Wait a bit and verify it's running
                self.log("Waiting 10 seconds for scanner to initialize...")
                time.sleep(10)
                verify_status = self.get_scanner_status()
                
                self.log(f"Scanner verification result: {verify_status}")
                
                if verify_status['running']:
                    self.log(f"Scanner restarted successfully on attempt {restart_attempts}")
                    return
                else:
                    self.log(f"Scanner failed verification after restart attempt {restart_attempts}", "WARNING")
                    self.log(f"Verification failure reason: {verify_status['reason']}")
                    
                    # Additional debugging for verification failures
                    self.log("=== DEBUG INFO FOR FAILED VERIFICATION ===")
                    self.log(f"PID file exists: {self.pid_file.exists()}")
                    self.log(f"Status file exists: {self.status_file.exists()}")
                    
                    # Check if the scanner process we started is still running
                    if hasattr(self, 'last_started_pid'):
                        still_running = self._is_process_running(self.last_started_pid)
                        self.log(f"Scanner process {self.last_started_pid} still running: {still_running}")
                        if not still_running:
                            self.log("Scanner process exited after startup - likely failed during initialization")
                        else:
                            self.log("Scanner process is running but not creating PID/status files")
                    
                    # Check what files exist in the directory
                    try:
                        import os
                        files = os.listdir(str(self.script_dir))
                        scanner_files = [f for f in files if 'scanner' in f.lower()]
                        self.log(f"Scanner-related files in directory: {scanner_files}")
                        
                        # Check directory permissions
                        dir_stat = os.stat(str(self.script_dir))
                        self.log(f"Directory permissions: {oct(dir_stat.st_mode)}")
                        
                        # Test if we can create files in the directory
                        test_file = self.script_dir / "test_write_permissions.tmp"
                        try:
                            with open(test_file, 'w') as f:
                                f.write("test")
                            test_file.unlink()
                            self.log("Directory write permissions: OK")
                        except Exception as perm_e:
                            self.log(f"Directory write permissions: FAILED - {perm_e}")
                        
                        # Test if we can create and lock a PID file like the scanner does
                        test_pid_file = self.script_dir / "test_modulation_scanner.pid"
                        try:
                            import fcntl
                            with open(test_pid_file, 'w') as f:
                                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                                f.write("12345")
                                f.flush()
                                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                            test_pid_file.unlink()
                            self.log("File locking test: OK")
                        except Exception as lock_e:
                            self.log(f"File locking test: FAILED - {lock_e}")
                            if test_pid_file.exists():
                                try:
                                    test_pid_file.unlink()
                                except:
                                    pass
                        
                        # Check for .env file
                        env_file = self.script_dir / ".env"
                        self.log(f".env file exists: {env_file.exists()}")
                        
                        # Check for required Python files
                        required_files = ['modulation_scanner.py', 'multithreading_base.py']
                        for req_file in required_files:
                            file_path = self.script_dir / req_file
                            self.log(f"{req_file} exists: {file_path.exists()}")
                            
                    except Exception as e:
                        self.log(f"Error checking directory contents: {e}")
                    
                    # Check for startup logs
                    startup_log = self.script_dir / "scanner_startup.log"
                    startup_err = self.script_dir / "scanner_startup_errors.log"
                    
                    if startup_err.exists():
                        try:
                            with open(startup_err, 'r') as f:
                                err_content = f.read().strip()
                            if err_content:
                                self.log(f"Startup errors: {err_content[:1000]}")
                            else:
                                self.log("Startup errors file exists but is empty")
                        except Exception as e:
                            self.log(f"Could not read startup errors: {e}")
                    else:
                        self.log("No startup errors file found")
                    
                    if startup_log.exists():
                        try:
                            with open(startup_log, 'r') as f:
                                log_content = f.read().strip()
                            if log_content:
                                self.log(f"Startup output: {log_content[-500:]}")  # Last 500 chars
                            else:
                                self.log("Startup log file exists but is empty")
                        except Exception as e:
                            self.log(f"Could not read startup log: {e}")
                    else:
                        self.log("No startup log file found")
                    
                    self.log("=== END DEBUG INFO ===")
            
            if restart_attempts < self.max_restart_attempts:
                self.log(f"Waiting {self.restart_delay}s before next attempt")
                time.sleep(self.restart_delay)
        
        self.log(f"Failed to restart scanner after {self.max_restart_attempts} attempts", "ERROR")

    def status_report(self):
        """Generate detailed status report"""
        self.log("=== SCANNER STATUS REPORT ===")
        
        # Show raw status file contents for debugging
        if self.status_file.exists():
            try:
                with open(self.status_file, 'r') as f:
                    raw_content = f.read()
                self.log(f"Raw status file content: {raw_content}")
                
                # Parse and show individual fields
                try:
                    status_data = json.loads(raw_content)
                    self.log(f"Status data fields:")
                    for key, value in status_data.items():
                        self.log(f"  {key}: {value} (type: {type(value).__name__})")
                    
                    # Show current time for comparison
                    current_time = time.time()
                    self.log(f"Current timestamp: {current_time}")
                    
                    last_heartbeat = status_data.get('last_heartbeat', 0)
                    self.log(f"Heartbeat timestamp: {last_heartbeat}")
                    self.log(f"Heartbeat age: {current_time - last_heartbeat:.0f} seconds")
                    
                except json.JSONDecodeError as e:
                    self.log(f"Status file JSON parse error: {e}")
                    
            except Exception as e:
                self.log(f"Error reading status file: {e}")
        
        status = self.get_scanner_status()
        
        if status['running']:
            self.log(f"✓ Scanner is RUNNING")
            self.log(f"  PID: {status['pid']}")
            self.log(f"  Status: {status['status']}")
            self.log(f"  Uptime: {status['uptime']}")
            self.log(f"  Last heartbeat: {status['heartbeat_age']:.0f}s ago")
        else:
            self.log(f"✗ Scanner is NOT RUNNING")
            self.log(f"  Reason: {status['reason']}")
        
        # Show file status
        self.log(f"PID file: {'EXISTS' if self.pid_file.exists() else 'MISSING'}")
        self.log(f"Status file: {'EXISTS' if self.status_file.exists() else 'MISSING'}")
        
        self.log("=== END STATUS REPORT ===")

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Modulation Scanner Watchdog')
    parser.add_argument('--check', action='store_true', help='Check and restart if needed (default)')
    parser.add_argument('--status', action='store_true', help='Show status report only')
    parser.add_argument('--start', action='store_true', help='Force start scanner')
    parser.add_argument('--stop', action='store_true', help='Stop scanner')
    
    args = parser.parse_args()
    
    watchdog = ScannerWatchdog()
    
    if args.status:
        watchdog.status_report()
    elif args.start:
        watchdog.start_scanner()
    elif args.stop:
        status = watchdog.get_scanner_status()
        if status['running']:
            watchdog.stop_scanner(status['pid'])
        else:
            watchdog.log("Scanner is not running")
    else:
        # Default: check and restart if needed
        watchdog.check_and_restart()

if __name__ == "__main__":
    main()
