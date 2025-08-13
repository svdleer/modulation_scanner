# Multithreading Modulation Scanner - Installation Guide

## Overview
This system provides automated network modulation scanning with concurrent processing, database maintenance, web reporting capabilities, and production-ready process management with watchdog monitoring.

## Prerequisites
- Python 3.8 or higher
- Access to MySQL database (ACCESS database)
- Network access to CCAP devices
- SMTP server for email reports (optional)
- SSH/Netmiko access to network devices
- For production: cron (Linux) or Task Scheduler (Windows)

## Quick Installation

### 1. Verify Virtual Environment
The system already has a virtual environment configured in `.venv`. Verify it's working:
```cmd
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe --version
```

### 2. Install Dependencies
```cmd
cd c:\Users\svdleer\Documents\Python\Source\multithreading
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/pip.exe install -r requirements.txt
```

### 3. Configure Environment
The `.env` file is already configured. Verify these settings:

**Database Configuration:**
- `ACCESS_HOST`: MySQL server for storing modulation data
- `ACCESS_DATABASE`: Database name (usually 'access')
- `ACCESS_USER`: MySQL username
- `ACCESS_PASSWORD`: MySQL password
- `DATA_RETENTION_DAYS`: How many days to keep in database (default: 8)

**Time Windows:**
- **Data Collection**: Last 2 days (48 hours) for detailed analysis
- **Report Generation**: Previous day (00:00 to 23:59) by default
- **Database Retention**: Keep 8 days of historical data

**Email Configuration (for reports):**
- `SMTP_HOST`: SMTP server (currently localhost)
- `FROM_EMAIL`: Sender email address
- `TO_EMAILS`: Comma-separated recipient emails

### 4. Test Database Connection
```cmd
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe -c "from multithreading_base import MultithreadingBase; mb = MultithreadingBase(); print('Database connection: OK')"
```

### 5. Test Installation
```cmd
# Test the modulation scanner (dry run - will not scan devices)
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe modulation_scanner.py --help

# Test the report generator
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe report_modulation.py --help
```

## Running the System

### Continuous Modulation Scanning
```cmd
# Run continuous scanning (checks every 30s, scans devices every 10 minutes)
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe modulation_scanner.py
```

### Generate Modulation Reports
```cmd
# Generate yesterday's report (default behavior)
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe report_modulation.py

# Generate report for specific date
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe report_modulation.py --date 2025-08-12

# Generate today's report (override default)
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe report_modulation.py --date 2025-08-13

# Enable debug mode
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe report_modulation.py --debug

# Generate only web JSON (faster for web updates)
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe report_modulation.py --json-only

# Generate only CSV report
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe report_modulation.py --csv-only
```

### Database Maintenance
```cmd
# Run database maintenance
C:/Users/svdleer/Documents/Python\Source\multithreading/.venv/Scripts/python.exe database_maintenance.py

# Check maintenance status
C:/Users/svdleer/Documents/Python\Source\multithreading/.venv/Scripts/python.exe database_maintenance.py --status
```

## Automated Deployment (Production)

### Windows Scheduled Task
1. Create scheduled task to run `modulation_scanner.py` continuously
2. Set up daily task to run `report_modulation.py`
3. Optional: Create task to update web reports every 30 minutes with `--json-only`
4. Weekly task for `database_maintenance.py`

### Linux Cron Jobs
```bash
# Run scanner continuously (use systemd service instead)
# 0 */1 * * * /path/to/venv/bin/python /path/to/modulation_scanner.py

# Generate daily reports at 6 AM
0 6 * * * /path/to/venv/bin/python /path/to/report_modulation.py

# Update web reports every 30 minutes (optional)
*/30 * * * * /path/to/venv/bin/python /path/to/report_modulation.py --json-only

# Database maintenance weekly on Sunday at 2 AM
0 2 * * 0 /path/to/venv/bin/python /path/to/database_maintenance.py
```

## Web Interface

The system includes a modern web interface (`report_modulation_web.php`) with:
- Real-time search and filtering
- Responsive design with Tailwind CSS
- Interactive sorting and pagination
- VodafoneZiggo branding
- Auto-refresh for current day reports
- Date selector for historical data

### Web Report Updates
You can update web reports independently of the main scanner:

```cmd
# Quick web update (generates JSON only)
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe report_modulation.py --json-only

# Update for specific date
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe report_modulation.py --json-only --date 2025-08-12
```

The web interface automatically:
- Deploys updated JSON files to the web server
- Creates 'latest' symlinks for current data  
- Performs housekeeping to remove old files
- Updates with proper www-data permissions

Deploy the PHP file to your web server and ensure database connectivity.

## Monitoring & Debugging

### Debug Modes
Enable debug logging for specific device types in `.env`:
```properties
DEBUG_CCAP0=true
DEBUG_CCAP1=true
DEBUG_CCAP2=true
```

### Log Files
- Scanner logs: Check console output or configure logging
- Watchdog file: `modulation_scanner.status` (JSON format)
- Database errors: Check MySQL logs

### Health Checks
- Monitor `modulation_scanner.status` for system health
- Check database connection with test command
- Verify email delivery with debug mode

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Verify ACCESS_HOST, ACCESS_USER, ACCESS_PASSWORD in `.env`
   - Check MySQL server is running and accessible
   - Verify firewall rules allow MySQL connection

2. **Device Connection Timeout**
   - Check network connectivity to CCAP devices
   - Verify SSH credentials in code (n3user/S4ndw1cH)
   - Check device SSH access policies

3. **Email Not Sending**
   - Verify SMTP_HOST and SMTP_PORT in `.env`
   - Check SMTP server authentication requirements
   - Enable email debugging with `--debug` flag

4. **Performance Issues**
   - Adjust concurrent thread count in `modulation_scanner.py`
   - Check database indexing on `modulation_new` table
   - Monitor memory usage during large scans

### Email Debugging
The system now includes comprehensive email debugging:
```cmd
C:/Users/svdleer/Documents/Python/Source/multithreading/.venv/Scripts/python.exe report_modulation.py --debug
```

This will show detailed SMTP connection information and help diagnose delivery issues.

## System Architecture

- **modulation_scanner.py**: Main scanning engine with concurrent processing
- **multithreading_base.py**: Base class with database connections
- **report_modulation.py**: Report generation with performance optimizations
- **database_maintenance.py**: Automated database cleanup and maintenance
- **report_modulation_web.php**: Modern web interface

## Data Management Features

### Time Windows
- **Data Collection**: Keeps last 48 hours (2 days) of detailed data for analysis
- **Report Generation**: Defaults to previous day (00:00 to 23:59)
- **Database Retention**: Maintains 8 days of historical data
- **Automatic Cleanup**: Removes data older than retention period

### Performance Features

- Vectorized pandas operations for 10-50x speed improvement
- Concurrent device processing (10 threads)  
- Intelligent device scheduling (10-minute intervals)
- Database connection pooling
- Automated cleanup and maintenance
- Web deployment automation with SCP
- Date-filtered queries for optimal performance

The system is production-ready and handles large-scale network monitoring efficiently with intelligent data lifecycle management.

## Production Deployment & Watchdog System

### Process Management Features
The system includes sophisticated process management to ensure reliable production operation:

- **Exclusive Process Locking**: Prevents multiple scanner instances using fcntl file locking
- **Graceful Shutdown**: Handles SIGTERM and SIGINT signals for clean exits
- **Heartbeat Monitoring**: Continuous health status updates with timestamp
- **Automatic Restart**: Watchdog system detects and restarts failed processes
- **PID Management**: Tracks process IDs and detects stale processes

### Watchdog System
The `scanner_watchdog.py` provides comprehensive monitoring and automatic recovery:

```cmd
# Check scanner status and restart if needed
python scanner_watchdog.py

# Show detailed status report
python scanner_watchdog.py --status

# Force start the scanner
python scanner_watchdog.py --start

# Stop scanner gracefully
python scanner_watchdog.py --stop
```

### Production Files Created
- `modulation_scanner.pid`: Process ID file with exclusive lock
- `modulation_scanner.status`: JSON status file with heartbeat and uptime
- `watchdog.log`: Watchdog monitoring and restart logs
- `maintenance.log`: System maintenance operation logs

### Cron/Scheduled Task Setup

#### Windows Task Scheduler
Create two scheduled tasks:

**Task 1: Full Maintenance (every 4 hours)**
- Program: `C:\Users\svdleer\Documents\Python\Source\multithreading\maintenance_cron_simple.bat`
- Schedule: Daily, repeat every 4 hours

**Task 2: Watchdog Check (every 5 minutes)**
- Program: `C:\Users\svdleer\Documents\Python\Source\multithreading\maintenance_cron_simple.bat`
- Arguments: `--watchdog-only`  
- Schedule: Daily, repeat every 5 minutes

#### Linux Cron
Add to crontab (`crontab -e`):
```bash
# Full maintenance every 4 hours
0 */4 * * * /path/to/maintenance_cron.sh

# Watchdog check every 5 minutes  
*/5 * * * * /path/to/maintenance_cron.sh --watchdog-only

# Optional: Web report updates every 15 minutes
*/15 * * * * cd /path/to/project && python report_modulation.py --json-only
```

### Watchdog Configuration
Edit `scanner_watchdog.py` to customize:
- `max_heartbeat_age = 300`: Maximum seconds since last heartbeat (5 minutes)
- `max_restart_attempts = 3`: Maximum restart attempts before giving up
- `restart_delay = 30`: Seconds between restart attempts

### Monitoring Commands

```cmd
# Check if scanner is running and healthy
python scanner_watchdog.py --status

# View recent watchdog activity
type watchdog.log

# Check process status manually
tasklist | findstr python

# View scanner status file contents
type modulation_scanner.status
```

### Production Safety Features

1. **Multi-Instance Prevention**: File locking prevents concurrent scanner processes
2. **Graceful Shutdown**: Signal handlers ensure clean database connections
3. **Health Monitoring**: Continuous heartbeat updates track scanner health
4. **Automatic Recovery**: Watchdog detects failures and restarts processes
5. **Stale Lock Cleanup**: Detects and removes locks from crashed processes
6. **Retry Logic**: Multiple restart attempts with delays between attempts

### Troubleshooting Production Issues

#### Scanner Won't Start
```cmd
# Check for existing processes
python scanner_watchdog.py --status

# Remove stale files if needed (Windows)
del modulation_scanner.pid modulation_scanner.status

# Force start
python scanner_watchdog.py --start
```

#### Watchdog Logs Show Continuous Restarts
1. Check main scanner logs for errors
2. Verify database connectivity
3. Check system resources (CPU, memory)
4. Review network access to CCAP devices

#### Database Connection Issues
```cmd
# Test database connection
python -c "from multithreading_base import MultithreadingBase; MultithreadingBase()"

# Run database maintenance
python database_maintenance.py
```

The production deployment system ensures maximum uptime and reliability through automated monitoring, graceful error handling, and intelligent restart capabilities.
