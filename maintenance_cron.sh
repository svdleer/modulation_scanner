#!/bin/bash

# Cron script for running modulation scanner maintenance with watchdog
# Add this to crontab with: crontab -e
# Example cron entries:
# # Run maintenance every 4 hours
# 0 */4 * * * /path/to/maintenance_cron.sh
# # Check scanner every 5 minutes (watchdog)
# */5 * * * * /path/to/maintenance_cron.sh --watchdog-only
# # Health check only
# 0 */6 * * * /path/to/maintenance_cron.sh --check-only

# Configuration - Update these paths for your environment
SCRIPT_DIR="/home/svdleer/scripts/python/modulation"  # Specific script directory
LOG_DIR="$SCRIPT_DIR"  # Use script directory for logs (no permission issues)
PYTHON_CMD="/home/svdleer/python/venv/bin/python"  # Use specific venv Python

# Set environment variables with full paths
export PATH="/usr/local/bin:/usr/bin:/bin:/sbin"
export PYTHONPATH="$SCRIPT_DIR"

# Change to script directory
cd "$SCRIPT_DIR"

# Virtual environment is already specified in PYTHON_CMD above
# source /path/to/venv/bin/activate

# Current timestamp for logging - use full path for date
TIMESTAMP=$(/bin/date '+%Y-%m-%d %H:%M:%S')
LOG_FILE="$LOG_DIR/maintenance.log"

# Function to log messages - use full paths
log_message() {
    /bin/echo "[$TIMESTAMP] $1" | /usr/bin/tee -a "$LOG_FILE"
}

# Parse command line arguments
CHECK_ONLY=false
REPORTS_ONLY=false
WATCHDOG_ONLY=false
FORCE=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --check-only)
            CHECK_ONLY=true
            shift
            ;;
        --reports-only)
            REPORTS_ONLY=true
            shift
            ;;
        --watchdog-only)
            WATCHDOG_ONLY=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            log_message "Unknown option: $1"
            log_message "Valid options: --check-only, --reports-only, --watchdog-only, --force, --dry-run"
            exit 1
            ;;
    esac
done

# Function to run database maintenance
run_maintenance() {
    log_message "Starting database maintenance..."
    
    local maintenance_args="--full"
    
    if [ "$FORCE" = true ]; then
        maintenance_args="$maintenance_args --force"
    fi
    
    if [ "$DRY_RUN" = true ]; then
        maintenance_args="$maintenance_args --dry-run"
    fi
    
    $PYTHON_CMD "$SCRIPT_DIR/database_maintenance.py" $maintenance_args 2>&1 | /usr/bin/tee -a "$LOG_FILE"
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        log_message "Database maintenance completed successfully"
    else
        log_message "ERROR: Database maintenance failed with exit code $exit_code"
        return 1
    fi
}

# Function to check table health
check_health() {
    log_message "Checking database health..."
    
    $PYTHON_CMD "$SCRIPT_DIR/database_maintenance.py" --analyze 2>&1 | /usr/bin/tee -a "$LOG_FILE"
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        log_message "Health check completed successfully"
    else
        log_message "WARNING: Health check failed with exit code $exit_code"
        return 1
    fi
}

# Function to run watchdog check
run_watchdog() {
    log_message "Running watchdog check..."
    
    if [ -f "$SCRIPT_DIR/scanner_watchdog.py" ]; then
        # Create a simple background process approach
        local watchdog_output
        local watchdog_pid
        
        # Start watchdog in background and capture PID
        log_message "Starting watchdog process..."
        $PYTHON_CMD "$SCRIPT_DIR/scanner_watchdog.py" --check > /tmp/watchdog_output.tmp 2>&1 &
        watchdog_pid=$!
        
        # Wait up to 60 seconds for completion
        local wait_count=0
        while [ $wait_count -lt 60 ]; do
            if ! /bin/ps -p $watchdog_pid > /dev/null 2>&1; then
                # Process completed
                break
            fi
            sleep 1
            wait_count=$((wait_count + 1))
        done
        
        # Check if process is still running (timeout)
        if /bin/ps -p $watchdog_pid > /dev/null 2>&1; then
            log_message "ERROR: Watchdog process timed out, killing it"
            kill $watchdog_pid 2>/dev/null || true
            wait $watchdog_pid 2>/dev/null || true
            return 1
        fi
        
        # Get exit code and output
        wait $watchdog_pid
        local exit_code=$?
        
        # Display output
        if [ -f /tmp/watchdog_output.tmp ]; then
            /bin/cat /tmp/watchdog_output.tmp | /usr/bin/tee -a "$LOG_FILE"
            /bin/rm -f /tmp/watchdog_output.tmp
        fi
        
        if [ $exit_code -eq 0 ]; then
            log_message "Watchdog check completed successfully"
        else
            log_message "WARNING: Watchdog check failed with exit code $exit_code"
            
            # Additional debugging: Check if scanner process is actually running
            # Check PID file in /tmp (moved to avoid NFS issues)
            if [ -f "/tmp/modulation_scanner.pid" ]; then
                local scanner_pid=$(/bin/cat "/tmp/modulation_scanner.pid" 2>/dev/null)
                if [ -n "$scanner_pid" ]; then
                    if /bin/ps -p "$scanner_pid" > /dev/null 2>&1; then
                        log_message "DEBUG: Scanner process $scanner_pid is running but unhealthy"
                        
                        # Check scanner status file (now in /tmp to avoid NFS issues)
                        if [ -f "/tmp/modulation_scanner.status" ]; then
                            log_message "DEBUG: Scanner status file contents:"
                            /bin/cat "/tmp/modulation_scanner.status" 2>&1 | /usr/bin/tee -a "$LOG_FILE"
                            log_message "DEBUG: Status file timestamp: $(/bin/ls -l "/tmp/modulation_scanner.status")"
                        else
                            log_message "DEBUG: Scanner status file not found at /tmp/modulation_scanner.status"
                        fi
                        
                        # Check scanner logs for errors
                        if [ -f "$SCRIPT_DIR/modulation_scanner.log" ]; then
                            log_message "DEBUG: Last 5 lines of scanner log:"
                            /usr/bin/tail -n 5 "$SCRIPT_DIR/modulation_scanner.log" 2>&1 | /usr/bin/tee -a "$LOG_FILE"
                        fi
                    else
                        log_message "DEBUG: Scanner process $scanner_pid is not running (crashed?)"
                    fi
                fi
            else
                log_message "DEBUG: Scanner PID file not found at /tmp/modulation_scanner.pid"
            fi
            return 1
        fi
    else
        log_message "WARNING: scanner_watchdog.py not found, skipping watchdog check"
    fi
}

# Function to generate reports
generate_reports() {
    log_message "Generating modulation reports..."
    
    if [ -f "$SCRIPT_DIR/modulation_report_generator.py" ]; then
        $PYTHON_CMD "$SCRIPT_DIR/modulation_report_generator.py" 2>&1 | /usr/bin/tee -a "$LOG_FILE"
    elif [ -f "$SCRIPT_DIR/report_modulation.py" ]; then
        $PYTHON_CMD "$SCRIPT_DIR/report_modulation.py" 2>&1 | /usr/bin/tee -a "$LOG_FILE"
    else
        log_message "WARNING: No report generator found"
        return 1
    fi
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        log_message "Report generation completed successfully"
    else
        log_message "ERROR: Report generation failed with exit code $exit_code"
        return 1
    fi
}

# Main execution logic
log_message "=== Maintenance Cron Job Started ==="

if [ "$WATCHDOG_ONLY" = true ]; then
    # Only run watchdog check
    run_watchdog
    EXIT_CODE=$?
    
elif [ "$CHECK_ONLY" = true ]; then
    # Only run health checks
    check_health
    EXIT_CODE=$?
    
elif [ "$REPORTS_ONLY" = true ]; then
    # Only generate reports
    generate_reports
    EXIT_CODE=$?
    
else
    # Full maintenance routine
    EXIT_CODE=0
    
    # Step 1: Check health first
    if ! check_health; then
        log_message "Health check failed, but continuing with maintenance..."
    fi
    
    # Step 2: Run maintenance
    if ! run_maintenance; then
        log_message "Maintenance failed"
        EXIT_CODE=1
    fi
    
    # Step 3: Run watchdog check to ensure scanner is running
    if ! run_watchdog; then
        log_message "Watchdog check failed, but continuing..."
    fi
    
    # Step 4: Generate reports (if maintenance succeeded or forced)
    if [ $EXIT_CODE -eq 0 ] || [ "$FORCE" = true ]; then
        if ! generate_reports; then
            log_message "Report generation failed"
            EXIT_CODE=1
        fi
    fi
fi

log_message "=== Maintenance Cron Job Completed (Exit Code: $EXIT_CODE) ==="

# Clean up old log files (keep last 30 days) - only in script directory
/usr/bin/find "$LOG_DIR" -name "maintenance*.log" -type f -mtime +30 -delete 2>/dev/null || true

exit $EXIT_CODE
