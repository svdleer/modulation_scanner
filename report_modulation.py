#!/home/svdleer/python/venv/bin/python3

"""
Modulation Report Generator - Python version
Optimized replacement for reportmodulation.pl


"""

import os
import sys
import json
import smtplib
import pandas as pd
import subprocess
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
from sqlalchemy import create_engine
from multithreading_base import MultithreadingBase

load_dotenv()

class ModulationReportGenerator(MultithreadingBase):
    def __init__(self):
        """Initialize the report generator"""
        super().__init__()
        
        self.logger.info("ModulationReportGenerator initialized")
        
        # Report configuration
        self.output_dir = os.getenv('REPORT_OUTPUT_DIR', 'reports')
        self.enable_email = os.getenv('ENABLE_EMAIL_REPORTS', 'true').lower() == 'true'
        self.enable_cache = os.getenv('ENABLE_REPORT_CACHE', 'true').lower() == 'true'
        self.cache_duration_hours = int(os.getenv('CACHE_DURATION_HOURS', '1'))
        
        # Email configuration
        self.smtp_host = os.getenv('SMTP_HOST', 'localhost')
        self.smtp_port = int(os.getenv('SMTP_PORT', '25'))
        self.from_email = os.getenv('FROM_EMAIL', 'silvester.vanderleer@vodafoneziggo.com')
        self.to_emails = os.getenv('TO_EMAILS', 'silvester.vanderleer@vodafoneziggo.com').split(',')
        
        # Web deployment configuration
        self.web_server = os.getenv('WEB_SERVER', 'appdb-sh.oss.local')
        self.web_path = os.getenv('WEB_PATH', '/var/www/modulation/reports')
        self.enable_web_deployment = os.getenv('ENABLE_WEB_DEPLOYMENT', 'true').lower() == 'true'
        self.housekeeping_days = int(os.getenv('HOUSEKEEPING_DAYS', '30'))
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Log email configuration for debugging
        self.logger.info(f"Email configuration: enable={self.enable_email}, host={self.smtp_host}, port={self.smtp_port}")
        self.logger.info(f"From: {self.from_email}, To: {self.to_emails}")
        
        # Create SQLAlchemy engine for pandas compatibility
        self._create_sqlalchemy_engine()
        
    def _create_sqlalchemy_engine(self):
        """Create SQLAlchemy engine for pandas compatibility"""
        try:
            # Get database credentials from config
            access_config = self.config['ACCESS']
            
            # Create SQLAlchemy connection string
            connection_string = (
                f"mysql+mysqlconnector://{access_config['USER']}:"
                f"{access_config['PASSWORD']}@{access_config['HOST']}/"
                f"{access_config['DATABASE']}"
            )
            
            self.sqlalchemy_engine = create_engine(
                connection_string,
                pool_size=access_config['POOL_SIZE'],
                max_overflow=0,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            self.logger.info("SQLAlchemy engine created successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to create SQLAlchemy engine: {str(e)}")
            # Fall back to None, will use mysql connector directly
            self.sqlalchemy_engine = None
        
    def get_modulation_data_optimized(self, report_date=None):
        """
        Get modulation data with optimized queries for specific date range
        
        Args:
            report_date: Date string in 'YYYY-MM-DD' format. If None, uses previous day.
        
        Instead of nested subqueries for every row, we:
        1. Get data for specific date range in one query
        2. Process hops detection in Python with pandas
        3. Calculate percentages in bulk
        """
        try:
            # Default to previous day if no date specified
            if report_date is None:
                report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            self.logger.info(f"Fetching modulation data for date range: {report_date} 00:00 to 23:59")
            
            # OPTIMIZED QUERY with proper date filtering
            # Get data from 00:00:00 to 23:59:59 of the specified date
            date_filtered_query = """
                SELECT cmts, upstream, modulation, timestamp
                FROM modulation_new 
                WHERE DATE(timestamp) = %s
                ORDER BY cmts, upstream, timestamp
            """
            
            # Execute query and get pandas DataFrame using SQLAlchemy engine
            if self.sqlalchemy_engine:
                # Use SQLAlchemy engine (preferred by pandas)
                # SQLAlchemy expects params as a dictionary or tuple, not a list
                df = pd.read_sql(date_filtered_query, self.sqlalchemy_engine, params=(report_date,))
            else:
                # Fall back to direct mysql connector (with warning)
                conn = self.access_pool.get_connection()
                df = pd.read_sql(date_filtered_query, conn, params=[report_date])
                conn.close()
            
            if df.empty:
                self.logger.warning(f"No modulation data found for {report_date}")
                return pd.DataFrame()
            
            self.logger.info(f"Retrieved {len(df)} modulation records for {report_date}")
            
            # OPTIMIZATION 2: Use pandas to detect modulation changes (hops) efficiently
            self.logger.info("Processing modulation hops...")
            
            # MUCH FASTER approach: Use pandas vectorized operations instead of groupby.apply
            # Sort all data by device, upstream, and timestamp
            df = df.sort_values(['cmts', 'upstream', 'timestamp'])
            
            # Create a unique identifier for each upstream
            df['upstream_id'] = df['cmts'] + '_' + df['upstream'].astype(str)
            
            # Detect modulation changes using vectorized shift operations
            # Mark where upstream_id changes (new upstream starts)
            df['new_upstream'] = df['upstream_id'] != df['upstream_id'].shift()
            
            # Mark where modulation changes within the same upstream
            df['modulation_changed'] = (
                df['new_upstream'] |  # First record of each upstream
                ((df['modulation'] != df['modulation'].shift()) & ~df['new_upstream'])  # Modulation change within upstream
            )
            
            # Count total hops per upstream (much faster than groupby)
            hop_counts = df[df['modulation_changed']].groupby('upstream_id').size()
            
            self.logger.info(f"Found {df['modulation_changed'].sum()} total modulation hops")
            
            # OPTIMIZATION 3: Calculate statistics in bulk using pandas groupby (vectorized)
            self.logger.info("Calculating modulation statistics...")
            
            # Calculate statistics using vectorized pandas operations
            # Group by upstream_id for all calculations
            upstream_groups = df.groupby('upstream_id')
            
            # Get basic stats for each upstream
            upstream_stats = pd.DataFrame({
                'measurements': upstream_groups.size(),
                'hops': hop_counts,
                'cmts': upstream_groups['cmts'].first(),
                'upstream': upstream_groups['upstream'].first()
            }).fillna(0)  # Fill NaN hops with 0
            
            # Calculate modulation percentages using value_counts
            modulation_stats_list = []
            
            for upstream_id, group in upstream_groups:
                total_measurements = len(group)
                modulation_counts = group['modulation'].value_counts()
                
                qam64_pct = round((modulation_counts.get('QAM64', 0) / total_measurements) * 100)
                qam16_pct = round((modulation_counts.get('QAM16', 0) / total_measurements) * 100)
                qpsk_pct = round((modulation_counts.get('QPSK', 0) / total_measurements) * 100)
                
                modulation_stats_list.append({
                    'upstream_id': upstream_id,
                    'qam64_pct': qam64_pct,
                    'qam16_pct': qam16_pct,
                    'qpsk_pct': qpsk_pct
                })
            
            # Convert to DataFrame and merge with upstream_stats
            modulation_stats_df = pd.DataFrame(modulation_stats_list).set_index('upstream_id')
            result_df = upstream_stats.join(modulation_stats_df)
            
            # Convert hops to int and sort by hops descending, then by cmts
            result_df['hops'] = result_df['hops'].astype(int)
            result_df = result_df.sort_values(['hops', 'cmts'], ascending=[False, True])
            
            # Select final columns
            result_df = result_df[['cmts', 'upstream', 'hops', 'qam64_pct', 'qam16_pct', 'qpsk_pct', 'measurements']].reset_index(drop=True)
            
            self.logger.info(f"Generated statistics for {len(result_df)} upstream interfaces")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Failed to get modulation data: {str(e)}")
            raise
    
    def get_cached_report_path(self, report_date):
        """Get the path for a cached report file"""
        return os.path.join(self.output_dir, f"modulation_report_{report_date}_cached.json")
    
    def get_csv_report_path(self, report_date):
        """Get the path for a CSV report file"""
        return os.path.join(self.output_dir, f"modulation_report_{report_date}.csv")
    
    def load_cached_report(self, report_date):
        """Load cached report if available and not expired"""
        if not self.enable_cache:
            return None
            
        cache_file = self.get_cached_report_path(report_date)
        
        if not os.path.exists(cache_file):
            return None
            
        try:
            # Check if cache is expired
            cache_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
            if cache_age.total_seconds() > (self.cache_duration_hours * 3600):
                self.logger.info(f"Cache expired ({cache_age}), regenerating report")
                return None
            
            # Load cached data
            with open(cache_file, 'r') as f:
                cached_data = json.load(f)
                
            self.logger.info(f"Using cached report from {cache_age} ago")
            return pd.DataFrame(cached_data)
            
        except Exception as e:
            self.logger.warning(f"Failed to load cached report: {str(e)}")
            return None
    
    def save_cached_report(self, df, report_date):
        """Save report data to cache"""
        if not self.enable_cache:
            return
            
        try:
            cache_file = self.get_cached_report_path(report_date)
            
            # Convert DataFrame to JSON and save
            with open(cache_file, 'w') as f:
                json.dump(df.to_dict('records'), f, indent=2)
                
            self.logger.info(f"Saved report cache to {cache_file}")
            
        except Exception as e:
            self.logger.warning(f"Failed to save report cache: {str(e)}")
    
    def generate_csv_report(self, report_date=None):
        """Generate CSV report for the specified date"""
        if report_date is None:
            # Default to previous day (yesterday)
            report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
        self.logger.info(f"Generating modulation report for {report_date}")
        
        # Try to load from cache first
        df = self.load_cached_report(report_date)
        
        if df is None:
            # Generate new report
            self.logger.info("Generating new report from database...")
            df = self.get_modulation_data_optimized(report_date)
            
            if df.empty:
                self.logger.error(f"No data available for report on {report_date}")
                return None
                
            # Save to cache
            self.save_cached_report(df, report_date)
        
        # Generate CSV file
        csv_file = self.get_csv_report_path(report_date)
        
        # Write CSV with proper header matching Perl version
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            f.write("CMTS;upstream;hops;%QAM64;%QAM16;%QPSK;Metingen\n")
            
            for _, row in df.iterrows():
                line = f"{row['cmts']};{row['upstream']};{row['hops']};{row['qam64_pct']};{row['qam16_pct']};{row['qpsk_pct']};{row['measurements']}\n"
                f.write(line)
        
        self.logger.info(f"CSV report generated: {csv_file}")
        return csv_file
    
    def send_email_report(self, csv_file, report_date):
        """Send email report with CSV attachment"""
        if not self.enable_email:
            self.logger.info("Email reports disabled, skipping email")
            return
            
        try:
            self.logger.info("Sending email report...")
            self.logger.info(f"SMTP Host: {self.smtp_host}, Port: {self.smtp_port}")
            self.logger.info(f"From: {self.from_email}")
            self.logger.info(f"To: {self.to_emails}")
            self.logger.info(f"CSV file: {csv_file}")
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)
            msg['Subject'] = f"Modulation report {report_date}"
            
            # Email body
            body = """LS,

Het meest recente modulation report is te vinden in de bijlage.

Met vriendelijke groet,
Modulation Scanner 

Expert Engineer Access Engineering
Network & Technology - Access & Transport


Deze e-mail is automatisch verzonden"""
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach CSV file
            if csv_file and os.path.exists(csv_file):
                self.logger.info(f"Attaching CSV file: {csv_file}")
                with open(csv_file, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename=modulation_report_{report_date}.csv'
                )
                msg.attach(part)
            else:
                self.logger.warning(f"CSV file not found or invalid: {csv_file}")
            
            # Send email with enhanced error handling
            self.logger.info("Connecting to SMTP server...")
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            
            # Enable debugging for SMTP
            server.set_debuglevel(1)
            
            self.logger.info("Sending email...")
            send_result = server.sendmail(self.from_email, self.to_emails, msg.as_string())
            server.quit()
            
            # Check if there were any rejected recipients
            if send_result:
                self.logger.warning(f"Some recipients were rejected: {send_result}")
            else:
                self.logger.info(f"Email sent successfully to {len(self.to_emails)} recipients: {self.to_emails}")
            
            # Log message details for debugging
            self.logger.info(f"Email subject: {msg['Subject']}")
            self.logger.info(f"Message size: {len(msg.as_string())} bytes")
            
        except smtplib.SMTPException as e:
            self.logger.error(f"SMTP Error: {str(e)}")
            self.logger.error(f"Error type: {type(e).__name__}")
        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
            self.logger.error(f"Error type: {type(e).__name__}")
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
    
    def generate_json_for_web(self, report_date=None):
        """Generate JSON file optimized for web display"""
        if report_date is None:
            # Default to previous day (yesterday)
            report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Get report data (from cache if available)
        df = self.load_cached_report(report_date)
        
        if df is None:
            df = self.get_modulation_data_optimized(report_date)
            if not df.empty:
                self.save_cached_report(df, report_date)
        
        if df.empty:
            return None
        
        # Create web-optimized JSON structure
        web_data = {
            'generated_at': datetime.now().isoformat(),
            'report_date': report_date,
            'total_upstreams': len(df),
            'total_hops': int(df['hops'].sum()),
            'summary': {
                'top_hoppers': df.to_dict('records'),  # Include ALL devices, not just top 10
                'by_device_type': self._get_device_type_summary(df),
                'modulation_distribution': self._get_modulation_distribution(df)
            },
            'full_data': df.to_dict('records')
        }
        
        # Save JSON file for web
        json_file = os.path.join(self.output_dir, f"modulation_report_{report_date}_web.json")
        with open(json_file, 'w') as f:
            json.dump(web_data, f, indent=2)
        
        self.logger.info(f"Web JSON report generated: {json_file}")
        
        # Deploy to web server if enabled
        if self.enable_web_deployment:
            # Ensure web directory exists first
            self.ensure_web_directory()
            self.deploy_to_web_server(json_file, report_date)
            
        return json_file
    
    def deploy_to_web_server(self, json_file, report_date):
        """Deploy JSON file to web server with proper permissions and housekeeping"""
        if not json_file or not os.path.exists(json_file):
            self.logger.error("JSON file not found for web deployment")
            return False
            
        try:
            self.logger.info(f"Deploying JSON report to web server {self.web_server}...")
            
            # Step 1: Copy file to temporary location first (user's home directory)
            temp_file = f"/tmp/modulation_report_{report_date}_web.json"
            scp_command = [
                'scp', 
                json_file, 
                f"{self.web_server}:{temp_file}"
            ]
            
            self.logger.info(f"Running: {' '.join(scp_command)}")
            result = subprocess.run(scp_command, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                self.logger.error(f"SCP failed: {result.stderr}")
                return False
            
            self.logger.info("File copied to temporary location on web server")
            
            # Step 2: Move file to final location with sudo and set proper ownership
            final_file = f"{self.web_path}/modulation_report_{report_date}_web.json"
            deployment_commands = [
                f"sudo mkdir -p {self.web_path}",
                f"sudo mv {temp_file} {final_file}",
                f"sudo chown www-data:www-data {final_file}",
                f"sudo chmod 644 {final_file}"
            ]
            
            for cmd in deployment_commands:
                ssh_command = ['ssh', self.web_server, cmd]
                self.logger.info(f"Running: {cmd}")
                
                result = subprocess.run(ssh_command, capture_output=True, text=True, timeout=15)
                if result.returncode != 0:
                    self.logger.error(f"Command failed: {cmd} - {result.stderr}")
                    # Try to clean up temp file if something fails
                    cleanup_cmd = ['ssh', self.web_server, f"rm -f {temp_file}"]
                    subprocess.run(cleanup_cmd, capture_output=True, text=True, timeout=10)
                    return False
                else:
                    self.logger.info(f"Successfully executed: {cmd}")
            
            # Step 3: Housekeeping - remove old files
            self.perform_web_housekeeping()
            
            self.logger.info("Web deployment completed successfully")
            return True
            
        except subprocess.TimeoutExpired:
            self.logger.error("Web deployment timed out")
            return False
        except Exception as e:
            self.logger.error(f"Web deployment failed: {str(e)}")
            return False
    
    def perform_web_housekeeping(self):
        """Remove old JSON files from web server to prevent disk space issues"""
        try:
            self.logger.info(f"Performing housekeeping on web server (keeping last {self.housekeeping_days} days)...")
            
            # Command to find and remove old files
            cleanup_command = (
                f"find {self.web_path} -name 'modulation_report_*_web.json' "
                f"-type f -mtime +{self.housekeeping_days} -delete"
            )
            
            ssh_command = ['ssh', self.web_server, cleanup_command]
            self.logger.info(f"Running housekeeping: {cleanup_command}")
            
            result = subprocess.run(ssh_command, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                self.logger.info("Housekeeping completed successfully")
                if result.stdout.strip():
                    self.logger.info(f"Housekeeping output: {result.stdout.strip()}")
            else:
                self.logger.warning(f"Housekeeping warning: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            self.logger.error("Housekeeping timed out")
        except Exception as e:
            self.logger.error(f"Housekeeping failed: {str(e)}")
    
    def create_web_symlink(self, report_date):
        """Create a symlink for 'latest' report on web server"""
        try:
            latest_link = f"{self.web_path}/modulation_report_latest_web.json"
            target_file = f"modulation_report_{report_date}_web.json"
            
            symlink_commands = [
                f"cd {self.web_path}",
                f"sudo rm -f modulation_report_latest_web.json",
                f"sudo ln -s {target_file} modulation_report_latest_web.json",
                f"sudo chown -h www-data:www-data modulation_report_latest_web.json"
            ]
            
            # Execute commands as a single compound command
            combined_command = " && ".join(symlink_commands)
            ssh_command = ['ssh', self.web_server, combined_command]
            
            self.logger.info("Creating 'latest' symlink on web server...")
            result = subprocess.run(ssh_command, capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0:
                self.logger.info("Latest symlink created successfully")
            else:
                self.logger.warning(f"Symlink creation warning: {result.stderr}")
                
        except Exception as e:
            self.logger.error(f"Symlink creation failed: {str(e)}")
    
    def perform_web_housekeeping(self):
        """Remove old JSON files from web server to prevent disk space issues"""
        try:
            self.logger.info(f"Performing housekeeping on web server (keeping last {self.housekeeping_days} days)...")
            
            # Command to find and remove old files with sudo
            cleanup_command = (
                f"sudo find {self.web_path} -name 'modulation_report_*_web.json' "
                f"-type f -mtime +{self.housekeeping_days} -delete"
            )
            
            ssh_command = ['ssh', self.web_server, cleanup_command]
            self.logger.info(f"Running housekeeping: {cleanup_command}")
            
            result = subprocess.run(ssh_command, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                self.logger.info("Housekeeping completed successfully")
                if result.stdout.strip():
                    self.logger.info(f"Housekeeping output: {result.stdout.strip()}")
            else:
                self.logger.warning(f"Housekeeping warning: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            self.logger.error("Housekeeping timed out")
        except Exception as e:
            self.logger.error(f"Housekeeping failed: {str(e)}")
    
    def ensure_web_directory(self):
        """Ensure web directory exists with proper permissions"""
        try:
            self.logger.info("Ensuring web directory exists with proper permissions...")
            
            setup_commands = [
                f"sudo mkdir -p {self.web_path}",
                f"sudo chown www-data:www-data {self.web_path}",
                f"sudo chmod 755 {self.web_path}"
            ]
            
            for cmd in setup_commands:
                ssh_command = ['ssh', self.web_server, cmd]
                result = subprocess.run(ssh_command, capture_output=True, text=True, timeout=15)
                
                if result.returncode != 0:
                    self.logger.warning(f"Directory setup warning: {cmd} - {result.stderr}")
                else:
                    self.logger.info(f"Successfully executed: {cmd}")
                    
        except Exception as e:
            self.logger.error(f"Directory setup failed: {str(e)}")
    
    def _get_device_type_summary(self, df):
        """Get summary statistics by device type"""
        try:
            device_summary = []
            
            # Extract device type from CMTS name (CCAP0xx, CCAP1xx, CCAP2xx)
            df['device_type'] = df['cmts'].str.extract(r'(CCAP[012])\d+')[0]
            
            for device_type, group in df.groupby('device_type'):
                if pd.isna(device_type):
                    continue
                    
                device_summary.append({
                    'device_type': device_type,
                    'total_upstreams': len(group),
                    'total_hops': int(group['hops'].sum()),
                    'avg_qam64_pct': round(group['qam64_pct'].mean(), 1),
                    'avg_qam16_pct': round(group['qam16_pct'].mean(), 1),
                    'avg_qpsk_pct': round(group['qpsk_pct'].mean(), 1)
                })
            
            return device_summary
            
        except Exception as e:
            self.logger.warning(f"Failed to generate device type summary: {str(e)}")
            return []
    
    def _get_modulation_distribution(self, df):
        """Get overall modulation distribution statistics"""
        try:
            # Weight by number of measurements per upstream
            total_measurements = df['measurements'].sum()
            
            if total_measurements == 0:
                return {}
            
            # Calculate weighted averages
            weighted_qam64 = (df['qam64_pct'] * df['measurements']).sum() / total_measurements
            weighted_qam16 = (df['qam16_pct'] * df['measurements']).sum() / total_measurements  
            weighted_qpsk = (df['qpsk_pct'] * df['measurements']).sum() / total_measurements
            
            return {
                'overall_qam64_pct': round(weighted_qam64, 1),
                'overall_qam16_pct': round(weighted_qam16, 1),
                'overall_qpsk_pct': round(weighted_qpsk, 1),
                'total_measurements': int(total_measurements)
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to generate modulation distribution: {str(e)}")
            return {}
    
    def run_web_only_report(self, report_date=None):
        """Generate web JSON and copy as normal file, no email"""
        if report_date is None:
            # Default to previous day (yesterday)
            report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        try:
            self.logger.info(f"Starting web-only report generation for {report_date}")
            
            # Generate web JSON
            web_json_file = self.generate_json_for_web(report_date)
            
            if not web_json_file:
                return {
                    'success': False,
                    'error': 'Failed to generate web JSON',
                    'report_date': report_date
                }
            
            # Copy web JSON as normal file
            import shutil
            normal_json_file = os.path.join(self.output_dir, f"modulation_report_{report_date}.json")
            shutil.copy2(web_json_file, normal_json_file)
            
            self.logger.info(f"Web JSON copied as normal file: {normal_json_file}")
            self.logger.info("Web-only report generation completed successfully (no email sent)")
            
            return {
                'web_json_file': web_json_file,
                'normal_json_file': normal_json_file,
                'report_date': report_date,
                'success': True
            }
            
        except Exception as e:
            self.logger.error(f"Web-only report generation failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'report_date': report_date
            }

    def run_full_report(self, report_date=None):
        """Generate complete report with CSV, JSON and email"""
        if report_date is None:
            # Default to previous day (yesterday)
            report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        try:
            self.logger.info(f"Starting full report generation for {report_date}")
            
            # Generate CSV report
            csv_file = self.generate_csv_report(report_date)
            
            # Generate web JSON
            json_file = self.generate_json_for_web(report_date)
            
            # Create latest symlink on web server
            if json_file and self.enable_web_deployment:
                self.create_web_symlink(report_date)
            
            # Send email if enabled
            if csv_file:
                self.send_email_report(csv_file, report_date)
            
            self.logger.info("Full report generation completed successfully")
            
            return {
                'csv_file': csv_file,
                'json_file': json_file,
                'report_date': report_date,
                'success': True
            }
            
        except Exception as e:
            self.logger.error(f"Full report generation failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'report_date': report_date
            }

if __name__ == "__main__":
    """
    Run report generator
    
    Usage:
        python report_modulation.py                    # Today's report
        python report_modulation.py 2025-08-11        # Specific date
        python report_modulation.py --json-only       # Only generate web JSON
        python report_modulation.py --csv-only        # Only generate CSV
        python report_modulation.py --web-only        # Generate web JSON and copy as normal file, no email
    """
    
    def show_help():
        """Display help information"""
        help_text = """
Modulation Report Generator - Python version
Optimized replacement for reportmodulation.pl

USAGE:
    python report_modulation.py [OPTIONS] [DATE]

ARGUMENTS:
    DATE                    Report date in YYYY-MM-DD format (default: yesterday)

OPTIONS:
    --help, -h             Show this help message and exit
    --json-only            Generate only web JSON file (no CSV, no email)
    --csv-only             Generate only CSV file (no JSON, no email)  
    --web-only             Generate web JSON and copy as normal file (no email)
    
EXAMPLES:
    python report_modulation.py
        Generate full report for yesterday (CSV, JSON, email, web deployment)
        
    python report_modulation.py 2025-08-12
        Generate full report for specific date
        
    python report_modulation.py --json-only
        Generate only web JSON for yesterday
        
    python report_modulation.py --csv-only 2025-08-12
        Generate only CSV for specific date
        
    python report_modulation.py --web-only
        Generate web JSON and copy as normal file, no email sent

ENVIRONMENT VARIABLES:
    REPORT_OUTPUT_DIR      Output directory for reports (default: reports)
    ENABLE_EMAIL_REPORTS   Enable/disable email reports (default: true)
    ENABLE_REPORT_CACHE    Enable/disable report caching (default: true)
    CACHE_DURATION_HOURS   Cache duration in hours (default: 1)
    SMTP_HOST             SMTP server hostname (default: localhost)
    SMTP_PORT             SMTP server port (default: 25)
    FROM_EMAIL            From email address
    TO_EMAILS             Comma-separated list of recipient emails
    WEB_SERVER            Web server hostname for deployment
    WEB_PATH              Web server path for JSON files
    ENABLE_WEB_DEPLOYMENT Enable/disable web deployment (default: true)
    HOUSEKEEPING_DAYS     Days to keep old files on web server (default: 30)

OUTPUT FILES:
    modulation_report_YYYY-MM-DD.csv           CSV report file
    modulation_report_YYYY-MM-DD_web.json     Web-optimized JSON file
    modulation_report_YYYY-MM-DD.json         Normal JSON file (--web-only)
    modulation_report_YYYY-MM-DD_cached.json  Cached report data

DESCRIPTION:
    This tool generates modulation reports from the modulation database.
    It analyzes modulation hops and generates statistics for upstream interfaces.
    Reports can be generated in CSV and JSON formats, sent via email,
    and deployed to a web server for visualization.
        """
        print(help_text)
    
    try:
        generator = ModulationReportGenerator()
        
        # Parse command line arguments
        report_date = None
        json_only = False
        csv_only = False
        web_only = False
        show_help_flag = False
        
        if len(sys.argv) > 1:
            for arg in sys.argv[1:]:
                if arg in ['--help', '-h']:
                    show_help_flag = True
                elif arg == '--json-only':
                    json_only = True
                elif arg == '--csv-only':
                    csv_only = True
                elif arg == '--web-only':
                    web_only = True
                elif arg.startswith('--'):
                    print(f"Unknown option: {arg}")
                    print("Use --help for usage information")
                    sys.exit(1)
                else:
                    # Assume it's a date
                    report_date = arg
        
        if show_help_flag:
            show_help()
            sys.exit(0)
        
        if json_only:
            result = generator.generate_json_for_web(report_date)
            if result:
                print(f"Web JSON generated: {result}")
            else:
                print("Failed to generate web JSON")
                sys.exit(1)
        elif csv_only:
            result = generator.generate_csv_report(report_date)
            if result:
                print(f"CSV generated: {result}")
            else:
                print("Failed to generate CSV")
                sys.exit(1)
        elif web_only:
            result = generator.run_web_only_report(report_date)
            if result['success']:
                print(f"Web-only report generated successfully for {result['report_date']}")
                print(f"Web JSON: {result['web_json_file']}")
                print(f"Normal JSON: {result['normal_json_file']}")
            else:
                print(f"Web-only report generation failed: {result['error']}")
                sys.exit(1)
        else:
            # Full report
            result = generator.run_full_report(report_date)
            
            if result['success']:
                print(f"Report generated successfully for {result['report_date']}")
                if result.get('csv_file'):
                    print(f"CSV: {result['csv_file']}")
                if result.get('json_file'):
                    print(f"JSON: {result['json_file']}")
            else:
                print(f"Report generation failed: {result['error']}")
                sys.exit(1)
                
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)
