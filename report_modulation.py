#!/usr/bin/env python3

"""
Modulation Report Generator - Python version
Optimized replacement for reportmodulation.pl


"""

import os
import sys
import json
import smtplib
import pandas as pd
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
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
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
    def get_modulation_data_optimized(self):
        """
        Get modulation data with optimized queries - much faster than original Perl version
        
        Instead of nested subqueries for every row, we:
        1. Get all data in one query
        2. Process hops detection in Python with pandas
        3. Calculate percentages in bulk
        """
        try:
            self.logger.info("Fetching modulation data with optimized query...")
            
            # OPTIMIZED QUERY 1: Get all modulation data ordered by device/upstream/timestamp
            # This replaces the complex nested query with a simple, fast query
            base_query = """
                SELECT cmts, upstream, modulation, timestamp
                FROM modulation_new 
                ORDER BY cmts, upstream, timestamp
            """
            
            # Execute query and get pandas DataFrame
            conn = self.access_pool.get_connection()
            df = pd.read_sql(base_query, conn)
            conn.close()
            
            if df.empty:
                self.logger.warning("No modulation data found")
                return pd.DataFrame()
            
            self.logger.info(f"Retrieved {len(df)} modulation records")
            
            # OPTIMIZATION 2: Use pandas to detect modulation changes (hops) efficiently
            self.logger.info("Processing modulation hops...")
            
            # Group by device and upstream, then detect changes
            def detect_hops(group):
                """Detect modulation changes within a group"""
                # Sort by timestamp to ensure correct order
                group = group.sort_values('timestamp')
                
                # Mark rows where modulation changed from previous row
                group['modulation_changed'] = group['modulation'].ne(group['modulation'].shift())
                
                # First row of each group is always considered a change
                group.iloc[0, group.columns.get_loc('modulation_changed')] = True
                
                return group
            
            # Apply hop detection to each upstream
            df = df.groupby(['cmts', 'upstream']).apply(detect_hops).reset_index(drop=True)
            
            # Keep only rows where modulation actually changed (hops)
            hops_df = df[df['modulation_changed']].copy()
            
            self.logger.info(f"Found {len(hops_df)} modulation hops")
            
            # OPTIMIZATION 3: Calculate statistics in bulk using pandas groupby
            self.logger.info("Calculating modulation statistics...")
            
            # Group by cmts and upstream to get statistics
            stats_list = []
            
            for (cmts, upstream), group in df.groupby(['cmts', 'upstream']):
                # Count total measurements for this upstream
                total_measurements = len(group)
                
                # Count hops for this upstream
                hops = len(group[group['modulation_changed']])
                
                # Calculate percentages for each modulation type
                modulation_counts = group['modulation'].value_counts()
                
                qam64_pct = round((modulation_counts.get('QAM64', 0) / total_measurements) * 100)
                qam16_pct = round((modulation_counts.get('QAM16', 0) / total_measurements) * 100) 
                qpsk_pct = round((modulation_counts.get('QPSK', 0) / total_measurements) * 100)
                
                stats_list.append({
                    'cmts': cmts,
                    'upstream': upstream,
                    'hops': hops,
                    'qam64_pct': qam64_pct,
                    'qam16_pct': qam16_pct,
                    'qpsk_pct': qpsk_pct,
                    'measurements': total_measurements
                })
            
            # Convert to DataFrame and sort by hops descending, then by cmts
            result_df = pd.DataFrame(stats_list)
            result_df = result_df.sort_values(['hops', 'cmts'], ascending=[False, True])
            
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
            report_date = datetime.now().strftime('%Y-%m-%d')
            
        self.logger.info(f"Generating modulation report for {report_date}")
        
        # Try to load from cache first
        df = self.load_cached_report(report_date)
        
        if df is None:
            # Generate new report
            self.logger.info("Generating new report from database...")
            df = self.get_modulation_data_optimized()
            
            if df.empty:
                self.logger.error("No data available for report")
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
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)
            msg['Subject'] = f"Modulation report {report_date}"
            
            # Email body
            body = """LS,

Het meest recente modulation report is te vinden in de bijlage.

Met vriendelijke groet,
Modulation Scanner (Python)

Expert Engineer Access Engineering
Network & Technology - Access & Transport


Deze e-mail is automatisch verzonden"""
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach CSV file
            if csv_file and os.path.exists(csv_file):
                with open(csv_file, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= modulation_report_{report_date}.csv'
                )
                msg.attach(part)
            
            # Send email
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            server.sendmail(self.from_email, self.to_emails, msg.as_string())
            server.quit()
            
            self.logger.info(f"Email sent successfully to {len(self.to_emails)} recipients")
            
        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
    
    def generate_json_for_web(self, report_date=None):
        """Generate JSON file optimized for web display"""
        if report_date is None:
            report_date = datetime.now().strftime('%Y-%m-%d')
        
        # Get report data (from cache if available)
        df = self.load_cached_report(report_date)
        
        if df is None:
            df = self.get_modulation_data_optimized()
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
                'top_hoppers': df.head(10).to_dict('records'),
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
        return json_file
    
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
    
    def run_full_report(self, report_date=None):
        """Generate complete report with CSV, JSON and email"""
        if report_date is None:
            report_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            self.logger.info(f"Starting full report generation for {report_date}")
            
            # Generate CSV report
            csv_file = self.generate_csv_report(report_date)
            
            # Generate web JSON
            json_file = self.generate_json_for_web(report_date)
            
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
        python modulation_report_generator.py                    # Today's report
        python modulation_report_generator.py 2025-08-11        # Specific date
        python modulation_report_generator.py --json-only       # Only generate web JSON
        python modulation_report_generator.py --csv-only        # Only generate CSV
    """
    
    try:
        generator = ModulationReportGenerator()
        
        # Parse command line arguments
        report_date = None
        json_only = False
        csv_only = False
        
        if len(sys.argv) > 1:
            for arg in sys.argv[1:]:
                if arg == '--json-only':
                    json_only = True
                elif arg == '--csv-only':
                    csv_only = True
                elif arg.startswith('--'):
                    continue
                else:
                    # Assume it's a date
                    report_date = arg
        
        if json_only:
            generator.generate_json_for_web(report_date)
        elif csv_only:
            generator.generate_csv_report(report_date)
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
