#!/home/svdleer/python/venv/bin/python3

"""
Database Maintenance Module
Prevents AUTO_INCREMENT overflow by implementing smart cleanup strategies

This module solves the "running out of database IDs" issue by:
1. Automatic cleanup of old data
2. Smart batch processing to avoid performance issues  
3. AUTO_INCREMENT reset when needed
4. Monitoring and alerting
"""

import os
import sys
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from multithreading_base import MultithreadingBase

load_dotenv()

class DatabaseMaintenance(MultithreadingBase):
    def __init__(self):
        """Initialize database maintenance"""
        super().__init__()
        
        self.logger.info("DatabaseMaintenance initialized")
        
        # Maintenance configuration from environment
        self.enable_auto_cleanup = os.getenv('ENABLE_AUTO_CLEANUP', 'true').lower() == 'true'
        self.data_retention_days = int(os.getenv('DATA_RETENTION_DAYS', '7'))
        self.cleanup_batch_size = int(os.getenv('CLEANUP_BATCH_SIZE', '10000'))
        self.enable_id_reset = os.getenv('ENABLE_ID_RESET', 'true').lower() == 'true'
        self.id_reset_threshold = int(os.getenv('ID_RESET_THRESHOLD', '80'))  # Percentage
        self.cleanup_schedule_hour = int(os.getenv('CLEANUP_SCHEDULE_HOUR', '2'))  # 2 AM
        
        # Table configuration - detect which table to use
        self.table_name = 'modulation_new'  # Primary table
        self.fallback_table = 'modulation'   # Fallback table
        
        self.logger.info(f"Maintenance config: retention={self.data_retention_days}d, "
                        f"batch_size={self.cleanup_batch_size}, auto_cleanup={self.enable_auto_cleanup}")
    
    def get_table_info(self, table_name):
        """Get detailed information about table structure and AUTO_INCREMENT status"""
        try:
            conn = self.access_pool.get_connection()
            cursor = conn.cursor()
            
            # Get table status including AUTO_INCREMENT info
            cursor.execute(f"SHOW TABLE STATUS LIKE '{table_name}'")
            table_status = cursor.fetchone()
            
            if not table_status:
                return None
            
            # Get column information to find AUTO_INCREMENT column
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = cursor.fetchall()
            
            # Find AUTO_INCREMENT column
            auto_inc_column = None
            for col in columns:
                if 'auto_increment' in col[5].lower():  # Extra field contains auto_increment
                    auto_inc_column = col[0]
                    break
            
            # Get current max ID
            if auto_inc_column:
                cursor.execute(f"SELECT MAX({auto_inc_column}) FROM {table_name}")
                max_id_result = cursor.fetchone()
                max_id = max_id_result[0] if max_id_result[0] else 0
            else:
                max_id = 0
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            # Get oldest record timestamp
            cursor.execute(f"SELECT MIN(timestamp) FROM {table_name}")
            oldest_record = cursor.fetchone()[0]
            
            conn.close()
            
            # Determine column type and maximum possible value
            auto_inc_info = table_status[10] if len(table_status) > 10 else 0  # Auto_increment value
            
            # Common AUTO_INCREMENT limits
            int_limits = {
                'INT': 2147483647,          # 2^31 - 1
                'BIGINT': 9223372036854775807,  # 2^63 - 1
                'MEDIUMINT': 8388607,       # 2^23 - 1
                'SMALLINT': 32767           # 2^15 - 1
            }
            
            # Try to determine column type (simplified)
            max_possible = int_limits.get('INT', 2147483647)  # Default to INT
            
            return {
                'table_name': table_name,
                'row_count': row_count,
                'current_auto_increment': auto_inc_info,
                'max_id_used': max_id,
                'max_possible_id': max_possible,
                'usage_percentage': (auto_inc_info / max_possible) * 100 if auto_inc_info else 0,
                'auto_increment_column': auto_inc_column,
                'oldest_record': oldest_record,
                'table_exists': True
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {str(e)}")
            return {'table_exists': False, 'error': str(e)}
    
    def cleanup_old_data(self, table_name=None, dry_run=False):
        """
        Clean up old data in batches to prevent performance issues
        
        Args:
            table_name: Table to clean up (defaults to self.table_name)
            dry_run: If True, only log what would be deleted without actually deleting
        """
        if table_name is None:
            table_name = self.table_name
        
        try:
            cutoff_date = datetime.now() - timedelta(days=self.data_retention_days)
            
            self.logger.info(f"{'DRY RUN: ' if dry_run else ''}Cleaning up {table_name} data older than {cutoff_date}")
            
            conn = self.access_pool.get_connection()
            cursor = conn.cursor()
            
            # First, count how many records will be affected
            count_query = f"SELECT COUNT(*) FROM {table_name} WHERE timestamp < %s"
            cursor.execute(count_query, (cutoff_date,))
            total_to_delete = cursor.fetchone()[0]
            
            if total_to_delete == 0:
                self.logger.info(f"No old records found in {table_name}")
                conn.close()
                return 0
            
            self.logger.info(f"Found {total_to_delete:,} records to delete from {table_name}")
            
            if dry_run:
                conn.close()
                return total_to_delete
            
            # Delete in batches to avoid locking the table for too long
            total_deleted = 0
            batch_count = 0
            
            while total_deleted < total_to_delete:
                batch_count += 1
                
                # Delete a batch
                delete_query = f"DELETE FROM {table_name} WHERE timestamp < %s LIMIT %s"
                cursor.execute(delete_query, (cutoff_date, self.cleanup_batch_size))
                deleted_in_batch = cursor.rowcount
                
                if deleted_in_batch == 0:
                    break  # No more records to delete
                
                total_deleted += deleted_in_batch
                conn.commit()
                
                self.logger.info(f"Batch {batch_count}: Deleted {deleted_in_batch:,} records "
                               f"({total_deleted:,}/{total_to_delete:,} total)")
                
                # Small delay between batches to reduce database load
                if batch_count % 10 == 0:  # Every 10 batches
                    time.sleep(1)
            
            conn.close()
            
            self.logger.info(f"Cleanup completed: {total_deleted:,} records deleted from {table_name}")
            return total_deleted
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old data from {table_name}: {str(e)}")
            if 'conn' in locals():
                conn.close()
            raise
    
    def reset_auto_increment(self, table_name=None, dry_run=False):
        """
        Reset AUTO_INCREMENT counter to optimize ID usage
        
        This is safe to do after cleanup - it sets AUTO_INCREMENT to MAX(id) + 1
        """
        if table_name is None:
            table_name = self.table_name
        
        try:
            conn = self.access_pool.get_connection()
            cursor = conn.cursor()
            
            # Get current maximum ID
            table_info = self.get_table_info(table_name)
            if not table_info or not table_info['table_exists']:
                self.logger.error(f"Cannot reset AUTO_INCREMENT: table {table_name} not found")
                return False
            
            max_id = table_info['max_id_used']
            new_auto_increment = max_id + 1
            
            self.logger.info(f"{'DRY RUN: ' if dry_run else ''}Resetting {table_name} AUTO_INCREMENT from "
                           f"{table_info['current_auto_increment']} to {new_auto_increment}")
            
            if not dry_run:
                reset_query = f"ALTER TABLE {table_name} AUTO_INCREMENT = %s"
                cursor.execute(reset_query, (new_auto_increment,))
                conn.commit()
                
                self.logger.info(f"AUTO_INCREMENT reset successful for {table_name}")
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to reset AUTO_INCREMENT for {table_name}: {str(e)}")
            if 'conn' in locals():
                conn.close()
            return False
    
    def analyze_table_health(self, table_name=None):
        """Analyze table health and provide recommendations"""
        if table_name is None:
            table_name = self.table_name
        
        try:
            table_info = self.get_table_info(table_name)
            
            if not table_info or not table_info['table_exists']:
                self.logger.error(f"Cannot analyze {table_name}: table not found")
                return None
            
            analysis = {
                'table_name': table_name,
                'health_status': 'HEALTHY',
                'warnings': [],
                'recommendations': [],
                'stats': table_info
            }
            
            # Check AUTO_INCREMENT usage
            usage_pct = table_info['usage_percentage']
            if usage_pct > self.id_reset_threshold:
                analysis['health_status'] = 'CRITICAL'
                analysis['warnings'].append(f"AUTO_INCREMENT usage at {usage_pct:.1f}% (threshold: {self.id_reset_threshold}%)")
                analysis['recommendations'].append("Immediate cleanup and AUTO_INCREMENT reset recommended")
            elif usage_pct > 50:
                analysis['health_status'] = 'WARNING'
                analysis['warnings'].append(f"AUTO_INCREMENT usage at {usage_pct:.1f}%")
                analysis['recommendations'].append("Consider scheduling cleanup soon")
            
            # Check data age
            if table_info['oldest_record']:
                oldest_age = datetime.now() - table_info['oldest_record']
                if oldest_age.days > self.data_retention_days * 2:
                    analysis['warnings'].append(f"Oldest record is {oldest_age.days} days old (retention: {self.data_retention_days} days)")
                    analysis['recommendations'].append("Run cleanup to remove old data")
            
            # Check row count
            if table_info['row_count'] > 1000000:  # 1 million records
                analysis['warnings'].append(f"Large table: {table_info['row_count']:,} records")
                analysis['recommendations'].append("Consider more frequent cleanup or shorter retention period")
            
            self.logger.info(f"Table {table_name} health: {analysis['health_status']} "
                           f"(Usage: {usage_pct:.1f}%, Rows: {table_info['row_count']:,})")
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Failed to analyze table {table_name}: {str(e)}")
            return None
    
    def run_maintenance(self, force=False, dry_run=False):
        """
        Run complete maintenance routine
        
        Args:
            force: Run maintenance even if not scheduled
            dry_run: Only show what would be done without making changes
        """
        try:
            self.logger.info(f"{'DRY RUN: ' if dry_run else ''}Starting database maintenance routine")
            
            # Check if maintenance is enabled
            if not self.enable_auto_cleanup and not force:
                self.logger.info("Auto cleanup disabled, skipping maintenance")
                return {
                    'maintenance_time': datetime.now().isoformat(),
                    'tables_processed': [],
                    'total_records_deleted': 0,
                    'auto_increment_resets': [],
                    'dry_run': dry_run,
                    'skipped': True,
                    'skip_reason': 'Auto cleanup disabled'
                }
            
            # Check if it's the right time (unless forced)
            current_hour = datetime.now().hour
            if not force and current_hour != self.cleanup_schedule_hour:
                self.logger.debug(f"Not maintenance time (current: {current_hour}, scheduled: {self.cleanup_schedule_hour})")
                return {
                    'maintenance_time': datetime.now().isoformat(),
                    'tables_processed': [],
                    'total_records_deleted': 0,
                    'auto_increment_resets': [],
                    'dry_run': dry_run,
                    'skipped': True,
                    'skip_reason': f'Not maintenance time (current: {current_hour}, scheduled: {self.cleanup_schedule_hour})'
                }
            
            results = {
                'maintenance_time': datetime.now().isoformat(),
                'tables_processed': [],
                'total_records_deleted': 0,
                'auto_increment_resets': [],
                'dry_run': dry_run
            }
            
            # Process each table
            tables_to_maintain = [self.table_name]
            
            # Also check fallback table if it exists
            fallback_info = self.get_table_info(self.fallback_table)
            if fallback_info and fallback_info['table_exists']:
                tables_to_maintain.append(self.fallback_table)
            
            for table in tables_to_maintain:
                self.logger.info(f"Processing table: {table}")
                
                # Analyze table health
                health = self.analyze_table_health(table)
                if not health:
                    continue
                
                table_result = {
                    'table_name': table,
                    'health_before': health,
                    'records_deleted': 0,
                    'auto_increment_reset': False
                }
                
                # Run cleanup if needed
                if health['health_status'] in ['WARNING', 'CRITICAL'] or force:
                    deleted = self.cleanup_old_data(table, dry_run)
                    table_result['records_deleted'] = deleted
                    results['total_records_deleted'] += deleted
                    
                    # Reset AUTO_INCREMENT if enabled and needed
                    if self.enable_id_reset and (health['stats']['usage_percentage'] > self.id_reset_threshold or force):
                        reset_success = self.reset_auto_increment(table, dry_run)
                        table_result['auto_increment_reset'] = reset_success
                        if reset_success:
                            results['auto_increment_resets'].append(table)
                
                # Re-analyze after cleanup
                table_result['health_after'] = self.analyze_table_health(table)
                results['tables_processed'].append(table_result)
            
            # Log summary
            self.logger.info(f"Maintenance completed: {results['total_records_deleted']:,} records deleted, "
                           f"{len(results['auto_increment_resets'])} tables reset")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Maintenance routine failed: {str(e)}")
            raise
    
    def get_maintenance_status(self):
        """Get current maintenance status and table health"""
        try:
            status = {
                'timestamp': datetime.now().isoformat(),
                'maintenance_enabled': self.enable_auto_cleanup,
                'next_scheduled': f"{self.cleanup_schedule_hour:02d}:00 daily",
                'retention_days': self.data_retention_days,
                'tables': {}
            }
            
            # Check each table
            for table in [self.table_name, self.fallback_table]:
                health = self.analyze_table_health(table)
                if health:
                    status['tables'][table] = health
            
            return status
            
        except Exception as e:
            self.logger.error(f"Failed to get maintenance status: {str(e)}")
            return {'error': str(e)}

def main():
    """Command line interface for database maintenance"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Database Maintenance Tool')
    parser.add_argument('--analyze', '--analyse', action='store_true', help='Analyze table health only')
    parser.add_argument('--cleanup', action='store_true', help='Run cleanup only') 
    parser.add_argument('--reset-id', action='store_true', help='Reset AUTO_INCREMENT only')
    parser.add_argument('--full', action='store_true', help='Run full maintenance')
    parser.add_argument('--status', action='store_true', help='Show maintenance status')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes (implies --full if no other action)')
    parser.add_argument('--force', action='store_true', help='Force maintenance regardless of schedule')
    parser.add_argument('--table', help='Specific table to work on')
    
    args = parser.parse_args()
    
    # Handle dry-run with no other action - default to showing what full maintenance would do
    if args.dry_run and not any([args.analyze, args.cleanup, args.reset_id, args.full, args.status]):
        args.full = True
    elif not any([args.analyze, args.cleanup, args.reset_id, args.full, args.status]):
        # If no specific action, default to showing status
        args.status = True
    
    try:
        maintenance = DatabaseMaintenance()
        
        if args.table:
            maintenance.table_name = args.table
        
        if args.analyze:
            health = maintenance.analyze_table_health()
            if health:
                print(f"\nTable Health Analysis:")
                print(f"Status: {health['health_status']}")
                print(f"Records: {health['stats']['row_count']:,}")
                print(f"AUTO_INCREMENT usage: {health['stats']['usage_percentage']:.1f}%")
                if health['warnings']:
                    print(f"Warnings: {', '.join(health['warnings'])}")
                if health['recommendations']:
                    print(f"Recommendations: {', '.join(health['recommendations'])}")
        
        elif args.cleanup:
            deleted = maintenance.cleanup_old_data(dry_run=args.dry_run)
            print(f"{'Would delete' if args.dry_run else 'Deleted'} {deleted:,} records")
        
        elif args.reset_id:
            success = maintenance.reset_auto_increment(dry_run=args.dry_run)
            print(f"AUTO_INCREMENT reset: {'Success' if success else 'Failed'}")
        
        elif args.full:
            results = maintenance.run_maintenance(force=args.force, dry_run=args.dry_run)
            if results.get('skipped'):
                print(f"Maintenance skipped: {results['skip_reason']}")
            else:
                print(f"Maintenance completed: {results['total_records_deleted']:,} records {'would be ' if args.dry_run else ''}deleted")
        
        elif args.status:
            status = maintenance.get_maintenance_status()
            print(f"\nDatabase Maintenance Status:")
            print(f"Maintenance enabled: {status['maintenance_enabled']}")
            print(f"Next scheduled: {status['next_scheduled']}")
            print(f"Retention period: {status['retention_days']} days")
            print(f"Timestamp: {status['timestamp']}")
            
            for table_name, table_info in status['tables'].items():
                print(f"\nTable: {table_name}")
                print(f"  Status: {table_info['health_status']}")
                print(f"  Records: {table_info['stats']['row_count']:,}")
                print(f"  AUTO_INCREMENT usage: {table_info['stats']['usage_percentage']:.1f}%")
                print(f"  Current AUTO_INCREMENT: {table_info['stats']['current_auto_increment']:,}")
                print(f"  Max ID used: {table_info['stats']['max_id_used']:,}")
                if table_info['stats'].get('oldest_record'):
                    age = datetime.now() - table_info['stats']['oldest_record']
                    print(f"  Oldest record: {age.days} days ago")
                if table_info['warnings']:
                    print(f"  Warnings: {'; '.join(table_info['warnings'])}")
                if table_info['recommendations']:
                    print(f"  Recommendations: {'; '.join(table_info['recommendations'])}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
