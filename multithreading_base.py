import os
import logging
import threading
from queue import Queue
import mysql.connector
from mysql.connector import pooling, errorcode
from cryptography.fernet import Fernet
from dotenv import load_dotenv

class MultithreadingBase:
    def __init__(self, config=None):
        """Initialize with proper config setup"""
        load_dotenv()  # Load environment variables
        # Initialize logger first
        self.logger = logging.getLogger(self.__class__.__name__)
        self._init_logging()
        
        # Set default configuration (UPPERCASE keys)
        self.config = {
            'ACCESS': {
                'HOST': os.getenv('ACCESS_HOST'),
                'DATABASE': os.getenv('ACCESS_DATABASE'),
                'USER': os.getenv('ACCESS_USER'),
                'PASSWORD': os.getenv('ACCESS_PASSWORD'),
                'POOL_SIZE': int(os.getenv('ACCESS_POOL_SIZE', '10'))
            },
            'REPORTING': {
                'HOST': os.getenv('REPORTING_HOST'),
                'DATABASE': os.getenv('REPORTING_DATABASE'),
                'USER': os.getenv('REPORTING_USER'),
                'PASSWORD': os.getenv('REPORTING_PASSWORD'),
                'POOL_SIZE': int(os.getenv('REPORTING_POOL_SIZE', '5'))
            },
            'FERNET_KEY': os.getenv('FERNET_KEY')
        }
        
        # Update with any provided config
        if config:
            self._merge_config(config)
            
        # Initialize components
        self._init_mysql_pools()
        self._init_threading()

    def _merge_config(self, override_config):
        """Merge override_config into self.config recursively."""
        def merge(dct, merge_dct):
            for k, v in merge_dct.items():
                if (k in dct and isinstance(dct[k], dict) and isinstance(v, dict)):
                    merge(dct[k], v)
                else:
                    dct[k] = v
        merge(self.config, override_config)

    def _init_logging(self):
        """Configure logging system"""
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Clear existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
            
        # Add console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def _init_mysql_pools(self):
        """Initialize MySQL connection pools for access and reporting"""
        # ACCESS pool (destination)
        access_cfg = self.config.get('ACCESS')
        if access_cfg:
            try:
                self.access_pool = pooling.MySQLConnectionPool(
                    pool_name=f"{self.__class__.__name__}_access_pool",
                    pool_size=access_cfg.get('POOL_SIZE', 10),
                    host=access_cfg.get('HOST'),
                    database=access_cfg.get('DATABASE'),
                    user=access_cfg.get('USER'),
                    password=self._decrypt_password(access_cfg.get('PASSWORD', '')).strip(),
                    autocommit=True,
                )
                self.logger.info("ACCESS MySQL connection pool initialized")
            except Exception as e:
                self.logger.error(f"ACCESS pool initialization failed: {str(e)}")
                raise
        else:
            self.logger.warning("ACCESS configuration missing")

        # REPORTING pool (source)
        reporting_cfg = self.config.get('REPORTING')
        if reporting_cfg:
            try:
                self.reporting_pool = pooling.MySQLConnectionPool(
                    pool_name=f"{self.__class__.__name__}_reporting_pool",
                    pool_size=reporting_cfg.get('POOL_SIZE', 5),
                    host=reporting_cfg.get('HOST'),
                    database=reporting_cfg.get('DATABASE'),
                    user=reporting_cfg.get('USER'),
                    password=self._decrypt_password(reporting_cfg.get('PASSWORD', '')).strip(),
                    autocommit=True,
                )
                self.logger.info("REPORTING MySQL connection pool initialized")
            except Exception as e:
                self.logger.error(f"REPORTING pool initialization failed: {str(e)}")
                raise
        else:
            self.logger.warning("REPORTING configuration missing")

    def _init_threading(self):
        """Initialize threading components"""
        self.task_queue = Queue()
        self.thread_lock = threading.Lock()
        self.threads = []

    def _decrypt_password(self, encrypted_password):
        """Decrypt passwords using Fernet key from environment or fallback to hardcoded key"""
        if not encrypted_password or not encrypted_password.startswith('gAAAAAB'):
            return encrypted_password or ""

        try:
            fernet_key = self.config.get('FERNET_KEY') or os.getenv('FERNET_KEY')
            if not fernet_key:
                self.logger.warning("No FERNET_KEY found in environment, password decryption may fail")
                return encrypted_password
            
            fernet = Fernet(fernet_key.encode('utf-8'))
            return fernet.decrypt(encrypted_password.encode()).decode()
        except Exception as e:
            self.logger.error(f"Password decryption failed: {str(e)}")
            raise

    def execute_access_db_query(self, query, params=None, fetch_all=True):
        """Thread-safe query execution for ACCESS (destination) database"""
        conn = None
        cursor = None
        try:
            conn = self.access_pool.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, params or ())
            return cursor.fetchall() if fetch_all else cursor.fetchone()
        except Exception as e:
            self.logger.error(f"ACCESS DB query failed: {str(e)}")
            raise
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def execute_reporting_db_query(self, query, params=None, fetch_all=True):
        """Thread-safe query execution for REPORTING (source) database"""
        conn = None
        cursor = None
        try:
            conn = self.reporting_pool.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, params or ())
            return cursor.fetchall() if fetch_all else cursor.fetchone()
        except Exception as e:
            self.logger.error(f"REPORTING DB query failed: {str(e)}")
            raise
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def run_threaded_tasks(self, task_func, items, max_workers=5):
        """Execute tasks in parallel with thread pool"""
        threads = []
        for _ in range(max_workers):
            t = threading.Thread(target=self._worker, args=(task_func,))
            t.start()
            threads.append(t)
        
        for item in items:
            self.task_queue.put(item)
        
        self.task_queue.join()
        
        for _ in range(max_workers):
            self.task_queue.put(None)
        
        for t in threads:
            t.join()

    def _worker(self, task_func):
        """Worker thread processing"""
        while True:
            item = self.task_queue.get()
            if item is None:
                break
            try:
                task_func(item)
            except Exception as e:
                self.logger.error(f"Task failed for {item}: {str(e)}")
            finally:
                self.task_queue.task_done()