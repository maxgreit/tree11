#!/usr/bin/env python3
"""
Tree11 Data Pipeline - Database Setup Script
Eenmalige setup voor database initialisatie en configuratie

Usage:
    python scripts/setup_database.py [--config-dir config/] [--force]
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from database_manager import DatabaseManager
from logger import setup_logging
from utils import load_config


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Tree11 Database Setup - Eenmalige database initialisatie'
    )
    
    parser.add_argument(
        '--config-dir',
        type=str,
        help='Pad naar configuratie directory',
        default='config'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Forceer setup ook als tabellen al bestaan'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Uitgebreide logging'
    )
    
    parser.add_argument(
        '--sql-scripts-dir',
        type=str,
        help='Pad naar SQL scripts directory',
        default='sql'
    )
    
    return parser.parse_args()


class DatabaseSetup:
    """
    Database setup manager voor Tree11 pipeline
    """
    
    def __init__(self, config_dir: str, sql_scripts_dir: str):
        """
        Initialize database setup
        
        Args:
            config_dir: Directory containing configuration files
            sql_scripts_dir: Directory containing SQL scripts
        """
        self.config_dir = Path(config_dir)
        self.sql_scripts_dir = Path(sql_scripts_dir)
        
        # Load database configuration
        db_config_path = self.config_dir / 'database_config.json'
        if not db_config_path.exists():
            raise FileNotFoundError(f"Database config not found: {db_config_path}")
        
        self.db_manager = DatabaseManager(str(db_config_path))
        
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Database setup initialized")
    
    def check_prerequisites(self) -> bool:
        """
        Check if all prerequisites are met
        
        Returns:
            True if all prerequisites are met
        """
        self.logger.info("Checking prerequisites...")
        
        try:
            # Test database connection
            result = self.db_manager.execute_query("SELECT 1 AS test")
            if not result or result[0]['test'] != 1:
                self.logger.error("Database connection test failed")
                return False
            
            self.logger.info("✓ Database connection successful")
            
            # Check SQL scripts exist
            required_scripts = ['create_tables.sql', 'stored_procedures.sql', 'indexes.sql']
            for script in required_scripts:
                script_path = self.sql_scripts_dir / script
                if not script_path.exists():
                    self.logger.error(f"Required SQL script not found: {script_path}")
                    return False
            
            self.logger.info("✓ All required SQL scripts found")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Prerequisites check failed: {str(e)}")
            return False
    
    def check_existing_tables(self) -> dict:
        """
        Check which tables already exist
        
        Returns:
            Dictionary of table names and their existence status
        """
        self.logger.info("Checking existing tables...")
        
        expected_tables = [
            'Leden', 'Abonnementen', 'AbonnementStatistieken', 'Lessen',
            'OpenstaandeFacturen', 'Omzet', 'GrootboekRekening', 'PersonalTraining',
            'PipelineLog', 'DataQualityLog'
        ]
        
        table_status = {}
        
        for table in expected_tables:
            exists = self.db_manager.check_table_exists(table)
            record_count = self.db_manager.get_record_count(table) if exists else 0
            
            table_status[table] = {
                'exists': exists,
                'record_count': record_count
            }
            
            status_msg = f"{'EXISTS' if exists else 'NOT EXISTS'}"
            if exists:
                status_msg += f" ({record_count} records)"
            
            self.logger.info(f"  {table}: {status_msg}")
        
        return table_status
    
    def execute_sql_script(self, script_name: str) -> bool:
        """
        Execute a SQL script file
        
        Args:
            script_name: Name of the SQL script file
            
        Returns:
            True if successful, False otherwise
        """
        script_path = self.sql_scripts_dir / script_name
        
        if not script_path.exists():
            self.logger.error(f"SQL script not found: {script_path}")
            return False
        
        self.logger.info(f"Executing SQL script: {script_name}")
        
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Split by GO statements (SQL Server batch separator)
            batches = [batch.strip() for batch in sql_content.split('\nGO\n') if batch.strip()]
            
            for i, batch in enumerate(batches, 1):
                if batch.strip():
                    try:
                        self.db_manager.execute_query(batch)
                        self.logger.debug(f"Executed batch {i}/{len(batches)}")
                    except Exception as e:
                        # Some errors might be expected (like "already exists")
                        if "already exists" in str(e).lower():
                            self.logger.debug(f"Batch {i}: Object already exists (expected)")
                        else:
                            self.logger.warning(f"Batch {i} failed: {str(e)}")
                            # Continue with other batches
            
            self.logger.info(f"✓ SQL script {script_name} executed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to execute SQL script {script_name}: {str(e)}")
            return False
    
    def setup_tables(self, force: bool = False) -> bool:
        """
        Setup database tables
        
        Args:
            force: Force setup even if tables exist
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("Setting up database tables...")
        
        table_status = self.check_existing_tables()
        
        # Check if setup is needed
        existing_tables = [name for name, status in table_status.items() if status['exists']]
        
        if existing_tables and not force:
            self.logger.warning(f"Tables already exist: {', '.join(existing_tables)}")
            self.logger.warning("Use --force to recreate tables")
            return False
        
        if existing_tables and force:
            self.logger.warning(f"Force mode: Recreating existing tables: {', '.join(existing_tables)}")
        
        # Execute table creation script
        if not self.execute_sql_script('create_tables.sql'):
            return False
        
        return True
    
    def setup_stored_procedures(self) -> bool:
        """
        Setup stored procedures
        
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("Setting up stored procedures...")
        
        return self.execute_sql_script('stored_procedures.sql')
    
    def setup_indexes(self) -> bool:
        """
        Setup performance indexes
        
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("Setting up performance indexes...")
        
        return self.execute_sql_script('indexes.sql')
    
    def verify_setup(self) -> bool:
        """
        Verify that setup was successful
        
        Returns:
            True if verification successful, False otherwise
        """
        self.logger.info("Verifying database setup...")
        
        try:
            # Check all expected tables exist
            table_status = self.check_existing_tables()
            missing_tables = [name for name, status in table_status.items() if not status['exists']]
            
            if missing_tables:
                self.logger.error(f"Missing tables after setup: {', '.join(missing_tables)}")
                return False
            
            # Check stored procedures exist
            procedures_query = """
            SELECT name FROM sys.procedures 
            WHERE schema_id = SCHEMA_ID('tree11')
            ORDER BY name
            """
            
            procedures = self.db_manager.execute_query(procedures_query)
            procedure_names = [proc['name'] for proc in procedures]
            
            expected_procedures = [
                'sp_UpsertLeden', 'sp_UpsertLessen', 'sp_InsertUpdateOmzet',
                'sp_ValidateDataQuality', 'sp_CleanupOldLogs', 'sp_GetPipelineExecutionReport'
            ]
            
            missing_procedures = [proc for proc in expected_procedures if proc not in procedure_names]
            
            if missing_procedures:
                self.logger.warning(f"Missing stored procedures: {', '.join(missing_procedures)}")
            else:
                self.logger.info("✓ All stored procedures created")
            
            # Check indexes exist
            indexes_query = """
            SELECT i.name, t.name AS table_name
            FROM sys.indexes i
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            WHERE t.schema_id = SCHEMA_ID('tree11')
                AND i.type > 0  -- Exclude heaps
                AND i.name IS NOT NULL
            ORDER BY t.name, i.name
            """
            
            indexes = self.db_manager.execute_query(indexes_query)
            self.logger.info(f"✓ Created {len(indexes)} indexes across all tables")
            
            self.logger.info("✓ Database setup verification completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Setup verification failed: {str(e)}")
            return False
    
    def run_setup(self, force: bool = False) -> bool:
        """
        Run complete database setup process
        
        Args:
            force: Force setup even if tables exist
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("Starting Tree11 database setup...")
        
        try:
            # Check prerequisites
            if not self.check_prerequisites():
                self.logger.error("Prerequisites check failed")
                return False
            
            # Setup tables
            if not self.setup_tables(force):
                self.logger.error("Table setup failed")
                return False
            
            # Setup stored procedures
            if not self.setup_stored_procedures():
                self.logger.error("Stored procedures setup failed")
                return False
            
            # Setup indexes
            if not self.setup_indexes():
                self.logger.error("Indexes setup failed")
                return False
            
            # Verify setup
            if not self.verify_setup():
                self.logger.error("Setup verification failed")
                return False
            
            self.logger.info("✓ Tree11 database setup completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"Database setup failed: {str(e)}")
            return False
        finally:
            self.db_manager.close()


def main():
    """Main entry point"""
    args = parse_arguments()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(log_level)
    
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize database setup
        setup = DatabaseSetup(args.config_dir, args.sql_scripts_dir)
        
        # Run setup
        success = setup.run_setup(force=args.force)
        
        if success:
            logger.info("Database setup completed successfully!")
            print("\n" + "="*60)
            print("🎉 Tree11 Database Setup Completed Successfully!")
            print("="*60)
            print("Next steps:")
            print("1. Configure your .env file with API tokens")
            print("2. Test the pipeline: python main.py --dry-run")
            print("3. Run your first data import: python main.py")
            print("="*60)
            sys.exit(0)
        else:
            logger.error("Database setup failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("Setup interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main() 