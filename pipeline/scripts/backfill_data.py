#!/usr/bin/env python3
"""
Tree11 Data Pipeline - Backfill Data Script
Historische data import met batch processing en progress tracking

Usage:
    python scripts/backfill_data.py [--tables Leden,Lessen] [--start-date 2023-01-01] [--end-date 2023-12-31]
"""

import os
import sys
import argparse
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
import time

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_extractor import DataExtractor
from data_transformer import DataTransformer
from database_manager import DatabaseManager
from logger import setup_logging
from utils import load_config, parse_date_string, format_duration


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Tree11 Backfill Data - Historische data import'
    )
    
    parser.add_argument(
        '--tables',
        type=str,
        help='Kommagescheiden lijst van tabellen om te verwerken',
        default=None
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start datum voor historische data (YYYY-MM-DD)',
        default=None
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='Eind datum voor historische data (YYYY-MM-DD)',
        default=None
    )
    
    parser.add_argument(
        '--config-dir',
        type=str,
        help='Pad naar configuratie directory',
        default='config'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        help='Batch grootte voor verwerking',
        default=30
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Droog draaien - geen database wijzigingen'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Uitgebreide logging'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Hervat onderbroken backfill'
    )
    
    return parser.parse_args()


class BackfillManager:
    """
    Manager voor historische data import
    """
    
    def __init__(self, config_dir: str, dry_run: bool = False):
        """
        Initialize backfill manager
        
        Args:
            config_dir: Directory containing configuration files
            dry_run: If True, no database changes will be made
        """
        self.config_dir = Path(config_dir)
        self.dry_run = dry_run
        
        # Load configurations
        self.api_config_path = self.config_dir / 'api_endpoints.json'
        self.schema_config_path = self.config_dir / 'schema_mappings.json'
        self.db_config_path = self.config_dir / 'database_config.json'
        
        # Initialize components
        self.extractor = DataExtractor(str(self.api_config_path))
        self.transformer = DataTransformer(str(self.schema_config_path))
        
        if not dry_run:
            self.db_manager = DatabaseManager(str(self.db_config_path))
        else:
            self.db_manager = None
        
        self.logger = logging.getLogger(__name__)
        
        # Progress tracking
        self.progress_file = Path('backfill_progress.json')
        self.progress = {}
        
        self.logger.info("Backfill manager initialized", dry_run=dry_run)
    
    def load_progress(self) -> dict:
        """Load backfill progress from file"""
        if self.progress_file.exists():
            try:
                import json
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning("Failed to load progress file", error=str(e))
        
        return {}
    
    def save_progress(self, progress: dict):
        """Save backfill progress to file"""
        try:
            import json
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2, default=str)
        except Exception as e:
            self.logger.warning("Failed to save progress file", error=str(e))
    
    def get_date_batches(self, start_date: date, end_date: date, 
                        batch_size: int) -> List[Tuple[date, date]]:
        """
        Split date range into batches
        
        Args:
            start_date: Start date
            end_date: End date
            batch_size: Number of days per batch
            
        Returns:
            List of (start_date, end_date) tuples for each batch
        """
        batches = []
        current_start = start_date
        
        while current_start <= end_date:
            current_end = min(current_start + timedelta(days=batch_size - 1), end_date)
            batches.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)
        
        return batches
    
    def get_backfillable_tables(self) -> List[str]:
        """
        Get list of tables that support historical data backfill
        
        Returns:
            List of table names that can be backfilled
        """
        # Only tables with date-based APIs support backfill
        backfillable = [
            'Lessen',  # activity_events with date range
            'LesDeelname',  # Requires course IDs from past lessons
            'Omzet',   # analytics_revenue with date range
            'GrootboekRekening',  # Part of revenue data
            'AbonnementStatistieken'  # analytics with date range
        ]
        
        return backfillable
    
    def validate_table_for_backfill(self, table_name: str) -> bool:
        """
        Validate if table supports backfill
        
        Args:
            table_name: Name of the table
            
        Returns:
            True if table supports backfill
        """
        backfillable = self.get_backfillable_tables()
        return table_name in backfillable
    
    def backfill_table_batch(self, table_name: str, start_date: date, 
                           end_date: date) -> dict:
        """
        Backfill data for a single table and date range
        
        Args:
            table_name: Name of the table
            start_date: Start date for batch
            end_date: End date for batch
            
        Returns:
            Dictionary with batch results
        """
        self.logger.info("Starting batch backfill",
                        table=table_name,
                        start_date=start_date,
                        end_date=end_date)
        
        batch_start_time = time.time()
        
        result = {
            'table': table_name,
            'start_date': start_date,
            'end_date': end_date,
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            'status': 'success',
            'error': None,
            'duration': 0
        }
        
        try:
            # Extract data for date range
            raw_data = self._extract_table_data_for_range(table_name, start_date, end_date)
            result['extracted'] = len(raw_data)
            
            if not raw_data:
                self.logger.info("No data found for date range",
                               table=table_name,
                               start_date=start_date,
                               end_date=end_date)
                result['duration'] = time.time() - batch_start_time
                return result
            
            # Transform data
            if table_name in ['Omzet', 'GrootboekRekening']:
                # Special handling for revenue data
                if table_name == 'Omzet':
                    omzet_records, _ = self.transformer.transform_revenue_data(raw_data)
                    transformed_data = omzet_records
                else:  # GrootboekRekening
                    _, grootboek_records = self.transformer.transform_revenue_data(raw_data)
                    transformed_data = grootboek_records
            else:
                # Standard transformation
                transformed_data = self.transformer.transform_table_data(table_name, raw_data)
            
            # Validate data
            valid_data = self.transformer.validate_transformed_data(table_name, transformed_data)
            result['transformed'] = len(valid_data)
            
            if not valid_data:
                self.logger.warning("No valid data after transformation",
                                  table=table_name)
                result['duration'] = time.time() - batch_start_time
                return result
            
            # Load data to database
            if not self.dry_run:
                # Get update strategy
                schema_config = load_config(self.schema_config_path)
                table_config = schema_config['tables'].get(table_name, {})
                update_strategy = table_config.get('update_strategy', 'upsert')
                
                loaded_count = self.db_manager.load_table_data(table_name, valid_data, update_strategy)
                result['loaded'] = loaded_count
            else:
                result['loaded'] = len(valid_data)
                self.logger.info("DRY RUN: Would load records",
                               table=table_name,
                               records=len(valid_data))
            
            result['duration'] = time.time() - batch_start_time
            
            self.logger.info("Batch backfill completed",
                           table=table_name,
                           start_date=start_date,
                           end_date=end_date,
                           extracted=result['extracted'],
                           transformed=result['transformed'],
                           loaded=result['loaded'],
                           duration=result['duration'])
            
            return result
            
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            result['duration'] = time.time() - batch_start_time
            
            self.logger.error("Batch backfill failed",
                            table=table_name,
                            start_date=start_date,
                            end_date=end_date,
                            error=str(e))
            
            return result
    
    def _extract_table_data_for_range(self, table_name: str, start_date: date, 
                                    end_date: date) -> List[dict]:
        """Extract data for specific table and date range"""
        endpoint_mappings = {
            'Lessen': 'activity_events',
            'LesDeelname': 'courses_members',  # Special handling required
            'Omzet': 'analytics_revenue',
            'GrootboekRekening': 'analytics_revenue',
            'AbonnementStatistieken': [
                'analytics_memberships_new',
                'analytics_memberships_paused',
                'analytics_memberships_active',
                'analytics_memberships_expired'
            ]
        }
        
        endpoint_name = endpoint_mappings.get(table_name)
        if not endpoint_name:
            raise ValueError(f"No endpoint mapping for table: {table_name}")
        
        if table_name == 'LesDeelname':
            # Special handling for LesDeelname - use the extractor's method
            return self.extractor._extract_les_deelname_data(start_date, end_date)
        elif isinstance(endpoint_name, list):
            # Multiple endpoints (AbonnementStatistieken)
            all_data = []
            for endpoint in endpoint_name:
                try:
                    data = self.extractor.api_client.extract_endpoint_data(
                        endpoint, start_date, end_date
                    )
                    all_data.extend(data)
                except Exception as e:
                    self.logger.warning("Failed to extract from endpoint",
                                      endpoint=endpoint,
                                      error=str(e))
            return all_data
        else:
            # Single endpoint
            return self.extractor.api_client.extract_endpoint_data(
                endpoint_name, start_date, end_date
            )
    
    def backfill_table(self, table_name: str, start_date: date, end_date: date,
                      batch_size: int, resume: bool = False) -> dict:
        """
        Backfill historical data for a table
        
        Args:
            table_name: Name of the table
            start_date: Start date for backfill
            end_date: End date for backfill
            batch_size: Number of days per batch
            resume: Whether to resume from previous progress
            
        Returns:
            Dictionary with backfill results
        """
        self.logger.info("Starting table backfill",
                        table=table_name,
                        start_date=start_date,
                        end_date=end_date,
                        batch_size=batch_size)
        
        if not self.validate_table_for_backfill(table_name):
            raise ValueError(f"Table {table_name} does not support backfill")
        
        # Load progress if resuming
        if resume:
            self.progress = self.load_progress()
        
        # Get date batches
        batches = self.get_date_batches(start_date, end_date, batch_size)
        
        # Track results
        table_result = {
            'table': table_name,
            'start_date': start_date,
            'end_date': end_date,
            'total_batches': len(batches),
            'completed_batches': 0,
            'total_extracted': 0,
            'total_transformed': 0,
            'total_loaded': 0,
            'batch_results': [],
            'status': 'success',
            'errors': []
        }
        
        # Process each batch
        for i, (batch_start, batch_end) in enumerate(batches, 1):
            batch_key = f"{table_name}_{batch_start}_{batch_end}"
            
            # Check if batch already completed (resume functionality)
            if resume and batch_key in self.progress.get('completed_batches', {}):
                self.logger.info("Skipping completed batch",
                               table=table_name,
                               batch=f"{i}/{len(batches)}",
                               start_date=batch_start,
                               end_date=batch_end)
                table_result['completed_batches'] += 1
                continue
            
            self.logger.info("Processing batch",
                           table=table_name,
                           batch=f"{i}/{len(batches)}",
                           start_date=batch_start,
                           end_date=batch_end)
            
            # Process batch
            batch_result = self.backfill_table_batch(table_name, batch_start, batch_end)
            table_result['batch_results'].append(batch_result)
            
            # Update totals
            table_result['total_extracted'] += batch_result['extracted']
            table_result['total_transformed'] += batch_result['transformed']
            table_result['total_loaded'] += batch_result['loaded']
            
            if batch_result['status'] == 'error':
                table_result['errors'].append(batch_result['error'])
            else:
                table_result['completed_batches'] += 1
                
                # Save progress
                if batch_key not in self.progress.get('completed_batches', {}):
                    if 'completed_batches' not in self.progress:
                        self.progress['completed_batches'] = {}
                    self.progress['completed_batches'][batch_key] = {
                        'completed_at': datetime.now().isoformat(),
                        'extracted': batch_result['extracted'],
                        'loaded': batch_result['loaded']
                    }
                    self.save_progress(self.progress)
            
            # Progress update
            progress_pct = (i / len(batches)) * 100
            self.logger.info("Batch progress",
                           table=table_name,
                           progress=f"{progress_pct:.1f}%",
                           completed=f"{i}/{len(batches)}")
        
        # Finalize results
        if table_result['errors']:
            table_result['status'] = 'partial_success' if table_result['completed_batches'] > 0 else 'error'
        
        self.logger.info("Table backfill completed",
                        table=table_name,
                        status=table_result['status'],
                        completed_batches=table_result['completed_batches'],
                        total_batches=table_result['total_batches'],
                        total_extracted=table_result['total_extracted'],
                        total_loaded=table_result['total_loaded'])
        
        return table_result
    
    def run_backfill(self, tables: Optional[List[str]] = None,
                    start_date: Optional[date] = None,
                    end_date: Optional[date] = None,
                    batch_size: int = 30,
                    resume: bool = False) -> dict:
        """
        Run complete backfill process
        
        Args:
            tables: Optional list of tables to backfill
            start_date: Start date for backfill
            end_date: End date for backfill
            batch_size: Number of days per batch
            resume: Whether to resume from previous progress
            
        Returns:
            Dictionary with backfill results
        """
        backfill_start_time = time.time()
        
        # Determine tables to backfill
        if tables:
            backfill_tables = [t for t in tables if self.validate_table_for_backfill(t)]
            invalid_tables = [t for t in tables if not self.validate_table_for_backfill(t)]
            
            if invalid_tables:
                self.logger.warning("Invalid tables for backfill, skipping",
                                  tables=invalid_tables)
        else:
            backfill_tables = self.get_backfillable_tables()
        
        # Set default date range if not provided
        if not start_date:
            start_date = date.today() - timedelta(days=90)  # Default: last 90 days
        if not end_date:
            end_date = date.today() - timedelta(days=1)  # Until yesterday
        
        self.logger.info("Starting backfill process",
                        tables=backfill_tables,
                        start_date=start_date,
                        end_date=end_date,
                        batch_size=batch_size,
                        resume=resume)
        
        # Track overall results
        backfill_result = {
            'start_time': datetime.now(),
            'end_time': None,
            'duration': 0,
            'tables': backfill_tables,
            'start_date': start_date,
            'end_date': end_date,
            'batch_size': batch_size,
            'table_results': {},
            'total_extracted': 0,
            'total_loaded': 0,
            'status': 'success',
            'errors': []
        }
        
        try:
            # Process each table
            for table_name in backfill_tables:
                self.logger.info("Starting table backfill", table=table_name)
                
                table_result = self.backfill_table(
                    table_name, start_date, end_date, batch_size, resume
                )
                
                backfill_result['table_results'][table_name] = table_result
                backfill_result['total_extracted'] += table_result['total_extracted']
                backfill_result['total_loaded'] += table_result['total_loaded']
                
                if table_result['status'] == 'error':
                    backfill_result['errors'].extend(table_result['errors'])
            
            # Finalize results
            backfill_result['end_time'] = datetime.now()
            backfill_result['duration'] = time.time() - backfill_start_time
            
            if backfill_result['errors']:
                backfill_result['status'] = 'partial_success'
            
            self.logger.info("Backfill process completed",
                           status=backfill_result['status'],
                           total_extracted=backfill_result['total_extracted'],
                           total_loaded=backfill_result['total_loaded'],
                           duration=format_duration(backfill_result['duration']))
            
            return backfill_result
            
        except Exception as e:
            backfill_result['status'] = 'error'
            backfill_result['end_time'] = datetime.now()
            backfill_result['duration'] = time.time() - backfill_start_time
            backfill_result['errors'].append(str(e))
            
            self.logger.error("Backfill process failed",
                            error=str(e),
                            duration=format_duration(backfill_result['duration']))
            
            return backfill_result
        
        finally:
            if self.db_manager:
                self.db_manager.close()


def main():
    """Main entry point"""
    args = parse_arguments()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(log_level)
    
    logger = logging.getLogger(__name__)
    
    try:
        # Parse dates
        start_date = None
        end_date = None
        
        if args.start_date:
            start_date = parse_date_string(args.start_date)
            if not start_date:
                raise ValueError(f"Invalid start date: {args.start_date}")
        
        if args.end_date:
            end_date = parse_date_string(args.end_date)
            if not end_date:
                raise ValueError(f"Invalid end date: {args.end_date}")
        
        # Parse tables
        tables = None
        if args.tables:
            tables = [table.strip() for table in args.tables.split(',')]
        
        # Initialize backfill manager
        backfill_manager = BackfillManager(args.config_dir, args.dry_run)
        
        if args.dry_run:
            logger.info("Running in dry-run mode - no database changes will be made")
        
        # Run backfill
        result = backfill_manager.run_backfill(
            tables=tables,
            start_date=start_date,
            end_date=end_date,
            batch_size=args.batch_size,
            resume=args.resume
        )
        
        # Print summary
        print("\n" + "="*60)
        print("📊 Backfill Summary")
        print("="*60)
        print(f"Status: {result['status'].upper()}")
        print(f"Duration: {format_duration(result['duration'])}")
        print(f"Date Range: {result['start_date']} to {result['end_date']}")
        print(f"Total Extracted: {result['total_extracted']:,} records")
        print(f"Total Loaded: {result['total_loaded']:,} records")
        
        if result['table_results']:
            print("\nTable Results:")
            for table_name, table_result in result['table_results'].items():
                print(f"  {table_name}: {table_result['status']} "
                      f"({table_result['total_loaded']:,} records)")
        
        if result['errors']:
            print(f"\nErrors ({len(result['errors'])}):")
            for error in result['errors']:
                print(f"  - {error}")
        
        print("="*60)
        
        if result['status'] == 'success':
            logger.info("Backfill completed successfully")
            sys.exit(0)
        else:
            logger.error("Backfill completed with errors")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("Backfill interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main() 