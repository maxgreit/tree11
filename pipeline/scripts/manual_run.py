#!/usr/bin/env python3
"""
Tree11 Data Pipeline - Manual Run Script
Ad-hoc pipeline runs en handmatige data operaties

Usage:
    python scripts/manual_run.py [--action extract|transform|load|full] [--tables Leden,Lessen]
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_extractor import DataExtractor
from data_transformer import DataTransformer
from database_manager import DatabaseManager
from pipeline_runner import PipelineRunner
from logger import setup_logging
from utils import load_config, parse_date_string, format_duration


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Tree11 Manual Run - Ad-hoc pipeline operaties'
    )
    
    parser.add_argument(
        '--action',
        type=str,
        choices=['extract', 'transform', 'load', 'full', 'test', 'validate'],
        help='Actie om uit te voeren',
        default='full'
    )
    
    parser.add_argument(
        '--tables',
        type=str,
        help='Kommagescheiden lijst van tabellen om te verwerken',
        default=None
    )
    
    parser.add_argument(
        '--config-dir',
        type=str,
        help='Pad naar configuratie directory',
        default='config'
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start datum voor data (YYYY-MM-DD)',
        default=None
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='Eind datum voor data (YYYY-MM-DD)',
        default=None
    )
    
    parser.add_argument(
        '--output-file',
        type=str,
        help='Output bestand voor geëxtraheerde data (JSON)',
        default=None
    )
    
    parser.add_argument(
        '--input-file',
        type=str,
        help='Input bestand met data om te transformeren/laden',
        default=None
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
        '--force',
        action='store_true',
        help='Forceer operatie ook bij waarschuwingen'
    )
    
    return parser.parse_args()


class ManualRunner:
    """
    Manual runner voor ad-hoc pipeline operaties
    """
    
    def __init__(self, config_dir: str, dry_run: bool = False):
        """
        Initialize manual runner
        
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
        
        self.pipeline_runner = PipelineRunner(str(self.config_dir))
        
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Manual runner initialized", dry_run=dry_run)
    
    def action_extract(self, tables: Optional[List[str]] = None,
                      start_date: Optional[date] = None,
                      end_date: Optional[date] = None,
                      output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract data from APIs and optionally save to file
        
        Args:
            tables: Optional list of tables to extract
            start_date: Start date for extraction
            end_date: End date for extraction
            output_file: Optional output file to save data
            
        Returns:
            Dictionary with extraction results
        """
        self.logger.info("Starting data extraction",
                        tables=tables,
                        start_date=start_date,
                        end_date=end_date)
        
        try:
            # Extract data using DataExtractor
            if tables:
                extracted_data = {}
                for table in tables:
                    self.logger.info("Extracting table data", table=table)
                    
                    # Use pipeline runner's extract method for consistency
                    table_data = self.pipeline_runner.extract_table_data(table)
                    extracted_data[table] = table_data
                    
                    self.logger.info("Table extraction completed",
                                   table=table,
                                   records=len(table_data))
            else:
                # Extract all data
                extracted_data = self.extractor.extract_all_data()
            
            # Calculate totals
            total_records = sum(len(records) for records in extracted_data.values())
            
            # Save to file if requested
            if output_file:
                output_path = Path(output_file)
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(extracted_data, f, indent=2, default=str, ensure_ascii=False)
                
                self.logger.info("Data saved to file",
                               file=output_file,
                               total_records=total_records)
            
            result = {
                'action': 'extract',
                'status': 'success',
                'tables': list(extracted_data.keys()),
                'total_records': total_records,
                'data': extracted_data if not output_file else None,
                'output_file': output_file
            }
            
            self.logger.info("Data extraction completed",
                           tables=len(extracted_data),
                           total_records=total_records)
            
            return result
            
        except Exception as e:
            self.logger.error("Data extraction failed", error=str(e))
            return {
                'action': 'extract',
                'status': 'error',
                'error': str(e)
            }
    
    def action_transform(self, tables: Optional[List[str]] = None,
                        input_file: Optional[str] = None,
                        output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Transform raw data to database schema
        
        Args:
            tables: Optional list of tables to transform
            input_file: Input file with raw data
            output_file: Optional output file for transformed data
            
        Returns:
            Dictionary with transformation results
        """
        self.logger.info("Starting data transformation",
                        tables=tables,
                        input_file=input_file)
        
        try:
            # Load input data
            if input_file:
                input_path = Path(input_file)
                if not input_path.exists():
                    raise FileNotFoundError(f"Input file not found: {input_file}")
                
                with open(input_path, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
            else:
                # Extract fresh data
                self.logger.info("No input file provided, extracting fresh data")
                extract_result = self.action_extract(tables)
                if extract_result['status'] != 'success':
                    return extract_result
                raw_data = extract_result['data']
            
            # Transform data
            transformed_data = {}
            
            for table_name, table_data in raw_data.items():
                if tables and table_name not in tables:
                    continue
                
                self.logger.info("Transforming table data",
                               table=table_name,
                               input_records=len(table_data))
                
                # Use pipeline runner's transform method
                transformed_records = self.pipeline_runner.transform_table_data(table_name, table_data)
                transformed_data[table_name] = transformed_records
                
                self.logger.info("Table transformation completed",
                               table=table_name,
                               output_records=len(transformed_records))
            
            # Calculate totals
            total_records = sum(len(records) for records in transformed_data.values())
            
            # Save to file if requested
            if output_file:
                output_path = Path(output_file)
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(transformed_data, f, indent=2, default=str, ensure_ascii=False)
                
                self.logger.info("Transformed data saved to file",
                               file=output_file,
                               total_records=total_records)
            
            result = {
                'action': 'transform',
                'status': 'success',
                'tables': list(transformed_data.keys()),
                'total_records': total_records,
                'data': transformed_data if not output_file else None,
                'output_file': output_file
            }
            
            self.logger.info("Data transformation completed",
                           tables=len(transformed_data),
                           total_records=total_records)
            
            return result
            
        except Exception as e:
            self.logger.error("Data transformation failed", error=str(e))
            return {
                'action': 'transform',
                'status': 'error',
                'error': str(e)
            }
    
    def action_load(self, tables: Optional[List[str]] = None,
                   input_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Load transformed data to database
        
        Args:
            tables: Optional list of tables to load
            input_file: Input file with transformed data
            
        Returns:
            Dictionary with loading results
        """
        if self.dry_run:
            self.logger.info("DRY RUN: Simulating data loading")
        
        self.logger.info("Starting data loading",
                        tables=tables,
                        input_file=input_file)
        
        try:
            # Load input data
            if input_file:
                input_path = Path(input_file)
                if not input_path.exists():
                    raise FileNotFoundError(f"Input file not found: {input_file}")
                
                with open(input_path, 'r', encoding='utf-8') as f:
                    transformed_data = json.load(f)
            else:
                # Extract and transform fresh data
                self.logger.info("No input file provided, processing fresh data")
                transform_result = self.action_transform(tables)
                if transform_result['status'] != 'success':
                    return transform_result
                transformed_data = transform_result['data']
            
            # Load data to database
            load_results = {}
            total_loaded = 0
            
            for table_name, table_data in transformed_data.items():
                if tables and table_name not in tables:
                    continue
                
                if not table_data:
                    self.logger.info("No data to load", table=table_name)
                    continue
                
                self.logger.info("Loading table data",
                               table=table_name,
                               records=len(table_data))
                
                if not self.dry_run:
                    # Use pipeline runner's load method
                    loaded_count = self.pipeline_runner.load_table_data(table_name, table_data)
                    load_results[table_name] = loaded_count
                    total_loaded += loaded_count
                else:
                    # Simulate loading
                    load_results[table_name] = len(table_data)
                    total_loaded += len(table_data)
                    self.logger.info("DRY RUN: Would load records",
                                   table=table_name,
                                   records=len(table_data))
            
            result = {
                'action': 'load',
                'status': 'success',
                'tables': list(load_results.keys()),
                'total_loaded': total_loaded,
                'table_results': load_results,
                'dry_run': self.dry_run
            }
            
            self.logger.info("Data loading completed",
                           tables=len(load_results),
                           total_loaded=total_loaded)
            
            return result
            
        except Exception as e:
            self.logger.error("Data loading failed", error=str(e))
            return {
                'action': 'load',
                'status': 'error',
                'error': str(e)
            }
    
    def action_full(self, tables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run full pipeline (extract, transform, load)
        
        Args:
            tables: Optional list of tables to process
            
        Returns:
            Dictionary with pipeline results
        """
        self.logger.info("Starting full pipeline run", tables=tables)
        
        try:
            # Use PipelineRunner for full pipeline
            result = self.pipeline_runner.run_pipeline(tables, self.dry_run)
            
            return {
                'action': 'full',
                'status': result['status'],
                'execution_id': result['execution_id'],
                'duration': result['duration'],
                'tables_processed': result['tables_processed'],
                'total_extracted': result['total_extracted'],
                'total_transformed': result['total_transformed'],
                'total_loaded': result['total_loaded'],
                'table_results': result['table_results'],
                'errors': result['errors']
            }
            
        except Exception as e:
            self.logger.error("Full pipeline failed", error=str(e))
            return {
                'action': 'full',
                'status': 'error',
                'error': str(e)
            }
    
    def action_test(self) -> Dict[str, Any]:
        """
        Test all pipeline components
        
        Returns:
            Dictionary with test results
        """
        self.logger.info("Starting pipeline component tests")
        
        test_results = {
            'action': 'test',
            'status': 'success',
            'tests': {},
            'errors': []
        }
        
        try:
            # Test API connectivity
            self.logger.info("Testing API connectivity")
            try:
                # Simple test with users endpoint
                test_data = self.extractor.api_client.extract_endpoint_data('users')
                test_results['tests']['api_connectivity'] = {
                    'status': 'success',
                    'sample_records': len(test_data[:5])  # First 5 records
                }
            except Exception as e:
                test_results['tests']['api_connectivity'] = {
                    'status': 'error',
                    'error': str(e)
                }
                test_results['errors'].append(f"API connectivity: {str(e)}")
            
            # Test database connectivity
            if not self.dry_run:
                self.logger.info("Testing database connectivity")
                try:
                    db_test = self.db_manager.execute_query("SELECT 1 AS test")
                    test_results['tests']['database_connectivity'] = {
                        'status': 'success',
                        'test_result': db_test[0]['test'] if db_test else None
                    }
                except Exception as e:
                    test_results['tests']['database_connectivity'] = {
                        'status': 'error',
                        'error': str(e)
                    }
                    test_results['errors'].append(f"Database connectivity: {str(e)}")
            else:
                test_results['tests']['database_connectivity'] = {
                    'status': 'skipped',
                    'reason': 'dry_run_mode'
                }
            
            # Test data transformation
            self.logger.info("Testing data transformation")
            try:
                # Sample data for transformation test
                sample_data = [{
                    'id': 'test-123',
                    'fullName': 'Test User',
                    'active': True,
                    'createdAt': '2023-12-01T14:30:00Z'
                }]
                
                transformed = self.transformer.transform_table_data('Leden', sample_data)
                test_results['tests']['data_transformation'] = {
                    'status': 'success',
                    'input_records': len(sample_data),
                    'output_records': len(transformed)
                }
            except Exception as e:
                test_results['tests']['data_transformation'] = {
                    'status': 'error',
                    'error': str(e)
                }
                test_results['errors'].append(f"Data transformation: {str(e)}")
            
            # Test configuration loading
            self.logger.info("Testing configuration loading")
            try:
                api_config = load_config(self.api_config_path)
                schema_config = load_config(self.schema_config_path)
                
                test_results['tests']['configuration'] = {
                    'status': 'success',
                    'api_endpoints': len(api_config.get('endpoints', {})),
                    'schema_tables': len(schema_config.get('tables', {}))
                }
            except Exception as e:
                test_results['tests']['configuration'] = {
                    'status': 'error',
                    'error': str(e)
                }
                test_results['errors'].append(f"Configuration: {str(e)}")
            
            # Overall status
            if test_results['errors']:
                test_results['status'] = 'partial_success' if any(
                    test['status'] == 'success' for test in test_results['tests'].values()
                ) else 'error'
            
            self.logger.info("Pipeline tests completed",
                           status=test_results['status'],
                           total_tests=len(test_results['tests']),
                           errors=len(test_results['errors']))
            
            return test_results
            
        except Exception as e:
            self.logger.error("Pipeline tests failed", error=str(e))
            return {
                'action': 'test',
                'status': 'error',
                'error': str(e)
            }
    
    def action_validate(self, tables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Validate data quality and schema compliance
        
        Args:
            tables: Optional list of tables to validate
            
        Returns:
            Dictionary with validation results
        """
        self.logger.info("Starting data validation", tables=tables)
        
        validation_results = {
            'action': 'validate',
            'status': 'success',
            'tables': {},
            'summary': {
                'total_tables': 0,
                'passed': 0,
                'warnings': 0,
                'failed': 0
            }
        }
        
        try:
            # Get tables to validate
            if tables:
                validate_tables = tables
            else:
                schema_config = load_config(self.schema_config_path)
                validate_tables = list(schema_config['tables'].keys())
            
            # Validate each table
            for table_name in validate_tables:
                self.logger.info("Validating table", table=table_name)
                
                table_validation = {
                    'status': 'success',
                    'checks': {},
                    'warnings': [],
                    'errors': []
                }
                
                try:
                    # Check if table exists
                    if not self.dry_run:
                        exists = self.db_manager.check_table_exists(table_name)
                        table_validation['checks']['table_exists'] = exists
                        
                        if exists:
                            # Check record count
                            record_count = self.db_manager.get_record_count(table_name)
                            table_validation['checks']['record_count'] = record_count
                            
                            if record_count == 0:
                                table_validation['warnings'].append("Table is empty")
                        else:
                            table_validation['errors'].append("Table does not exist")
                    
                    # Validate schema configuration
                    schema_config = load_config(self.schema_config_path)
                    table_config = schema_config['tables'].get(table_name)
                    
                    if table_config:
                        table_validation['checks']['schema_config'] = True
                        table_validation['checks']['column_count'] = len(table_config.get('columns', {}))
                    else:
                        table_validation['errors'].append("No schema configuration found")
                    
                    # Determine table status
                    if table_validation['errors']:
                        table_validation['status'] = 'error'
                        validation_results['summary']['failed'] += 1
                    elif table_validation['warnings']:
                        table_validation['status'] = 'warning'
                        validation_results['summary']['warnings'] += 1
                    else:
                        validation_results['summary']['passed'] += 1
                    
                    validation_results['tables'][table_name] = table_validation
                    validation_results['summary']['total_tables'] += 1
                    
                except Exception as e:
                    table_validation['status'] = 'error'
                    table_validation['errors'].append(str(e))
                    validation_results['tables'][table_name] = table_validation
                    validation_results['summary']['failed'] += 1
                    validation_results['summary']['total_tables'] += 1
            
            # Overall validation status
            if validation_results['summary']['failed'] > 0:
                validation_results['status'] = 'error'
            elif validation_results['summary']['warnings'] > 0:
                validation_results['status'] = 'warning'
            
            self.logger.info("Data validation completed",
                           status=validation_results['status'],
                           total_tables=validation_results['summary']['total_tables'],
                           passed=validation_results['summary']['passed'],
                           warnings=validation_results['summary']['warnings'],
                           failed=validation_results['summary']['failed'])
            
            return validation_results
            
        except Exception as e:
            self.logger.error("Data validation failed", error=str(e))
            return {
                'action': 'validate',
                'status': 'error',
                'error': str(e)
            }


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
        
        # Initialize manual runner
        runner = ManualRunner(args.config_dir, args.dry_run)
        
        if args.dry_run:
            logger.info("Running in dry-run mode - no database changes will be made")
        
        # Execute action
        logger.info("Executing action", action=args.action)
        
        start_time = datetime.now()
        
        if args.action == 'extract':
            result = runner.action_extract(tables, start_date, end_date, args.output_file)
        elif args.action == 'transform':
            result = runner.action_transform(tables, args.input_file, args.output_file)
        elif args.action == 'load':
            result = runner.action_load(tables, args.input_file)
        elif args.action == 'full':
            result = runner.action_full(tables)
        elif args.action == 'test':
            result = runner.action_test()
        elif args.action == 'validate':
            result = runner.action_validate(tables)
        else:
            raise ValueError(f"Unknown action: {args.action}")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Print results
        print("\n" + "="*60)
        print(f"🔧 Manual Run Results - {args.action.upper()}")
        print("="*60)
        print(f"Status: {result['status'].upper()}")
        print(f"Duration: {format_duration(duration)}")
        
        if 'total_records' in result:
            print(f"Total Records: {result['total_records']:,}")
        elif 'total_loaded' in result:
            print(f"Total Loaded: {result['total_loaded']:,}")
        
        if 'tables' in result and isinstance(result['tables'], list):
            print(f"Tables Processed: {', '.join(result['tables'])}")
        
        if 'output_file' in result and result['output_file']:
            print(f"Output File: {result['output_file']}")
        
        if 'errors' in result and result['errors']:
            print(f"\nErrors ({len(result['errors'])}):")
            for error in result['errors']:
                print(f"  - {error}")
        
        # Action-specific output
        if args.action == 'test':
            print(f"\nTest Results:")
            for test_name, test_result in result.get('tests', {}).items():
                status_icon = "✓" if test_result['status'] == 'success' else "✗"
                print(f"  {status_icon} {test_name}: {test_result['status']}")
        
        elif args.action == 'validate':
            summary = result.get('summary', {})
            print(f"\nValidation Summary:")
            print(f"  Passed: {summary.get('passed', 0)}")
            print(f"  Warnings: {summary.get('warnings', 0)}")
            print(f"  Failed: {summary.get('failed', 0)}")
        
        print("="*60)
        
        if result['status'] == 'success':
            logger.info("Manual run completed successfully")
            sys.exit(0)
        else:
            logger.error("Manual run completed with errors")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("Manual run interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main() 