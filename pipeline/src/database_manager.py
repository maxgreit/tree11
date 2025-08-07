"""
Database Manager for Tree11 Data Pipeline

Manages SQL Server connections and data operations for the Tree11 pipeline.
"""

import os
import json
import pandas as pd
import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from contextlib import contextmanager

from utils import load_config

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception for database operations"""
    pass


class DatabaseManager:
    """
    Manages database connections and operations for Tree11 data pipeline
    """
    
    def __init__(self, config_path: str = "config/database_config.json"):
        """
        Initialize DatabaseManager with configuration
        
        Args:
            config_path: Path to database configuration file
        """
        try:
            self.config = load_config(config_path)
            self.engine = self._create_engine()
            logger.info("Database manager initialized - config_file=%s", config_path)
        except Exception as e:
            logger.error("Failed to initialize database manager - error=%s", str(e))
            raise DatabaseError(f"Database initialization failed: {e}")
    
    def _create_engine(self) -> Engine:
        """
        Create SQLAlchemy engine with proper configuration
        
        Returns:
            SQLAlchemy Engine instance
        """
        try:
            # Build connection string
            connection_string = self._build_connection_string()
            
            # Create engine with connection pooling
            engine = create_engine(
                connection_string,
                pool_size=self.config.get("connection_pool", {}).get("pool_size", 5),
                max_overflow=self.config.get("connection_pool", {}).get("max_overflow", 10),
                pool_timeout=self.config.get("connection_pool", {}).get("pool_timeout", 30),
                pool_recycle=self.config.get("connection_pool", {}).get("pool_recycle", 3600),
                echo=False  # Set to True for SQL query logging
            )
            
            logger.info("Database engine created successfully")
            return engine
            
        except Exception as e:
            logger.error("Failed to create database engine - error=%s", str(e))
            raise DatabaseError(f"Engine creation failed: {e}")
    
    def _build_connection_string(self) -> str:
        """
        Build SQL Server connection string from configuration and environment
        
        Returns:
            Complete connection string for SQL Server
        """
        try:
            # Get database settings from environment variables
            server = os.getenv('DB_SERVER')
            database = os.getenv('DB_NAME') 
            username = os.getenv('DB_USERNAME')
            password = os.getenv('DB_PASSWORD')
            driver = os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server')
            
            if not all([server, database, username, password]):
                missing = [var for var in ['DB_SERVER', 'DB_NAME', 'DB_USERNAME', 'DB_PASSWORD'] 
                          if not os.getenv(var)]
                raise ValueError(f"Missing required environment variables: {missing}")
            
            # Build connection string with SSL settings for Azure SQL
            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{server}/{database}"
                f"?driver={driver.replace(' ', '+')}"
                f"&TrustServerCertificate=yes"
                f"&Connection+Timeout=30"
                f"&Encrypt=yes"
            )
            
            logger.debug(f"Connection string built successfully - server={server}, database={database}")
            return connection_string
            
        except Exception as e:
            logger.error(f"Failed to build connection string - error={str(e)}")
            raise DatabaseError(f"Connection string error: {e}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections
        
        Yields:
            Database connection with automatic cleanup
        """
        connection = None
        try:
            connection = self.engine.connect()
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database connection error - error={str(e)}")
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Execute SQL query and return results
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            List of dictionaries with query results
        """
        try:
            with self.get_connection() as conn:
                result = conn.execute(text(query), params or {})
                
                if result.returns_rows:
                    columns = result.keys()
                    rows = result.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
                else:
                    return []
                    
        except Exception as e:
            logger.error(f"Query execution failed - query={query[:100]}, error={str(e)}")
            raise DatabaseError(f"Query failed: {e}")
    
    def check_schema_exists(self) -> bool:
        """
        Check if the configured schema exists in database
        
        Returns:
            True if schema exists, False otherwise
        """
        try:
            query = """
                SELECT COUNT(*) as schema_count 
                FROM INFORMATION_SCHEMA.SCHEMATA 
                WHERE SCHEMA_NAME = :schema_name
            """
            result = self.execute_query(query, {
                "schema_name": self.config['database']['schema']
            })
            schema_exists = result[0]["schema_count"] > 0
            
            if not schema_exists:
                logger.error(f"Schema does not exist - schema={self.config['database']['schema']}")
            
            return schema_exists
            
        except Exception as e:
            logger.error(f"Schema existence check failed - schema={self.config['database']['schema']}, error={str(e)}")
            return False

    def check_table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database
        
        Args:
            table_name: Name of table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            query = """
                SELECT COUNT(*) as table_count 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = :table_name AND TABLE_SCHEMA = :schema_name
            """
            result = self.execute_query(query, {
                "table_name": table_name,
                "schema_name": self.config['database']['schema']
            })
            return result[0]["table_count"] > 0
            
        except Exception as e:
            logger.error(f"Table existence check failed - table={table_name}, error={str(e)}")
            return False

    def get_record_count(self, table_name: str) -> int:
        """
        Get the number of records in a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of records in the table
        """
        try:
            query = f"""
                SELECT COUNT(*) as record_count 
                FROM [{self.config['database']['schema']}].[{table_name}]
            """
            result = self.execute_query(query)
            return result[0]["record_count"] if result else 0
            
        except Exception as e:
            logger.error(f"Record count query failed - table={table_name}, error={str(e)}")
            return 0

    def load_table_data(self, table_name: str, data: List[Dict], update_strategy: str = 'upsert') -> int:
        """
        Load data into a database table
        
        Args:
            table_name: Name of the target table
            data: List of dictionaries containing the data to load
            update_strategy: Strategy for handling existing data ('insert', 'replace', 'upsert')
            
        Returns:
            Number of records loaded
        """
        if not data:
            logger.warning(f"No data provided for table {table_name}")
            return 0
        
        records_count = len(data)
        logger.info(f"Data inladen voor: {table_name}, rijen: {records_count}, strategie: {update_strategy}")
        
        try:
            # Check if schema exists
            if not self.check_schema_exists():
                raise DatabaseError(f"Schema '{self.config['database']['schema']}' does not exist. Please run the database setup scripts first.")
            
            # Check if table exists
            if not self.check_table_exists(table_name):
                raise DatabaseError(f"Table '{self.config['database']['schema']}.{table_name}' does not exist. Please run the database setup scripts first.")
            
            # Convert data to DataFrame
            df = pd.DataFrame(data)
            
            # Validate and fix column types
            df = self._validate_and_fix_columns(df, table_name)
            
            # Check if this table has composite primary keys
            composite_keys = self._get_composite_primary_keys(table_name)
            
            with self.get_connection() as conn:
                # Handle different update strategies
                if update_strategy == 'replace':
                    # For replace strategy: first delete all data, then insert
                    with conn.begin():
                        # Delete existing data
                        delete_query = f"DELETE FROM [{self.config['database']['schema']}].[{table_name}]"
                        conn.execute(text(delete_query))
                        logger.debug(f"Existing data deleted for replace strategy - table={table_name}")
                        
                        # Insert new data
                        df.to_sql(
                            name=table_name,
                            con=conn,
                            schema=self.config['database']['schema'],
                            if_exists='append',
                            index=False
                        )
                        
                elif update_strategy == 'upsert':
                    # For upsert strategy: use appropriate method based on key type
                    if len(composite_keys) > 1:
                        # Use composite key upsert
                        self._perform_upsert_composite(conn, table_name, df)
                    else:
                        # Use single key upsert
                        self._perform_upsert(conn, table_name, df)
                        
                elif update_strategy == 'insert':
                    # For insert strategy: simple append (may cause primary key violations)
                    df.to_sql(
                        name=table_name,
                        con=conn,
                        schema=self.config['database']['schema'],
                        if_exists='append',
                        index=False
                    )
                        
                else:
                    raise ValueError(f"Unsupported update strategy: {update_strategy}")
            
            logger.debug(f"Data load operation completed successfully - table={table_name}, loaded_records={records_count}, strategy={update_strategy}")
            
            return records_count
            
        except Exception as e:
            logger.error(f"Failed to load data into {table_name}: {e}")
            raise DatabaseError(f"Failed to load data into {table_name}: {e}")

    def _validate_and_fix_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Validate DataFrame columns against database schema and fix mismatches
        
        Args:
            df: DataFrame to validate and fix
            table_name: Name of target table
            
        Returns:
            Fixed DataFrame with correct columns
        """
        try:
            # Get expected database columns
            expected_columns = self._get_table_columns(table_name)
            current_columns = list(df.columns)
            
            logger.debug(f"Column validation - table={table_name}, df_columns={len(current_columns)}, db_columns={len(expected_columns)}")
            
            # Check for missing columns
            missing_columns = set(expected_columns) - set(current_columns)
            if missing_columns:
                logger.warning(f"Missing columns detected - table={table_name}, missing={missing_columns}")
                
                # Add missing columns with default values
                for col in missing_columns:
                    if col == 'DatumLaatsteUpdate':
                        df[col] = pd.Timestamp.now()
                    else:
                        # Add with appropriate default based on expected type
                        df[col] = self._get_default_value_for_column(col)
                        
                logger.info(f"Added missing columns - table={table_name}, added={missing_columns}")
            
            # Check for extra columns
            extra_columns = set(current_columns) - set(expected_columns)
            if extra_columns:
                logger.warning(f"Extra columns detected - table={table_name}, extra={extra_columns}")
                df = df.drop(columns=list(extra_columns))
                logger.info(f"Removed extra columns - table={table_name}, removed={extra_columns}")
            
            # Reorder columns to match database order
            df = df[expected_columns]
            
            logger.debug(f"Column validation completed - table={table_name}, final_columns={len(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.warning(f"Column validation failed - table={table_name}, error={str(e)}, proceeding with original DataFrame")
            return df

    def _get_table_columns(self, table_name: str) -> List[str]:
        """
        Get expected columns for a table from database schema
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column names in correct order
        """
        try:
            query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
            """
            
            result = self.execute_query(query, {
                'schema': self.config['database']['schema'],
                'table_name': table_name
            })
            
            return [row['COLUMN_NAME'] for row in result]
            
        except Exception as e:
            logger.warning(f"Could not get table schema - table={table_name}, error={str(e)}")
            
            # Fallback to expected columns based on table name
            return self._get_fallback_columns(table_name)

    def _get_fallback_columns(self, table_name: str) -> List[str]:
        """
        Get fallback column list when database query fails
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of expected column names
        """
        # Define expected columns for each table (from create_tables.sql)
        fallback_schemas = {
            'Lessen': [
                'Id', 'Naam', 'StartTijd', 'EindTijd', 'Capaciteit', 
                'LedenAantal', 'ProefledenAantal', 'BedrijfsLocatieId', 
                'Activiteiten', 'Terugkerend', 'AangemaaktOp', 'GewijzigdOp', 
                'Instructeurs', 'DatumLaatsteUpdate'
            ],
            'Leden': [
                'AccountId', 'BedrijfId', 'PrimaireLocatieId', 'Actief',
                'BetalingsVorm', 'KlantNummer', 'AangemaaktOp', 'GewijzigdOp',
                'DatumLaatsteUpdate'
            ],
            'ActieveAbonnementen': [
                'LedenId', 'AbonnementId', 'AbonnementNaam', 'Status', 'DatumLaatsteUpdate'
            ],
            'Abonnementen': [
                'AbonnementId', 'Naam', 'Beschrijving', 'Type', 'BetalingsType', 'Bedrag',
                'Valuta', 'VervalPeriode', 'ActivatieStrategie', 'AbonnementVereist',
                'ConsumptieMethode', 'AutoVerlenging', 'GrootboekGroepId', 'Sectie', 'DatumLaatsteUpdate'
            ],
            'OpenstaandeFacturen': [
                'Id', 'Nummer', 'NummerFormatted', 'Status', 'Type', 'Jaar',
                'BedrijfsLocatieId', 'TotaalBedrag', 'AangemaaktOp', 'DatumLaatsteUpdate'
            ],
            'AbonnementStatistieken': [
                'Datum', 'Categorie', 'Type', 'Aantal', 'DatumLaatsteUpdate'
            ],
            'Omzet': [
                'Datum', 'GrootboekRekeningId', 'Type', 'Omzet', 'DatumLaatsteUpdate'
            ],
            'GrootboekRekening': [
                'Id', 'Sleutel', 'Label', 'DatumLaatsteUpdate'
            ],
            'PersonalTraining': [
                'Id', 'Voornaam', 'Achternaam', 'Datum', 'Uren', 'DatumLaatsteUpdate'
            ]
        }
        
        return fallback_schemas.get(table_name, ['DatumLaatsteUpdate'])

    def _get_default_value_for_column(self, column_name: str):
        """
        Get appropriate default value for a column based on its name
        
        Args:
            column_name: Name of the column
            
        Returns:
            Default value for the column
        """
        # Map column patterns to default values
        if column_name.endswith('Id'):
            return ''
        elif column_name.endswith('Aantal'):
            return 0
        elif column_name.endswith('Bedrag'):
            return 0.0
        elif column_name.endswith('Op') or column_name.endswith('Tijd'):
            return pd.Timestamp.now()
        elif column_name == 'Actief' or column_name == 'Terugkerend':
            return False
        else:
            return ''

    def _perform_upsert(self, conn, table_name: str, df: pd.DataFrame):
        """
        Perform UPSERT operation by deleting conflicting records and inserting new data
        
        Args:
            conn: Database connection
            table_name: Name of target table
            df: DataFrame with data to upsert
        """
        try:
            # First check if this table has a composite primary key
            composite_keys = self._get_composite_primary_keys(table_name)
            
            if len(composite_keys) > 1:
                # This table has a composite primary key, use composite upsert
                logger.info(f"Using composite UPSERT for table with composite primary key - table={table_name}, keys={composite_keys}")
                self._perform_upsert_composite(conn, table_name, df)
                return
            
            # Single primary key handling
            primary_key = self._get_primary_key(table_name)
            
            if primary_key and primary_key in df.columns:
                # Remove duplicates from DataFrame before processing
                original_count = len(df)
                df_clean = df.drop_duplicates(subset=[primary_key], keep='last')
                duplicate_count = original_count - len(df_clean)
                
                if duplicate_count > 0:
                    logger.warning(f"Removed {duplicate_count} duplicates from DataFrame - table={table_name}, primary_key={primary_key}")
                
                # Extract primary key values from DataFrame and filter out NULL/empty values
                # Convert to string and handle potential list values
                pk_values = []
                for val in df_clean[primary_key].dropna():
                    if isinstance(val, list):
                        # If it's a list, convert to string representation
                        pk_values.append(str(val))
                    else:
                        pk_values.append(str(val))
                
                # Remove duplicates and empty strings (safer approach)
                unique_pk_values = []
                seen = set()
                for pk in pk_values:
                    if pk and pk.strip() and pk not in seen:
                        unique_pk_values.append(pk)
                        seen.add(pk)
                
                logger.debug(f"Primary key values for upsert - table={table_name}, pk={primary_key}, valid_values={len(unique_pk_values)}")
                
                # Convert to proper format for SQL query
                if unique_pk_values:
                    with conn.begin():
                        # Process in batches to avoid SQL Server parameter limit (2100 parameters)
                        batch_size = 1000
                        total_deleted = 0
                        
                        for i in range(0, len(unique_pk_values), batch_size):
                            batch = unique_pk_values[i:i + batch_size]
                            
                            # Create placeholders for the IN clause using named parameters
                            placeholders = ', '.join([f':pk_{j}' for j in range(len(batch))])
                            
                            # Delete existing records with conflicting primary keys
                            delete_query = f"""
                            DELETE FROM [{self.config['database']['schema']}].[{table_name}]
                            WHERE [{primary_key}] IN ({placeholders})
                            """
                            
                            # Create parameter dict
                            params = {f'pk_{j}': val for j, val in enumerate(batch)}
                            
                            logger.debug(f"Executing DELETE batch {i//batch_size + 1} - table={table_name}, batch_size={len(batch)}")
                            
                            result = conn.execute(text(delete_query), params)
                            deleted_count = result.rowcount if result.rowcount else 0
                            total_deleted += deleted_count
                        
                        logger.debug(f"Total deleted {total_deleted} conflicting records - table={table_name}, primary_key={primary_key}")
                        
                        # Insert all new data (using cleaned DataFrame)
                        df_clean.to_sql(
                            name=table_name,
                            con=conn,
                            schema=self.config['database']['schema'],
                            if_exists='append',
                            index=False
                        )
                        
                        logger.info(f"UPSERT afgerond voor: {table_name}, verwijderd: {total_deleted}, ingevoegd: {len(df_clean)}")
                        
                else:
                    logger.warning(f"No valid primary key values found for upsert - table={table_name}, total_records={len(df_clean)}")
                    # If no valid PK values, just insert without deleting
                    df_clean.to_sql(
                        name=table_name,
                        con=conn,
                        schema=self.config['database']['schema'],
                        if_exists='append',
                        index=False
                    )
                    logger.info(f"Simple insert completed (no valid PKs) - table={table_name}, inserted={len(df_clean)}")
                    
            else:
                logger.warning(f"Primary key not found or not in DataFrame - table={table_name}, pk={primary_key}, falling back to simple insert")
                # Fallback to simple insert if primary key detection fails
                # Still remove duplicates even without primary key
                original_count = len(df)
                df_clean = df.drop_duplicates(keep='last')
                duplicate_count = original_count - len(df_clean)
                
                if duplicate_count > 0:
                    logger.warning(f"Removed {duplicate_count} duplicates from DataFrame (no primary key) - table={table_name}")
                
                df_clean.to_sql(
                    name=table_name,
                    con=conn,
                    schema=self.config['database']['schema'],
                    if_exists='append',
                    index=False
                )
                
        except Exception as e:
            logger.error(f"UPSERT operation failed - table={table_name}, error={str(e)}")
            # Log additional details for debugging
            if primary_key and primary_key in df.columns:
                pk_sample = df[primary_key].head(5).tolist()
                logger.error(f"Primary key sample values - table={table_name}, pk={primary_key}, sample={pk_sample}")
            raise

    def _get_primary_key(self, table_name: str) -> str:
        """
        Get primary key column name for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Primary key column name or empty string if not found
        """
        try:
            # First try to get single primary key
            query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = :schema 
              AND TABLE_NAME = :table_name
              AND CONSTRAINT_NAME LIKE 'PK%'
            ORDER BY ORDINAL_POSITION
            """
            
            result = self.execute_query(query, {
                'schema': self.config['database']['schema'],
                'table_name': table_name
            })
            
            if result:
                return result[0]['COLUMN_NAME']
                
        except Exception as e:
            logger.warning(f"Could not get primary key from database - table={table_name}, error={str(e)}")
        
        return ''

    def _get_composite_primary_keys(self, table_name: str) -> List[str]:
        """
        Get all primary key column names for a table (for composite keys)
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of primary key column names
        """
        try:
            query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = :schema 
              AND TABLE_NAME = :table_name
              AND CONSTRAINT_NAME LIKE 'PK%'
            ORDER BY ORDINAL_POSITION
            """
            
            result = self.execute_query(query, {
                'schema': self.config['database']['schema'],
                'table_name': table_name
            })
            
            if result:
                return [row['COLUMN_NAME'] for row in result]
                
        except Exception as e:
            logger.warning(f"Could not get composite primary keys from database - table={table_name}, error={str(e)}")
        
        return []

    def _perform_upsert_composite(self, conn, table_name: str, df: pd.DataFrame):
        """
        Perform UPSERT operation for tables with composite primary keys
        
        Args:
            conn: Database connection
            table_name: Name of target table
            df: DataFrame with data to upsert
        """
        try:
            # Get all primary key columns
            primary_keys = self._get_composite_primary_keys(table_name)
            
            if primary_keys and all(pk in df.columns for pk in primary_keys):
                # Remove duplicates from DataFrame before processing
                original_count = len(df)
                df_clean = df.drop_duplicates(subset=primary_keys, keep='last')
                duplicate_count = original_count - len(df_clean)
                
                if duplicate_count > 0:
                    logger.warning(f"{duplicate_count} Duplicaten verwijderd uit DataFrame: {table_name}, op basis van: {primary_keys}")
                
                # For composite keys, we need to delete based on all key columns
                with conn.begin():
                    # Extract unique composite key values from DataFrame
                    composite_values = []
                    for _, row in df_clean.iterrows():
                        key_values = tuple(row[pk] for pk in primary_keys)
                        composite_values.append(key_values)
                    
                    # Remove duplicates from composite values
                    unique_composite_values = list(set(composite_values))
                    
                    if unique_composite_values:
                        # Process in batches to avoid SQL Server parameter limit (2100 parameters)
                        batch_size = 100
                        total_deleted = 0
                        
                        for i in range(0, len(unique_composite_values), batch_size):
                            batch = unique_composite_values[i:i + batch_size]
                            
                            # Create placeholders for the DELETE clause
                            placeholders = []
                            params = {}
                            
                            for j, composite_value in enumerate(batch):
                                # Create condition for each composite key combination
                                conditions = []
                                for k, pk in enumerate(primary_keys):
                                    param_name = f'pk_{j}_{k}'
                                    conditions.append(f"[{pk}] = :{param_name}")
                                    params[param_name] = composite_value[k]
                                
                                placeholders.append(f"({' AND '.join(conditions)})")
                            
                            # Delete existing records with conflicting composite primary keys
                            delete_query = f"""
                            DELETE FROM [{self.config['database']['schema']}].[{table_name}]
                            WHERE {' OR '.join(placeholders)}
                            """
                            
                            logger.debug(f"Executing DELETE batch {i//batch_size + 1} - table={table_name}, batch_size={len(batch)}")
                            
                            result = conn.execute(text(delete_query), params)
                            deleted_count = result.rowcount if result.rowcount else 0
                            total_deleted += deleted_count
                        
                        logger.debug(f"Total deleted {total_deleted} conflicting records - table={table_name}, composite_keys={primary_keys}")
                        
                        # Insert all new data
                        df_clean.to_sql(
                            name=table_name,
                            con=conn,
                            schema=self.config['database']['schema'],
                            if_exists='append',
                            index=False
                        )
                        
                        logger.info(f"Composite UPSERT completed - table={table_name}, deleted={total_deleted}, inserted={len(df_clean)}")
                        
                    else:
                        logger.warning(f"No valid composite key values found for upsert - table={table_name}, total_records={len(df_clean)}")
                        # If no valid composite key values, just insert without deleting
                        df_clean.to_sql(
                            name=table_name,
                            con=conn,
                            schema=self.config['database']['schema'],
                            if_exists='append',
                            index=False
                        )
                        logger.info(f"Simple insert completed (no valid composite keys) - table={table_name}, inserted={len(df_clean)}")
                    
            else:
                logger.warning(f"Composite primary keys not found in DataFrame - table={table_name}, falling back to simple insert")
                # Still remove duplicates even without composite primary key
                original_count = len(df)
                df_clean = df.drop_duplicates(keep='last')
                duplicate_count = original_count - len(df_clean)
                
                if duplicate_count > 0:
                    logger.warning(f"Removed {duplicate_count} duplicates from DataFrame (no composite primary key) - table={table_name}")
                
                df_clean.to_sql(
                    name=table_name,
                    con=conn,
                    schema=self.config['database']['schema'],
                    if_exists='append',
                    index=False
                )
                
        except Exception as e:
            logger.error(f"Composite UPSERT operation failed - table={table_name}, error={str(e)}")
            raise
    
    def log_pipeline_error(self, execution_id: str, error_msg: str) -> None:
        """
        Log a pipeline execution error (database logging disabled)
        
        Args:
            execution_id: Unique identifier for the pipeline execution
            error_msg: Error message to log
        """
        # Database logging is disabled - just log to console
        logger.error(f"Pipeline error - execution_id={execution_id}, error={error_msg}")

    def close(self) -> None:
        """
        Close database connections and cleanup resources
        """
        try:
            if hasattr(self, 'engine') and self.engine:
                self.engine.dispose()
                logger.info("Database connecties gesloten")
        except Exception as e:
            logger.error(f"Failed to close database connections - error={str(e)}")
