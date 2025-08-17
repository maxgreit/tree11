#!/usr/bin/env python3
"""
Tree11 Data Pipeline - Main Entry Point
Dagelijkse uitvoering van de volledige data pipeline met dependency management

Usage:
    python main.py [--tables table1,table2] [--config-dir config/] [--dry-run] [--verbose]
    python main.py --lessen --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--config-dir config/] [--dry-run] [--verbose]
"""

# Standaard imports
from datetime import datetime, timedelta
from typing import List, Optional
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
import traceback
import argparse
import logging
import sys
import os

# Voeg src directory toe aan pad
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Interne imports
from src.pipeline_runner import PipelineRunner
from src.logger import setup_simple_logging


def parse_arguments():
    """Argumenten parseren"""
    parser = argparse.ArgumentParser(
        description='Tree11 Data Pipeline - Dagelijkse data synchronisatie met dependency management'
    )
    
    parser.add_argument(
        '--tables',
        type=str,
        help='Kommagescheiden lijst van tabellen om te verwerken (standaard: alle). Ondersteunt dependencies.',
        default=None
    )
    
    parser.add_argument(
        '--config-dir',
        type=str,
        help='Pad naar configuratie directory',
        default='config'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Droog draaien - geen database wijzigingen'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Uitgebreide logging met debug informatie'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Forceer uitvoering ook bij recente run (voor toekomstig gebruik)'
    )
    
    parser.add_argument(
        '--health-check-only',
        action='store_true',
        help='Voer alleen health checks uit en stop'
    )
    
    parser.add_argument(
        '--show-dependencies',
        action='store_true',
        help='Toon tabel dependencies en verwerkingsvolgorde'
    )
    
    parser.add_argument(
        '--skip-health-checks',
        action='store_true',
        help='Sla database health checks over (voor debugging)'
    )
    
    parser.add_argument(
        '--historical',
        action='store_true',
                               help='Haal historische data op in plaats van dagelijkse data. Alle tabellen worden automatisch per maand verwerkt als periode > 31 dagen.'
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        help='Startdatum voor historische data (YYYY-MM-DD formaat)',
        default=None
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='Einddatum voor historische data (YYYY-MM-DD formaat)',
        default=None
    )
    
    parser.add_argument(
        '--historical-tables',
        type=str,
        help='Kommagescheiden lijst van tabellen voor historische data (standaard: Lessen,LesDeelname,Omzet,GrootboekRekening,AbonnementStatistiekenSpecifiek)',
        default='Lessen,LesDeelname,Omzet,GrootboekRekening,AbonnementStatistiekenSpecifiek'
    )
    
    parser.add_argument(
        '--weekly',
        action='store_true',
        help='Verwerk historische periode in wekelijkse batches (in plaats van maandelijks)'
    )
    
    parser.add_argument(
        '--disable-monthly-split',
        action='store_true',
        help='Schakel maandelijkse splitsing uit voor historische runs (>31 dagen)'
    )
    
    parser.add_argument(
        '--no-monthly-split-tables',
        type=str,
        default='AbonnementStatistiekenSpecifiek',
        help='Komma-gescheiden lijst van tabellen die niet per maand gesplitst moeten worden bij historische runs'
    )
    

    
    return parser.parse_args()


def split_date_range_by_months(start_date: str, end_date: str) -> List[tuple]:
    """
    Splits een datumrange in maandelijkse periodes
    
    Args:
        start_date: Startdatum in YYYY-MM-DD formaat
        end_date: Einddatum in YYYY-MM-DD formaat
        
    Returns:
        Lijst van tuples met (start_date, end_date) per maand
    """
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    periods = []
    current_start = start_dt
    
    while current_start <= end_dt:
        # Bereken einde van huidige maand
        if current_start.month == 12:
            next_month = current_start.replace(year=current_start.year + 1, month=1, day=1)
        else:
            next_month = current_start.replace(month=current_start.month + 1, day=1)
        
        # Einde van huidige maand is dag voor volgende maand
        current_end = next_month - timedelta(days=1)
        
        # Als einde van maand na einddatum, gebruik einddatum
        if current_end > end_dt:
            current_end = end_dt
        
        periods.append((
            current_start.strftime('%Y-%m-%d'),
            current_end.strftime('%Y-%m-%d')
        ))
        
        # Volgende periode start op eerste dag van volgende maand
        current_start = next_month
    
    return periods


def split_date_range_by_weeks(start_date: str, end_date: str) -> List[tuple]:
    """
    Splits een datumrange in weekperiodes (ma t/m zo), beginnend bij start_date.
    De eerste periode loopt van start_date t/m de eerstvolgende zondag.
    """
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    periods = []
    current_start = start_dt

    while current_start <= end_dt:
        # Weekeinde bepalen (zondag = 6)
        weekday = current_start.weekday()  # ma=0, zo=6
        days_to_sunday = 6 - weekday
        current_end = min(current_start + timedelta(days=days_to_sunday), end_dt)

        periods.append((
            current_start.strftime('%Y-%m-%d'),
            current_end.strftime('%Y-%m-%d')
        ))

        current_start = current_end + timedelta(days=1)

    return periods


def run_monthly_pipeline(runner: PipelineRunner, start_date: str, end_date: str, 
                        tables: List[str] = None, dry_run: bool = False, 
                        skip_health_checks: bool = False, verbose: bool = False) -> dict:
    """
    Pipeline voor data met automatische maandelijkse verwerking
    
    Args:
        runner: PipelineRunner instance
        start_date: Startdatum in YYYY-MM-DD formaat
        end_date: Einddatum in YYYY-MM-DD formaat
        tables: Lijst van tabellen om te verwerken (standaard: ['Lessen'])
        dry_run: Of het een droge run is
        skip_health_checks: Of health checks overgeslagen moeten worden
        verbose: Of uitgebreide logging gewenst is
        
    Returns:
        Dictionary met resultaten van alle maandelijkse runs
    """
    if tables is None:
        tables = ['Lessen']
    # Bereken totale periode
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    total_days = (end_dt - start_dt).days
    
    logging.debug(f"Maandelijkse pipeline gestart - periode: {start_date} tot {end_date} ({total_days} dagen), tabellen: {tables}")
    
    # Bepaal of we per maand moeten verwerken (meer dan 31 dagen)
    if total_days > 31:
        logging.debug(f"Periode is langer dan 1 maand ({total_days} dagen) - verwerken per maand")
        periods = split_date_range_by_months(start_date, end_date)
        logging.debug(f"Opgesplitst in {len(periods)} maandelijkse periodes")
        
        all_results = {
            'status': 'success',
            'total_periods': len(periods),
            'periods_processed': 0,
            'period_results': [],
            'total_extracted': 0,
            'total_transformed': 0,
            'total_loaded': 0,
            'errors': [],
            'periodicity': 'monthly'
        }
        
        for i, (period_start, period_end) in enumerate(periods, 1):
            logging.debug(f"Verwerken periode {i}/{len(periods)}: {period_start} tot {period_end}")
            
            try:
                # Run pipeline voor deze periode
                period_result = runner.run_pipeline(
                    tables=tables,
                    dry_run=dry_run,
                    skip_health_checks=skip_health_checks,
                    historical=True,
                    start_date=period_start,
                    end_date=period_end
                )
                
                # Update totals
                all_results['total_extracted'] += period_result.get('total_extracted', 0)
                all_results['total_transformed'] += period_result.get('total_transformed', 0)
                all_results['total_loaded'] += period_result.get('total_loaded', 0)
                
                if period_result['status'] == 'success':
                    all_results['periods_processed'] += 1
                    logging.debug(f"Periode {i} succesvol verwerkt - {period_result.get('total_loaded', 0)} records geladen")
                else:
                    all_results['errors'].append({
                        'period': f"{period_start} tot {period_end}",
                        'error': f"Status: {period_result['status']}",
                        'details': period_result.get('errors', [])
                    })
                    logging.warning(f"Periode {i} verwerkt met errors")
                
                all_results['period_results'].append({
                    'period': f"{period_start} tot {period_end}",
                    'result': period_result
                })
                
            except Exception as e:
                error_msg = f"Fout bij verwerken periode {period_start} tot {period_end}: {str(e)}"
                all_results['errors'].append({
                    'period': f"{period_start} tot {period_end}",
                    'error': str(e)
                })
                logging.error(error_msg)
        
        # Bepaal eindstatus
        if all_results['errors']:
            if all_results['periods_processed'] == 0:
                all_results['status'] = 'error'
            else:
                all_results['status'] = 'partial_success'
        
        logging.info(f"Maandelijkse pipeline voltooid - {all_results['periods_processed']}/{all_results['total_periods']} periodes succesvol")
        logging.info(f"Totaal records: {all_results['total_loaded']} geladen")
        
        return all_results
        
    else:
        logging.debug(f"Periode is 1 maand of korter ({total_days} dagen) - direct verwerken")
        
        # Direct verwerken als periode <= 1 maand
        return runner.run_pipeline(
            tables=tables,
            dry_run=dry_run,
            skip_health_checks=skip_health_checks,
            historical=True,
            start_date=start_date,
            end_date=end_date
        )


def run_weekly_pipeline(runner: PipelineRunner, start_date: str, end_date: str,
                        tables: List[str] = None, dry_run: bool = False,
                        skip_health_checks: bool = False, verbose: bool = False) -> dict:
    """
    Pipeline voor data met automatische wekelijkse verwerking
    """
    if tables is None:
        tables = ['Lessen']

    # Periodes splitsen in weken indien > 7 dagen
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    total_days = (end_dt - start_dt).days

    logging.debug(f"Wekelijkse pipeline gestart - periode: {start_date} tot {end_date} ({total_days} dagen), tabellen: {tables}")

    if total_days > 7:
        periods = split_date_range_by_weeks(start_date, end_date)
        logging.debug(f"Opgesplitst in {len(periods)} wekelijkse periodes")

        all_results = {
            'status': 'success',
            'total_periods': len(periods),
            'periods_processed': 0,
            'period_results': [],
            'total_extracted': 0,
            'total_transformed': 0,
            'total_loaded': 0,
            'errors': [],
            'periodicity': 'weekly'
        }

        for i, (period_start, period_end) in enumerate(periods, 1):
            logging.debug(f"Verwerken periode {i}/{len(periods)}: {period_start} tot {period_end}")
            try:
                period_result = runner.run_pipeline(
                    tables=tables,
                    dry_run=dry_run,
                    skip_health_checks=skip_health_checks,
                    historical=True,
                    start_date=period_start,
                    end_date=period_end
                )

                all_results['total_extracted'] += period_result.get('total_extracted', 0)
                all_results['total_transformed'] += period_result.get('total_transformed', 0)
                all_results['total_loaded'] += period_result.get('total_loaded', 0)

                if period_result['status'] == 'success':
                    all_results['periods_processed'] += 1
                else:
                    all_results['errors'].append({
                        'period': f"{period_start} tot {period_end}",
                        'error': f"Status: {period_result['status']}",
                        'details': period_result.get('errors', [])
                    })

                all_results['period_results'].append({
                    'period': f"{period_start} tot {period_end}",
                    'result': period_result
                })

            except Exception as e:
                all_results['errors'].append({
                    'period': f"{period_start} tot {period_end}",
                    'error': str(e)
                })
                logging.error(f"Fout bij verwerken periode {period_start} tot {period_end}: {str(e)}")

        if all_results['errors']:
            all_results['status'] = 'error' if all_results['periods_processed'] == 0 else 'partial_success'

        logging.info(f"Wekelijkse pipeline voltooid - {all_results['periods_processed']}/{all_results['total_periods']} periodes succesvol")
        logging.info(f"Totaal records: {all_results['total_loaded']} geladen")
        return all_results

    # Direct verwerken als periode <= 7 dagen
    return runner.run_pipeline(
        tables=tables,
        dry_run=dry_run,
        skip_health_checks=skip_health_checks,
        historical=True,
        start_date=start_date,
        end_date=end_date
    )

def show_table_dependencies(runner: PipelineRunner, specific_tables: Optional[List[str]] = None):
    """Toon tabel dependencies en verwerkingsvolgorde"""
    dependencies = runner.get_table_dependencies()
    
    # Show all dependencies
    for table, deps in dependencies.items():
        if deps:
            deps_str = ", ".join(deps)
            logging.debug(f"{table} → afhankelijk van: {deps_str}")
        else:
            logging.debug(f"{table} → geen dependencies")
    
    processing_order = runner.get_processing_order(specific_tables)
    
    for i, table in enumerate(processing_order, 1):
        logging.debug(f"  {i}. {table}")
    
    logging.debug(f"Totaal te verwerken tabellen: {len(processing_order)}")
    
    # Speciale notificaties
    if 'ActieveAbonnementen' in processing_order:
        logging.debug("ActieveAbonnementen wordt afgeleid van Leden data")
    
    if specific_tables:
        filtered_out = set(dependencies.keys()) - set(processing_order)
        if filtered_out:
            logging.debug(f"Uitgefilterd: {', '.join(filtered_out)}")

def print_execution_summary(result: dict):
    """Print een mooie samenvatting van de pipeline uitvoering"""
    logging.info(f"Status: {result['status'].upper()}")
    
    # Periode-gebaseerd resultaat (zowel maandelijks als wekelijks)
    if 'period_results' in result and 'total_periods' in result:
        periodicity = result.get('periodicity', 'periodieke')
        label = 'MAANDELIJKSE' if periodicity == 'monthly' else ('WEKELIJKSE' if periodicity == 'weekly' else 'PERIODIEKE')
        logging.info(f"=== SAMENVATTING {label} PIPELINE ===")
        logging.info(f"Totaal periodes: {result['total_periods']}")
        logging.info(f"Periodes succesvol verwerkt: {result['periods_processed']}")
        logging.info(f"Totaal geëxtraheerd: {result['total_extracted']:,} records")
        logging.info(f"Totaal getransformeerd: {result['total_transformed']:,} records")
        logging.info(f"Totaal geladen: {result['total_loaded']:,} records")
        
        if result['period_results']:
            logging.info("--- Per periode ---")
            for period_info in result['period_results']:
                period = period_info['period']
                period_result = period_info['result']
                logging.info(f"  {period}: {period_result.get('total_loaded', 0):,} rijen geladen")
        
        if result.get('errors'):
            logging.error(f"{len(result['errors'])} periode errors:")
            for error in result['errors']:
                logging.error(f"{error['period']}: {error['error']}")
        return
    
    # Controleer of dit een lessen route is (alleen lessen)
    elif 'period_results' in result:
        logging.info(f"Totaal periodes voor lessen: {result['total_periods']}")
        logging.info(f"Periodes succesvol verwerkt: {result['periods_processed']}")
        logging.info(f"Totaal aantal rijen geladen: {result['total_loaded']:,}")
        
        if result['period_results']:
            for period_info in result['period_results']:
                period = period_info['period']
                period_result = period_info['result']
                logging.info(f"Aantal rijen geladen: {period_result.get('total_loaded', 0):,}")
                logging.info(f"Duur: {period_result.get('duration', 0):.1f}s")
                
                if period_result['status'] == 'error':
                    logging.error(f"Fout: {period_result.get('error', 'Onbekende fout')}")
        
        if result['errors']:
            logging.error(f"{len(result['errors'])} periode errors:")
            for error in result['errors']:
                logging.error(f"{error['period']}: {error['error']}")
    
    else:
        # Normaal pipeline resultaat
        if 'duration' in result:
            logging.info(f"Duur: {result['duration']:.1f} seconden")
        if 'execution_id' in result:
            logging.info(f"Uitvoering ID: {result['execution_id']}")
        
        if result.get('table_results'):
            total_extracted = 0
            total_transformed = 0
            total_loaded = 0
            
            for table_name, table_result in result['table_results'].items():
                
                total_extracted += table_result['extracted']
                total_transformed += table_result['transformed']
                total_loaded += table_result['loaded']
            
            logging.info(f"Totaal geëxtraheerd: {total_extracted:,} records")
            logging.info(f"Totaal getransformeerd: {total_transformed:,} records")
            logging.info(f"Totaal geladen: {total_loaded:,} records")
        
        if result.get('errors'):
            logging.error(f"{len(result['errors'])} errors:")
            for error in result['errors']:
                if 'table' in error:
                    logging.error(f"{error['table']}: {error['error']}")
                else:
                    logging.error(f"Algemene fout: {error.get('general', error)}")

def main():
    """Main entry point"""
    args = parse_arguments()
    
    # Logging setup
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_simple_logging(log_level)
    
    # Variabelen inladen
    load_dotenv()
    
    try:
        # Configuratie directory valideren
        config_dir = Path(args.config_dir)
        if not config_dir.exists():
            logging.error(f"Configuratie directory niet gevonden: {config_dir}")
            sys.exit(1)
        
        # Historische data argumenten valideren
        if args.historical:
            if not args.start_date or not args.end_date:
                logging.error("Historische data vereist zowel --start-date als --end-date")
                sys.exit(1)
            
            # Datum formaat valideren
            try:
                start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
                end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
                
                if start_date > end_date:
                    logging.error("Startdatum moet voor einddatum liggen")
                    sys.exit(1)
                    
                if args.historical:
                    logging.info(f"Historische data modus: {args.start_date} tot {args.end_date}")
                
            except ValueError as e:
                logging.error(f"Ongeldig datum formaat. Gebruik YYYY-MM-DD: {e}")
                sys.exit(1)
        
        # Specifieke tabellen parseren; --tables heeft voorrang, ook in historische modus
        specific_tables = None
        if args.tables:
            specific_tables = [table.strip() for table in args.tables.split(',')]
            logging.info(f"Specifieke tabellen aangevraagd: {specific_tables}")
        elif args.historical:
            # Gebruik default historische tabellen als er geen --tables is opgegeven
                                # AbonnementStatistieken is uit de standaard pipeline gehaald, maar kan handmatig worden opgehaald
            specific_tables = [table.strip() for table in args.historical_tables.split(',')]
            
            # ProductVerkopen wordt standaard meegenomen in de dagelijkse pipeline
            if 'ProductVerkopen' not in specific_tables:
                specific_tables.append('ProductVerkopen')
                logging.info("ProductVerkopen toegevoegd aan standaard pipeline (afgelopen week data)")
            logging.info(f"Historische tabellen: {specific_tables}")
        
        # Pipeline runner initialiseren
        logging.info(f"Pipeline runner initialiseren met configuratie: {config_dir}")
        runner = PipelineRunner(str(config_dir))
        
        # Toon dependencies als ze worden aangevraagd
        if args.show_dependencies:
            show_table_dependencies(runner, specific_tables)
            sys.exit(0)
        
        # Health check modus
        if args.health_check_only:
            logging.info("Uitvoeren van health checks...")
            health_ok = runner.run_health_checks()
            if health_ok:
                logging.info("Alle health checks succesvol")
                sys.exit(0)
            else:
                logging.error("Health checks mislukt")
                sys.exit(1)
        
        # Toon uitvoermodus
        if args.historical:
            logging.info(f"Uitvoeren in HISTORICAL modus - data van {args.start_date} tot {args.end_date}")
        elif args.dry_run:
                logging.info("Uitvoeren in DRY-RUN modus - geen database wijzigingen")
        else:
            logging.info("Uitvoeren in PRODUCTION modus - database wordt bijgewerkt")
        
        # Special logging for ActieveAbonnementen (only in non-historical mode)
        if not args.historical and specific_tables and 'ActieveAbonnementen' in specific_tables:
            if 'Leden' not in specific_tables:
                logging.warning("ActieveAbonnementen requires Leden data - adding Leden to processing")
                specific_tables.append('Leden')
        
        # Special logging for Uitbetalingen (only in non-historical mode)
        if not args.historical and specific_tables and 'Uitbetalingen' in specific_tables:
            logging.info("Uitbetalingen wordt handmatig opgehaald (alle data wordt opgehaald)")
        
        # Uitvoeren van de pipeline met geavanceerde dependency management
        start_time = datetime.now()
        
        # Voorbereiden van de pipeline parameters
        pipeline_params = {
            'tables': specific_tables,
            'dry_run': args.dry_run,
            'skip_health_checks': args.skip_health_checks
        }
        
        # Voeg historische data parameters toe als in historische modus
        if args.historical:
            pipeline_params['historical'] = True
            pipeline_params['start_date'] = args.start_date
            pipeline_params['end_date'] = args.end_date
            
            # Uniforme behandeling voor alle tabellen in historical mode
            # Bereken totale periode
            start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')
            total_days = (end_dt - start_dt).days
            
            # Weekly modus heeft voorrang; anders maandelijks bij >31 dagen
            if args.weekly:
                logging.info(f"Wekelijkse modus ingeschakeld - verwerk alle tabellen per week: {specific_tables}")
                result = run_weekly_pipeline(
                    runner=runner,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    tables=specific_tables,
                    dry_run=args.dry_run,
                    skip_health_checks=args.skip_health_checks,
                    verbose=args.verbose
                )
            elif total_days > 31:
                # Optie: splitsing per maand uitzetten
                if args.disable_monthly_split:
                    logging.info(f"Periode > 31 dagen ({total_days} dagen) - maandelijkse splitsing UITGESCHAKELD voor alle tabellen")
                    result = runner.run_pipeline(**pipeline_params)
                else:
                    # Splits tabellen in: maandelijks vs niet-maandelijks (per volledige periode)
                    excluded = [t.strip() for t in (args.no_monthly_split_tables or '').split(',') if t.strip()]
                    monthly_tables = [t for t in specific_tables if t not in excluded]
                    direct_tables = [t for t in specific_tables if t in excluded]

                    monthly_result = None
                    direct_result = None

                    if monthly_tables:
                        logging.info(f"Periode > 31 dagen ({total_days} dagen) - verwerk per maand: {monthly_tables}")
                        monthly_result = run_monthly_pipeline(
                            runner=runner,
                            start_date=args.start_date,
                            end_date=args.end_date,
                            tables=monthly_tables,
                            dry_run=args.dry_run,
                            skip_health_checks=args.skip_health_checks,
                            verbose=args.verbose
                        )
                    
                    if direct_tables:
                        logging.info(f"Verwerk zónder maand-splitsing (volledige periode): {direct_tables}")
                        direct_params = dict(pipeline_params)
                        direct_params['tables'] = direct_tables
                        direct_result = runner.run_pipeline(**{
                            **direct_params,
                            'historical': True,
                            'start_date': args.start_date,
                            'end_date': args.end_date
                        })

                    # Toon samenvattingen afzonderlijk en bepaal gecombineerde status
                    if monthly_result and direct_result:
                        print_execution_summary(monthly_result)
                        print_execution_summary(direct_result)
                        # Combineer status voor exit-code
                        statuses = {monthly_result.get('status'), direct_result.get('status')}
                        if 'error' in statuses:
                            result = {'status': 'error'}
                        elif 'partial_success' in statuses:
                            result = {'status': 'partial_success'}
                        else:
                            result = {'status': 'success'}
                    elif monthly_result:
                        result = monthly_result
                    elif direct_result:
                        result = direct_result
                    else:
                        # Geen tabellen?
                        logging.warning("Geen tabellen om te verwerken in historische modus")
                        result = {'status': 'success', 'tables_processed': 0}
            else:
                logging.info(f"Periode <= 31 dagen ({total_days} dagen) - direct verwerken")
                result = runner.run_pipeline(**pipeline_params)
        else:
            # Normale mode (niet historical)
            result = runner.run_pipeline(**pipeline_params)
        
        end_time = datetime.now()
        
        # Print uitgebreide samenvatting
        print_execution_summary(result)
        
        # Bepaal de exit code op basis van de resultaten
        if result['status'] == 'success':
            logging.info("Pipeline uitvoering succesvol")
            sys.exit(0)
        elif result['status'] == 'partial_success':
            logging.warning("Pipeline uitvoering met sommige errors")
            sys.exit(2) 
        else:
            logging.error("Pipeline uitvoering mislukt")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logging.warning("Pipeline uitvoering afgebroken door gebruiker")
        sys.exit(130)
    except FileNotFoundError as e:
        logging.error(f"Configuratiebestand niet gevonden: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Onverwachte fout: {str(e)}")
        if args.verbose:
            logging.error(f"Volledige traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()