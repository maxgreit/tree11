"""
Tree11 Data Pipeline - Pipeline Runner
Main orchestrator voor alle pipeline stappen met dependency management
"""

# Standaard imports
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from pathlib import Path
import logging
import time

# Interne imports
from data_transformer import DataTransformer
from database_manager import DatabaseManager
from data_extractor import DataExtractor
from logger import PerformanceLogger
from utils import load_config

class PipelineRunner:
    """
    Main orchestrator voor de Tree11 data pipeline
    Voert de volledige pipeline uit met dependency management, error recovery, en health checks
    """
    
    def __init__(self, config_dir: str):
        """
        Initialiseert de pipeline runner
        
        Args:
            config_dir: Directory containing configuration files
        """
        self.config_dir = Path(config_dir)
        
        # Laad configuraties
        self.api_config = load_config(self.config_dir / 'api_endpoints.json')
        self.schema_config = load_config(self.config_dir / 'schema_mappings.json')
        self.db_config_path = self.config_dir / 'database_config.json'
        self.pipeline_config = load_config(self.config_dir / 'pipeline_config.json')
        
        # Initialiseer componenten
        self.extractor = DataExtractor(str(self.config_dir / 'api_endpoints.json'))
        self.transformer = DataTransformer(str(self.config_dir / 'schema_mappings.json'))
        self.db_manager = DatabaseManager(str(self.db_config_path))
        
        # Prestatietracking
        self.perf_logger = PerformanceLogger()

        # Uitvoeringsstatus
        self.execution_id = None
        self.results = {}
        
        logging.debug("Pipeline runner geïnitialiseerd")
    
    def get_table_dependencies(self) -> Dict[str, List[str]]:
        """
        Definieert de tabelafhankelijkheden voor de juiste verwerkingsvolgorde
        
        Returns:
            Dictionary met tabelnamen als sleutels en hun afhankelijkheden als waarden
        """
        return {
            'GrootboekRekening': [],
            'Leden': [],
            'Abonnementen': [],
            'ActieveAbonnementen': ['Leden'],
            'Omzet': ['GrootboekRekening'],
            'Lessen': [],
            'LesDeelname': ['Lessen'], # LesDeelname depends on Lessen
            'AbonnementStatistieken': [], # Uit standaard pipeline gehaald, maar logica blijft behouden
            'AbonnementStatistiekenSpecifiek': ['Abonnementen'],
            'Facturen': [],
            'PersonalTraining': [],
            'Uitbetalingen': []
        }
    
    def get_processing_order(self, requested_tables: Optional[List[str]] = None) -> List[str]:
        """
        Haalt de juiste verwerkingsvolgorde op op basis van de afhankelijkheden
        
        Args:
            requested_tables: Optional list of specific tables to process
            
        Returns:
            Lijst met tabelnamen in de juiste verwerkingsvolgorde
        """
        dependencies = self.get_table_dependencies()
        
        if requested_tables:
            # Filter alleen de gevraagde tabellen
            available_tables = set(requested_tables)
        else:
            # Alle beschikbare tabellen
            available_tables = set(dependencies.keys())
        
        # Topologisch sorteren voor de afhankelijkheidsvolgorde
        processed = set()
        result = []
        
        def process_table(table_name: str):
            if table_name in processed or table_name not in available_tables:
                return
            
            # Verwerk eerst de afhankelijkheden
            for dependency in dependencies.get(table_name, []):
                if dependency in available_tables:
                    process_table(dependency)
            
            result.append(table_name)
            processed.add(table_name)
        
        # Verwerk alle tabellen
        for table in available_tables:
            process_table(table)
        
        return result
    
    def extract_table_data(self, table_name: str, historical: bool = False, start_date: str = None, end_date: str = None) -> List[Dict]:
        """
        Haalt de onbewerkte gegevens op voor een specifieke tabel
        
        Args:
            table_name: Naam van de tabel waarvan de gegevens worden geëxtraheerd
            historical: Als True, haalt historische gegevens op in plaats van dagelijkse gegevens
            start_date: Startdatum voor historische gegevens (YYYY-MM-DD formaat)
            end_date: Einddatum voor historische gegevens (YYYY-MM-DD formaat)
            
        Returns:
            Lijst met onbewerkte gegevensrecords
        """
        logging.info(f"Data extraheren voor tabel: {table_name}, historisch: {historical}")
        
        if historical:
            logging.info(f"Historische data extraheren: {start_date} tot {end_date}")
        
        try:
            # Speciale behandeling voor LesDeelname - vereist course IDs van Lessen
            if table_name == 'LesDeelname':
                return self._extract_les_deelname_data(historical, start_date, end_date)
            
            # Speciale behandeling voor AbonnementStatistiekenSpecifiek - per AbonnementId
            if table_name == 'AbonnementStatistiekenSpecifiek':
                return self._extract_abonnement_statistieken_specifiek(historical, start_date, end_date)
            
            # Speciale behandeling voor ProductVerkopen - vereist speciale extractie per dag
            if table_name == 'ProductVerkopen':
                return self._extract_product_verkopen_data(historical, start_date, end_date)
            
            # AbonnementStatistieken is uit de standaard pipeline gehaald, maar kan nog steeds handmatig worden opgehaald
            if table_name == 'AbonnementStatistieken':
                logging.info("AbonnementStatistieken wordt handmatig opgehaald (uit standaard pipeline gehaald)")
            
            endpoint_name = self._get_endpoint_name_for_table(table_name)
            
            if not endpoint_name:
                logging.warning(f"Geen endpoint gevonden voor tabel - tabel={table_name}")
                return []
            
            # Meerdere endpoints verwerken
            if isinstance(endpoint_name, list):
                all_data = []
                for endpoint in endpoint_name:
                    logging.info(f"Data extraheren van endpoint: {endpoint}, tabel: {table_name}")
                    
                    if historical:
                        # Haalt historische gegevens op voor deze endpoint
                        endpoint_data = self.extractor.extract_endpoint_data(
                            endpoint, 
                            historical=True,
                            start_date=start_date,
                            end_date=end_date
                        )
                    else:
                        # Controleer of de endpoint datumparameters vereist
                        endpoint_config = self.extractor.api_client.endpoints[endpoint]
                        if 'parameters' in endpoint_config and any('date' in param.lower() for param in endpoint_config['parameters']):
                            
                            # Haalt de datumbereik op voor deze endpoint
                            start_date, end_date = self.extractor.api_client.get_date_range_for_endpoint(endpoint)
                            endpoint_data = self.extractor.api_client.extract_endpoint_data(endpoint, start_date=start_date, end_date=end_date)
                        else:
                            # Geen datumparameters vereist
                            endpoint_data = self.extractor.extract_endpoint_data(endpoint)
                    
                    all_data.extend(endpoint_data)
                    logging.info(f"{len(endpoint_data)} rijen geëxtraheerd van endpoint: {endpoint}, tabel: {table_name}")
                
                logging.info(f"Totaal {len(all_data)} rijen geëxtraheerd voor tabel: {table_name}")
                return all_data
            else:
                if historical:
                    # Haalt historische gegevens op
                    data = self.extractor.extract_endpoint_data(
                        endpoint_name,
                        historical=True,
                        start_date=start_date,
                        end_date=end_date
                    )
                else:
                    # Controleer of de endpoint datumparameters vereist
                    endpoint_config = self.extractor.api_client.endpoints[endpoint_name]
                    if 'parameters' in endpoint_config and any('date' in param.lower() for param in endpoint_config['parameters']):
                        # Haalt de datumbereik op voor deze endpoint
                        start_date, end_date = self.extractor.api_client.get_date_range_for_endpoint(endpoint_name)
                        data = self.extractor.api_client.extract_endpoint_data(endpoint_name, start_date=start_date, end_date=end_date)
                    else:
                        # Geen datumparameters vereist
                        data = self.extractor.extract_endpoint_data(endpoint_name)
                
                logging.info(f"{len(data)} rijen geëxtraheerd voor tabel: {table_name}")
                return data
                
        except Exception as e:
            logging.error(f"Fout bij het extraheren van gegevens voor tabel: {table_name}, fout: {str(e)}")
            raise
    
    def _extract_les_deelname_data(self, historical: bool = False, start_date: str = None, end_date: str = None) -> List[Dict]:
        """
        Speciale extractie voor LesDeelname - haalt eerst Lessen op en dan voor elke les de members
        
        Args:
            historical: Of het historische data is
            start_date: Startdatum voor historische data
            end_date: Einddatum voor historische data
            
        Returns:
            Lijst met alle lesdeelname records
        """
        logging.info("Speciale extractie voor LesDeelname - haalt course IDs op van Lessen")
        
        try:
            # Eerst Lessen ophalen om course IDs te krijgen
            if historical:
                lessen_data = self.extract_table_data('Lessen', historical=True, start_date=start_date, end_date=end_date)
            else:
                # Voor dagelijkse extractie: afgelopen week (inclusief morgen)
                from datetime import datetime, timedelta
                today = datetime.now().date()
                tomorrow = today + timedelta(days=1)
                week_ago = today - timedelta(days=7)
                start_date = week_ago.strftime('%Y-%m-%d')
                end_date = tomorrow.strftime('%Y-%m-%d')
                
                logging.info(f"LesDeelname dagelijkse extractie: {start_date} tot {end_date}")
                lessen_data = self.extract_table_data('Lessen', historical=True, start_date=start_date, end_date=end_date)
            
            if not lessen_data:
                logging.warning("Geen lessen gevonden voor LesDeelname extractie")
                return []
            
            # Filter lessen die vóór morgen zijn (inclusief vandaag)
            from datetime import datetime
            today = datetime.now().date()
            past_lessons = []
            
            for les in lessen_data:
                try:
                    # Parse starttijd van de les
                    start_tijd_str = les.get('StartTijd') or les.get('startAt')
                    if start_tijd_str:
                        if isinstance(start_tijd_str, str):
                            # Parse ISO datetime string
                            start_tijd = datetime.fromisoformat(start_tijd_str.replace('Z', '+00:00'))
                        else:
                            start_tijd = start_tijd_str
                        
                        # Check of les vóór morgen is (inclusief vandaag)
                        if start_tijd.date() <= today:
                            past_lessons.append(les)
                except Exception as e:
                    logging.warning(f"Fout bij parsen van les datum: {e}")
                    continue
            
            logging.info(f"Gevonden {len(past_lessons)} lessen in het verleden voor LesDeelname extractie")
            
            # Voor elke les in het verleden, haal de members op
            all_les_deelname = []
            
            for les in past_lessons:
                course_id = les.get('Id') or les.get('id')
                if not course_id:
                    logging.warning(f"Geen course ID gevonden voor les: {les}")
                    continue
                
                try:
                    logging.info(f"Haalt members op voor course: {course_id}")
                    
                    # Haal members op voor deze course
                    members_data = self.extractor.api_client.extract_endpoint_data(
                        'courses_members', 
                        course_id=course_id
                    )
                    
                    # Voeg course_id toe aan elke member record
                    for member in members_data:
                        member['course_id'] = course_id
                    
                    all_les_deelname.extend(members_data)
                    logging.info(f"{len(members_data)} members gevonden voor course {course_id}")
                    
                except Exception as e:
                    logging.error(f"Fout bij ophalen members voor course {course_id}: {e}")
                    continue
            
            logging.info(f"Totaal {len(all_les_deelname)} lesdeelname records geëxtraheerd")
            return all_les_deelname
            
        except Exception as e:
            logging.error(f"Fout bij LesDeelname extractie: {e}")
            raise

    def _extract_product_verkopen_data(self, historical: bool = False, start_date: str = None, end_date: str = None) -> List[Dict]:
        """
        Speciale extractie voor ProductVerkopen - haalt POS statistieken data op per dag
        
        Args:
            historical: Als True, gebruikt de opgegeven date range; anders laatste 7 dagen
            start_date: Startdatum in YYYY-MM-DD formaat (alleen bij historical=True)
            end_date: Einddatum in YYYY-MM-DD formaat (alleen bij historical=True)
            
        Returns:
            Lijst met alle product verkopen records
        """
        logging.info("Speciale extractie voor ProductVerkopen - haalt POS statistieken data op per dag")
        
        try:
            from datetime import datetime, timedelta, date
            
            # Bepaal de date range
            if historical and start_date and end_date:
                # Parse de string dates naar date objecten
                if isinstance(start_date, str):
                    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
                else:
                    start_date_obj = start_date
                    
                if isinstance(end_date, str):
                    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                else:
                    end_date_obj = end_date
                    
                logging.info(f"ProductVerkopen extractie voor historische periode: {start_date_obj} tot {end_date_obj}")
            else:
                # Gebruik standaard periode van afgelopen 7 dagen
                today = datetime.now().date()
                start_date_obj = today - timedelta(days=7)
                end_date_obj = today
                logging.info(f"ProductVerkopen extractie voor afgelopen 7 dagen: {start_date_obj} tot {end_date_obj}")
            
            # Verzamel alle data per dag
            all_products_data = []
            current_date = start_date_obj
            
            # Gebruik de pos_statistics endpoint
            endpoint_name = 'pos_statistics'
            
            # Haal endpoint configuratie op
            endpoint_config = self.extractor.api_client.endpoints[endpoint_name]
            
            # Loop door elke dag in de range
            while current_date <= end_date_obj:
                logging.info(f"ProductVerkopen data ophalen voor datum: {current_date}")
                
                # Voor de API: startDate = gewenste dag, endDate = dag erna
                api_end_date = current_date + timedelta(days=1)
                logging.debug(f"API call: start_date={current_date.isoformat()}, end_date={api_end_date.isoformat()}")
                
                # Bouw URL op voor deze specifieke dag
                url = self.extractor.api_client._build_url(
                    endpoint_name, 
                    start_date=current_date.isoformat(), 
                    end_date=api_end_date.isoformat(), 
                    location_id=self.extractor.api_client.base_config['location_id']
                )
                
                # Haal data op voor deze dag
                response = self.extractor.api_client._make_request(url)
                
                # Debug logging om te zien wat er wordt opgehaald
                logging.debug(f"API response keys voor {current_date}: {list(response.json().keys()) if response else 'Geen response'}")
                
                if response and 'salesPerProduct' in response.json():
                    response_data = response.json()
                    logging.debug(f"salesPerProduct array lengte voor {current_date}: {len(response_data['salesPerProduct'])}")
                    if response_data['salesPerProduct']:
                        logging.debug(f"Eerste product in array voor {current_date}: {response_data['salesPerProduct'][0]}")
                    
                    # Transformeer de data naar het juiste formaat voor de schema mappings
                    day_products_data = []
                    for i, product in enumerate(response_data['salesPerProduct']):
                        if product['sales'] > 0:  # Alleen producten met verkopen
                            # Maak een record dat overeenkomt met de schema mapping structuur
                            # De schema mappings verwachten: Product, ProductID, Aantal, Datum
                            # ProductVerkopenID wordt handmatig gegenereerd als unieke identifier
                            product_record = {
                                'ProductVerkopenID': f"PV_{current_date.strftime('%Y%m%d')}_{i:03d}",
                                'Product': product['name'],
                                'ProductID': product['id'],
                                'Aantal': product['sales'],
                                'Datum': current_date.isoformat()
                            }
                            day_products_data.append(product_record)
                            
                            # Debug logging om te zien wat er wordt gemaakt
                            logging.debug(f"Product record gemaakt voor {current_date}: {product_record}")
                    
                    # Voeg de data van deze dag toe aan de totale lijst
                    all_products_data.extend(day_products_data)
                    logging.info(f"ProductVerkopen data voor {current_date}: {len(day_products_data)} producten gevonden")
                    
                else:
                    logging.warning(f"Geen salesPerProduct data gevonden voor {current_date}")
                
                # Ga naar de volgende dag
                current_date += timedelta(days=1)
            
            logging.info(f"ProductVerkopen extractie voltooid: {len(all_products_data)} producten gevonden voor periode {start_date_obj} tot {end_date_obj}")
            if all_products_data:
                logging.debug(f"Eerste product record structuur: {all_products_data[0]}")
                logging.debug(f"Totaal aantal product records: {len(all_products_data)}")
            return all_products_data
            
        except Exception as e:
            logging.error(f"Fout bij ProductVerkopen extractie: {str(e)}")
            return []
    
    def _extract_abonnement_statistieken_specifiek(self, historical: bool = False, start_date: str = None, end_date: str = None) -> List[Dict]:
        """
        Haalt per-abonnement analytics statistieken op voor meerdere categorieën en betalingsvormen.
        Gebruikt parallel processing voor betere performance.
        """
        logging.info("Speciale extractie voor AbonnementStatistiekenSpecifiek - parallel processing")

        try:
            # Haal lijst van abonnementen op (uit DB of via API). Hier via API om consistent te zijn.
            # Cache de abonnementen lijst om herhaalde API calls te voorkomen
            if not hasattr(self, '_cached_memberships') or self._cached_memberships is None:
                logging.info("Abonnementen ophalen voor caching...")
                self._cached_memberships = self.extract_table_data('Abonnementen', historical=False)
                logging.info(f"Abonnementen gecached: {len(self._cached_memberships)} abonnementen")
            
            memberships = self._cached_memberships
            if not memberships:
                logging.warning("Geen abonnementen gevonden voor specifieke analytics extractie")
                return []

            endpoint_name = self._get_endpoint_name_for_table('AbonnementStatistiekenSpecifiek')
            
            # Bepaal datumrange (eenmalig)
            if historical and start_date and end_date:
                sdt = datetime.strptime(start_date, '%Y-%m-%d').date()
                edt = datetime.strptime(end_date, '%Y-%m-%d').date()
            else:
                sdt, edt = self.extractor.api_client.get_date_range_for_endpoint(endpoint_name)

            logging.info(f"Parallel processing voor {len(memberships)} abonnementen - periode: {sdt} tot {edt}")

            # Parallel processing met ThreadPoolExecutor
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import threading
            
            all_records: List[Dict] = []
            max_workers = min(10, len(memberships))  # Max 10 parallel workers
            
            def process_membership(membership):
                """Process single membership - thread-safe"""
                membership_id = membership.get('AbonnementId') or membership.get('id') or membership.get('Id')
                if not membership_id:
                    return []
                
                try:
                    # Haal alle categorieën en payment types op voor dit membership
                    endpoint_data = self.extractor.api_client.extract_endpoint_data(
                        endpoint_name,
                        start_date=sdt,
                        end_date=edt,
                        membership_id=membership_id
                    )

                    # Voeg membership_id in elke record voor transformatie
                    for record in endpoint_data:
                        record['membership_id'] = membership_id
                        record['granularity'] = 'DAY'

                    logging.debug(f"Abonnement {membership_id} verwerkt - {len(endpoint_data)} records")
                    return endpoint_data
                    
                except Exception as e:
                    logging.warning(f"Fout bij verwerken abonnement {membership_id}: {e}")
                    return []

            # Parallel processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_membership = {
                    executor.submit(process_membership, membership): membership 
                    for membership in memberships
                }
                
                # Collect results as they complete
                completed = 0
                for future in as_completed(future_to_membership):
                    membership = future_to_membership[future]
                    try:
                        result = future.result()
                        all_records.extend(result)
                        completed += 1
                        
                        if completed % 20 == 0:  # Progress update every 20 abonnementen
                            logging.info(f"Verwerkt: {completed}/{len(memberships)} abonnementen")
                            
                    except Exception as e:
                        membership_id = membership.get('AbonnementId', 'unknown')
                        logging.error(f"Fout bij toekomst voor abonnement {membership_id}: {e}")

            logging.info(f"Parallel processing voltooid - {len(all_records)} records opgehaald voor {len(memberships)} abonnementen")
            return all_records

        except Exception as e:
            logging.error(f"Fout bij specifieke abonnement statistieken extractie: {e}")
            raise
    
    def _get_endpoint_name_for_table(self, table_name: str) -> Optional[Union[str, List[str]]]:
        """Haalt de API endpoint naam op voor een specifieke tabel"""
        endpoint_mappings = {
            'Leden': 'users',
            'Abonnementen': 'memberships',
            'Lessen': 'activity_events',
            'LesDeelname': 'courses_members',
            'Facturen': 'invoices',
            'Omzet': 'analytics_revenue',
            'GrootboekRekening': 'analytics_revenue',
            'AbonnementStatistieken': ['analytics_memberships_new', 'analytics_memberships_paused', 
                                     'analytics_memberships_active', 'analytics_memberships_expired'],
            'AbonnementStatistiekenSpecifiek': 'analytics_memberships_specific',
            'Uitbetalingen': 'payouts',
            'ProductVerkopen': 'pos_statistics',
        }
        return endpoint_mappings.get(table_name)
    
    def transform_table_data(self, table_name: str, raw_data: List[Dict]) -> List[Dict]:
        """
        Transformeert de gegevens voor een specifieke tabel
        
        Args:
            table_name: Naam van de tabel
            raw_data: Onbewerkte gegevens van de extractie
            
        Returns:
            Lijst met getransformeerde records
        """
        self.perf_logger.start_timer(f"transform_{table_name}")
        
        try:
            if table_name == 'ActieveAbonnementen':
                # Haalt Leden gegevens op van de vorige verwerking
                if 'Leden' in self.results and self.results['Leden'].get('raw_data'):
                    leden_data = self.results['Leden']['raw_data']
                    transformed_data = self.transformer.extract_active_memberships(leden_data)
                    logging.debug(f"ActieveAbonnementen afgeleid van Leden gegevens - Leden: {len(leden_data)} rijen, ActieveAbonnementen: {len(transformed_data)} rijen")
                else:
                    logging.warning("Geen Leden gegevens beschikbaar voor ActieveAbonnementen afleiding")
                    transformed_data = []
            
            elif table_name in ['Omzet', 'GrootboekRekening']:
                if table_name == 'Omzet':
                    omzet_records, _ = self.transformer.transform_revenue_data(raw_data)
                    transformed_data = omzet_records
                else:
                    _, grootboek_records = self.transformer.transform_revenue_data(raw_data)
                    transformed_data = grootboek_records
            
            elif table_name == 'Uitbetalingen':
                transformed_data = self.transformer.transform_payouts_data(raw_data)
            
            elif table_name == 'LesDeelname':
                # Gebruik de normale transformer voor LesDeelname
                transformed_data = self.transformer.transform_table_data(table_name, raw_data)
            
            elif table_name == 'AbonnementStatistieken':
                transformed_data = []
                endpoint_name = self._get_endpoint_name_for_table(table_name)
                
                if isinstance(endpoint_name, list):
                    for endpoint in endpoint_name:
                        try:
                            if 'new' in endpoint:
                                category = 'Nieuw'
                            elif 'paused' in endpoint:
                                category = 'Gepauzeerd'
                            elif 'active' in endpoint:
                                category = 'Actief'
                            elif 'expired' in endpoint:
                                category = 'Verlopen'
                            else:
                                category = 'Onbekend'
                            
                            # Filtert de gegevens voor deze specifieke endpoint
                            endpoint_data = [r for r in raw_data if r.get('endpoint_type') == endpoint]
                            
                            # Haalt de betalingssoort op van de gefilterde endpoint gegevens
                            payment_type = None
                            if endpoint_data:
                                # Haalt de betalingssoort op van de eerste record die het heeft
                                for record in endpoint_data:
                                    if record.get('payment_type_filter'):
                                        payment_type = record.get('payment_type_filter')
                                        break
                                
                                # Logt de betalingssoort voor debugging
                                logging.debug(f"Betalingssoort gevonden voor {endpoint}: {payment_type}")
                            
                            if endpoint_data:
                                analytics_records = self.transformer.transform_analytics_data(
                                    endpoint, endpoint_data, category, payment_type
                                )
                                transformed_data.extend(analytics_records)
                                logging.debug(f"Getransformeerde statistieken gegevens voor {endpoint}, aantal rijen: {len(analytics_records)}")
                            
                        except Exception as e:
                            logging.warning(f"Fout bij het transformeren van analytics gegevens voor endpoint {endpoint} - fout: {str(e)}")
                
                logging.info(f"AbonnementStatistieken transformatie voltooid - totaal aantal rijen: {len(transformed_data)}")
            
            elif table_name == 'AbonnementStatistiekenSpecifiek':
                # Gebruik specifieke transformatie voor per-abonnement analytics
                transformed_data = self.transformer.transform_analytics_specific_data(raw_data)
                logging.info(f"AbonnementStatistiekenSpecifiek transformatie voltooid - totaal aantal rijen: {len(transformed_data)}")
            
            else:   
                # Standaard transformatie
                transformed_data = self.transformer.transform_table_data(table_name, raw_data)
            
            # Valideert de getransformeerde gegevens
            valid_data = self.transformer.validate_transformed_data(table_name, transformed_data)
            
            duration = self.perf_logger.end_timer(f"transform_{table_name}")
            logging.debug(f"Data transformatie voltooid - tabel: {table_name}, input: {len(raw_data)} rijen, output: {len(valid_data)} rijen, duur: {duration}")
            
            return valid_data
            
        except Exception as e:
            self.perf_logger.end_timer(f"transform_{table_name}")
            logging.error(f"Data transformatie mislukt - tabel: {table_name}, fout: {str(e)}")
            raise
    
    def load_table_data(self, table_name: str, data: List[Dict]) -> int:
        """
        Laadt gegevens in de database tabel
        
        Args:
            table_name: Naam van de tabel
            data: Getransformeerde gegevens om te laden
            
        Returns:
            Aantal geladen records
        """
        if not data:
            logging.info(f"Geen gegevens om te laden - tabel: {table_name}")
            return 0
        
        logging.info(f"Gegevens laden - tabel: {table_name}, aantal rijen: {len(data)}")
        self.perf_logger.start_timer(f"load_{table_name}")
        
        try:
            # Haalt de update strategie op van de configuratie
            table_config = self.schema_config['tables'].get(table_name, {})
            update_strategy = table_config.get('update_strategy', 'upsert')
            
            loaded_count = self.db_manager.load_table_data(table_name, data, update_strategy)
            
            duration = self.perf_logger.end_timer(f"load_{table_name}")
            logging.info(f"Data laden voltooid - tabel: {table_name}, geladen rijen: {loaded_count}, duur: {duration}")
            
            return loaded_count
            
        except Exception as e:
            self.perf_logger.end_timer(f"load_{table_name}")
            logging.error(f"Data laden mislukt - tabel: {table_name}, fout: {str(e)}")
            raise
    
    def run_health_checks(self) -> bool:
        """
        Uitvoert health checks voorafgaand aan de pipeline uitvoering
        
        Returns:
            True als alle checks succesvol zijn, False als er een fout is
        """
        logging.info("Health checks uitvoeren")
        
        try:
            # Database connectiviteit check
            test_result = self.db_manager.execute_query("SELECT 1 AS test")
            if not test_result or test_result[0]['test'] != 1:
                logging.error("Database health check mislukt")
                return False
            
            logging.info("✓ Database connectiviteit check succesvol")
            
            # API connectiviteit check (eenvoudige endpoint test)
            # Dit kan worden uitgebreid om specifieke endpoints te testen
            logging.info("✓ Health checks voltooid succesvol")
            return True
            
        except Exception as e:
            logging.error(f"Health checks mislukt - fout: {str(e)}")
            return False
    
    def process_table(self, table_name: str, dry_run: bool = False, historical: bool = False, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """
        Verwerkt een enkele tabel door de volledige ETL pipeline
        
        Args:
            table_name: Naam van de tabel om te verwerken
            dry_run: Als True, slaat de werkelijke gegevenslaadactie over
            historical: Als True, haalt historische gegevens op in plaats van dagelijkse gegevens
            start_date: Startdatum voor historische gegevens (YYYY-MM-DD formaat)
            end_date: Einddatum voor historische gegevens (YYYY-MM-DD formaat)
            
        Returns:
            Dictionary met verwerkingsresultaten
        """
        
        result = {
            'table': table_name,
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            'status': 'success',
            'error': None,
            'duration': 0
        }
        
        start_time = time.time()
        
        try:
            # Haalt de onbewerkte gegevens op
            raw_data = self.extract_table_data(table_name, historical=historical, start_date=start_date, end_date=end_date)
            result['extracted'] = len(raw_data)
            
            # Slaat de onbewerkte gegevens op
            if table_name not in self.results:
                self.results[table_name] = {}
            self.results[table_name]['raw_data'] = raw_data
            
            if table_name == 'ActieveAbonnementen' or raw_data:
                # Transformeren
                transformed_data = self.transform_table_data(table_name, raw_data)
                result['transformed'] = len(transformed_data)
                
                if transformed_data:
                    # Laden
                    if dry_run:
                        logging.info(f"DRY RUN: Slaat gegevenslaadactie over voor tabel: {table_name}, zou {len(transformed_data)} rijen laden")
                        result['loaded'] = 0
                    else:
                        loaded_count = self.load_table_data(table_name, transformed_data)
                        result['loaded'] = loaded_count
                else:
                    logging.warning(f"Geen geldige gegevens na transformatie - tabel: {table_name}")
            else:
                logging.warning(f"Geen gegevens geëxtraheerd, slaat transformatie en laden over - tabel: {table_name}")
            
            result['duration'] = time.time() - start_time
            logging.info(f"Tabel verwerken voltooid: {table_name}, {', '.join(f'{k}={v}' for k, v in result.items() if k != 'table')}")
            
            return result
            
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            result['duration'] = time.time() - start_time
            
            logging.error(f"Tabel verwerken mislukt: {table_name}, fout: {str(e)}, duur: {result['duration']}")
            
            return result
    
    def run_pipeline(self, tables: Optional[List[str]] = None, 
                    dry_run: bool = False, skip_health_checks: bool = False,
                    historical: bool = False, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """
        Voert de volledige pipeline uit
        
        Args:
            tables: Optionele lijst met specifieke tabellen om te verwerken
            dry_run: Als True, slaat de werkelijke gegevenslaadactie over
            skip_health_checks: Als True, slaat de database health checks over
            historical: Als True, haalt historische gegevens op in plaats van dagelijkse gegevens
            start_date: Startdatum voor historische gegevens (YYYY-MM-DD formaat)
            end_date: Einddatum voor historische gegevens (YYYY-MM-DD formaat)
            
        Returns:
            Dictionary met pipeline uitvoeringsresultaten
        """
        self.execution_id = f"tree11_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Voeg historische info toe aan de uitvoerings-ID als in historische modus
        if historical:
            self.execution_id = f"tree11_historical_{start_date}_to_{end_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logging.info(f"Pipeline uitvoering starten. Te verwerken tabellen: {tables}, dry_run: {dry_run}, historisch: {historical}")
        
        pipeline_start_time = time.time()
        
        # Initialiseert de resultatenopslag voor cross-table afhankelijkheden
        self.results = {}
        
        # Initialiseert de resultaten
        pipeline_result = {
            'execution_id': self.execution_id,
            'start_time': datetime.now(),
            'end_time': None,
            'duration': 0,
            'status': 'success',
            'tables_processed': 0,
            'total_extracted': 0,
            'total_transformed': 0,
            'total_loaded': 0,
            'table_results': {},
            'errors': [],
            'historical': historical,
            'start_date': start_date,
            'end_date': end_date
        }
        
        try:
            # Health checks uitvoeren
            if not skip_health_checks:
                if not self.run_health_checks():
                    raise Exception("Health checks mislukt")
            else:
                logging.debug("Health checks overslaan als gevraagd")
            
            # Haalt de verwerkingsvolgorde op
            processing_order = self.get_processing_order(tables)
            logging.debug(f"Verwerkingsvolgorde bepaald: {processing_order}")
            
            # Speciale notificatie over ActieveAbonnementen afhankelijkheid
            if not historical and 'ActieveAbonnementen' in processing_order:
                logging.debug("ActieveAbonnementen wordt afgeleid van Leden gegevens")
            
            # Verwerkt elke tabel
            for table_name in processing_order:
                logging.info(f"Tabel verwerken: {table_name}")
                
                # Geeft historische parameters door aan process_table functie
                table_result = self.process_table(
                    table_name, 
                    dry_run=dry_run,
                    historical=historical,
                    start_date=start_date,
                    end_date=end_date
                )
                pipeline_result['table_results'][table_name] = table_result
                
                # Update totaal aantal geëxtraheerde, getransformeerde en geladen rijen
                pipeline_result['total_extracted'] += table_result['extracted']
                pipeline_result['total_transformed'] += table_result['transformed']
                pipeline_result['total_loaded'] += table_result['loaded']
                
                if table_result['status'] == 'error':
                    pipeline_result['errors'].append({
                        'table': table_name,
                        'error': table_result['error']
                    })
                else:
                    pipeline_result['tables_processed'] += 1
            
            # Finaliseert de resultaten
            pipeline_result['end_time'] = datetime.now()
            pipeline_result['duration'] = time.time() - pipeline_start_time
            
            if pipeline_result['errors']:
                pipeline_result['status'] = 'partial_success'
                logging.warning(f"Pipeline voltooid met fouten - uitvoerings-ID: {self.execution_id}, aantal fouten: {len(pipeline_result['errors'])}")
            else:
                logging.info(f"Pipeline succesvol uitgevoerd - uitvoerings-ID: {self.execution_id}, duur: {pipeline_result['duration']:.2f}s")
            
            return pipeline_result
            
        except Exception as e:
            pipeline_result['status'] = 'error'
            pipeline_result['end_time'] = datetime.now()
            pipeline_result['duration'] = time.time() - pipeline_start_time
            pipeline_result['errors'].append({
                'general': str(e)
            })
            
            logging.error(f"Pipeline uitvoering mislukt - uitvoerings-ID: {self.execution_id}, fout: {str(e)}")
            
            # Logt de fout naar de database
            if not dry_run:
                self.db_manager.log_pipeline_error(self.execution_id, str(e))
            
            return pipeline_result
        
        finally:
            # Logt prestatiemetrieken
            self.perf_logger.log_metrics()
            
            # Sluit database verbindingen
            if hasattr(self, 'db_manager'):
                self.db_manager.close()