"""
Tree11 Data Pipeline - Data Transformer
Transforms raw API data to Dutch database schema with type conversion and validation
"""

# Standaard imports
from typing import Dict, List, Any, Optional
from pydantic import ValidationError
from datetime import datetime
from decimal import Decimal
import logging
import json

class TransformationError(Exception):
    """Custom exception voor transformatie fouten"""
    pass

class DataTransformer:
    """
    Transformatie van onbewerkte API gegevens naar Nederlandse database schema
    Handelt type conversie, veld toewijzing en gegevens validatie af
    """
    
    def __init__(self, schema_mappings_path: str):
        """
        Initialiseert de transformer met configuratie voor schema toewijzing
        
        Args:
            schema_mappings_path: Pad naar schema_mappings.json bestand
        """
        with open(schema_mappings_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        self.tables_config = self.config['tables']
        self.transformations = self.config['transformations']
        self.constants = self.config['constants']
        
        logging.debug(f"Data transformer geïnitialiseerd. Aantal te verwerken tabellen: {len(self.tables_config)}")
    
    def transform_table_data(self, table_name: str, raw_data: List[Dict]) -> List[Dict]:
        """
        Transformeer onbewerkte gegevens voor een specifieke tabel
        
        Args:
            table_name: Naam van de doeltabel
            raw_data: Lijst van onbewerkte records van de API
            
        Returns:
            Lijst van getransformeerde records klaar voor database invoer
        """
        if table_name not in self.tables_config:
            raise ValueError(f"Unknown table: {table_name}")
        
        table_config = self.tables_config[table_name]
        
        logging.info(f"Transformatie starten voor tabel: {table_name}, input: {len(raw_data)} rijen")
        
        transformed_data = []   
        errors = []
        
        for i, record in enumerate(raw_data):
            try:
                transformed_record = self._transform_record(record, table_config)
                if transformed_record:
                    transformed_data.append(transformed_record)
            except Exception as e:
                error_msg = f"Record {i}: {str(e)}"
                errors.append(error_msg)
                logging.warning(f"Transformatie mislukt voor tabel: {table_name}, record_index={i}, fout: {str(e)}")
        
        if errors:
            logging.warning(f"Transformatie voltooid met fouten voor tabel: {table_name}, output_records={len(transformed_data)}, error_count={len(errors)}")
        else:
            logging.info(f"Transformatie voltooid voor tabel: {table_name}, output_records={len(transformed_data)}")
        
        return transformed_data
    
    def _transform_record(self, raw_record: Dict, table_config: Dict) -> Optional[Dict]:
        """
        Transformeer een enkele record volgens de tabel configuratie
        
        Args:
            raw_record: Onbewerkte record van de API
            table_config: Tabel configuratie van de schema toewijzing
            
        Returns:
            Getransformeerde record of None als transformatie mislukt
        """
        transformed = {}
        columns_config = table_config['columns']
        
        # Verwerk gewone kolommen
        for source_field, field_config in columns_config.items():
            target_column = field_config['target_column']
            transformation = field_config['transformation']
            required = field_config.get('required', False)
            
            try:
                # Extracteer waarde uit bron
                raw_value = self._extract_field_value(raw_record, source_field)
                
                # Pas transformatie toe
                transformed_value = self._apply_transformation(
                    raw_value, transformation, field_config, raw_record
                )
                
                # Valideer vereiste kolommen
                if required and (transformed_value is None or transformed_value == ''):
                    if not field_config.get('allow_null', False):
                        raise TransformationError(
                            f"Required field {target_column} is null or empty"
                        )
                
                transformed[target_column] = transformed_value
                
            except Exception as e:
                logging.error(f"Transformatie mislukt - kolom: {source_field}, doelkolom: {target_column}, fout: {str(e)}")
                raise TransformationError(f"Kolom {source_field}: {str(e)}")
        
        # Verwerk aangepaste kolommen
        custom_fields = table_config.get('custom_fields', {})
        for field_name, field_config in custom_fields.items():
            try:
                custom_value = self._generate_custom_field(
                    field_name, field_config, raw_record, transformed
                )
                transformed[field_name] = custom_value
            except Exception as e:
                logging.warning(f"Aangepaste kolom generatie mislukt voor kolom: {field_name}, fout: {str(e)}")
        
        # Voeg metadata toe
        transformed['DatumLaatsteUpdate'] = datetime.now()
        
        return transformed
    
    def _extract_field_value(self, record: Dict, field_path: str) -> Any:
        """
        Extracteer waarde uit geneste record met puntnotatie
        
        Args:
            record: Bron record
            field_path: Kolompad (bijv., 'address.street', 'series[0].data[i]')
            
        Returns:
            Geëxtraheerde waarde of None
        """
        if field_path in record:
            return record[field_path]
        
        # Handel geneste kolommen met puntnotatie
        if '.' in field_path:
            parts = field_path.split('.')
            current = record
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return None
            return current
        
        # Handel array indexering (voor analytics gegevens)
        if '[' in field_path and ']' in field_path:
            # This is complex - for now return None
            # TODO: Implement array indexing for analytics data
            return None
        
        return None
    
    def _apply_transformation(self, value: Any, transformation: str, 
                            field_config: Dict, raw_record: Dict) -> Any:
        """
        Pas de opgegeven transformatie toe op een waarde
        
        Args:
            value: Onbewerkte waarde om te transformeren
            transformation: Type van transformatie
            field_config: Veld configuratie
            raw_record: Originele onbewerkte record voor context
            
        Returns:
            Getransformeerde waarde
        """
        try:
            if transformation == 'direct':
                return str(value) if value is not None else ''
            
            elif transformation == 'json_dump':
                return json.dumps(value, ensure_ascii=False) if value else '[]'
            
            elif transformation == 'boolean':
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes', 'on')
                return bool(value) if value is not None else False
            
            elif transformation == 'integer':
                if value == '' or value is None:
                    return 0
                return int(float(value))
            
            elif transformation == 'decimal':
                if value == '' or value is None:
                    return Decimal('0.00')
                return Decimal(str(value))
            
            elif transformation == 'iso_datetime':
                if not value:
                    return None
                if isinstance(value, str):
                    if value.endswith('Z'):
                        value = value.replace('Z', '+00:00')
                    return datetime.fromisoformat(value)
                return value
            
            elif transformation == 'parse_date':
                if not value:
                    return None
                if isinstance(value, str):
                    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y'):
                        try:
                            return datetime.strptime(value, fmt).date()
                        except ValueError:
                            continue
                    raise ValueError(f"Unable to parse date: {value}")
                elif isinstance(value, datetime):
                    return value.date()
                return value
            
            elif transformation == 'nested_field':
                return self._extract_field_value(raw_record, field_config.get('source_path', ''))
            
            elif transformation == 'null_to_empty':
                return str(value) if value is not None else ''
            
            elif transformation == 'extract_from_url':
                # Gebruik contextuele velden uit de record om een categorie/type te bepalen
                # Voorkeur: endpoint_category (indien door extractor gezet), anders variant 'category',
                # anders endpoint_type of de ruwe waarde zelf
                mapping = field_config.get('mapping', {})
                candidate = (
                    raw_record.get('endpoint_category')
                    or raw_record.get('category')
                    or raw_record.get('endpoint_type')
                    or value
                )
                return mapping.get(candidate, candidate)
            
            elif transformation == 'google_sheet_lookup':
                # TODO: Implement Google Sheet lookup for sectie
                return ''
            
            else:
                logging.warning(f"Onbekende transformatie - transformation={transformation}")
                return value
                
        except Exception as e:
            logging.error(f"Transformatie mislukt voor transformatie: {transformation}, waarde: {value}, fout: {str(e)}")
            raise TransformationError(f"Transformatie {transformation} mislukt: {str(e)}")
    
    def _generate_custom_field(self, field_name: str, field_config: Dict, 
                             raw_record: Dict, transformed_record: Dict) -> Any:
        """
        Genereer waarde voor aangepaste velden die niet direct van de API komen
        
        Args:
            field_name: Naam van het aangepaste veld
            field_config: Configuratie voor het aangepaste veld
            raw_record: Originele onbewerkte record
            transformed_record: Gedeeltelijk getransformeerde record
            
        Returns:
            Geëxtraheerde waarde voor het aangepaste veld
        """
        transformation = field_config['transformation']
        
        if transformation == 'google_sheet_lookup':
            # TODO: Implement actual Google Sheet lookup
            # For now, return empty string
            return ''
        
        elif transformation == 'course_id_from_context':
            # Haal course_id uit de raw_record (toegevoegd door data extractor)
            course_id = raw_record.get('course_id')
            if course_id:
                return str(course_id)
            else:
                logging.warning(f"Geen course_id gevonden in record voor LesDeelname: {raw_record}")
                return None
        
        elif transformation == 'membership_id_from_context':
            # Haal membership_id uit de raw_record context (toegevoegd bij specifieke analytics calls)
            membership_id = raw_record.get('membership_id')
            if membership_id:
                return str(membership_id)
            else:
                logging.warning(f"Geen membership_id gevonden in record voor AbonnementStatistiekenSpecifiek: {raw_record}")
                return None
        
        # Voeg meer aangepaste veld generatoren toe indien nodig
        logging.warning(f"Onbekende aangepaste veld transformatie: {transformation}")
        return None
    
    def transform_analytics_data(self, endpoint_name: str, raw_data: List[Dict], 
                               category: str, payment_type: str = None) -> List[Dict]:
        """
        Speciale transformatie voor analytics abonnement gegevens
        
        Args:
            endpoint_name: Naam van de analytics endpoint
            raw_data: Onbewerkte analytics gegevens
            category: Categorie (Nieuw, Gepauzeerd, etc.)
            payment_type: Betalings type (Eenmalig, Periodiek)
            
        Returns:
            Lijst van getransformeerde AbonnementStatistieken records
        """
        transformed_records = []
        
        # Groepeer gegevens op datum en betalings type voor combinatie van PERIODIC varianten
        date_payment_data = {}
        
        for raw_record in raw_data:
            labels = raw_record.get('labels', [])
            series = raw_record.get('series', [])
            
            if not series:
                continue
            
            data_points = series[0].get('data', [])
            
            # Haal betalings type uit de record zelf als het niet is opgegeven
            record_payment_type = raw_record.get('payment_type_filter', payment_type)
            
            for i, (label, value) in enumerate(zip(labels, data_points)):
                try:
                    # Parseer datum uit label
                    record_date = datetime.strptime(label, '%Y-%m-%d').date()
                    
                    # Map betalings type naar Nederlandse waarden
                    payment_type_mapping = {
                        'ONCE': 'Eenmalig',
                        'PERIODIC': 'Periodiek',
                        'PERIODIC_CUSTOM': 'Periodiek_Aangepast',
                    }
                    
                    mapped_payment_type = payment_type_mapping.get(record_payment_type, 'Onbekend')
                    
                    # Maak sleutel voor groepering
                    key = (record_date, mapped_payment_type)
                    
                    if key not in date_payment_data:
                        date_payment_data[key] = {
                            'Datum': record_date,
                            'Categorie': category,
                            'Type': mapped_payment_type,
                            'Aantal': 0,
                            'DatumLaatsteUpdate': datetime.now()
                        }
                    
                    # Voeg waarde toe aan bestaande record (combineer PERIODIC varianten)
                    date_payment_data[key]['Aantal'] += int(value) if value is not None else 0
                    
                except Exception as e:
                    logging.warning(f"Fout bij transformeren van analytics record - label={label}, waarde={value}, fout: {str(e)}")
        
        # Converteer terug naar lijst en voeg weergave-datum toe (dd-mm-yyyy)
        transformed_records = list(date_payment_data.values())
        for rec in transformed_records:
            try:
                if isinstance(rec.get('Datum'), datetime):
                    rec['DatumWeergave'] = rec['Datum'].strftime('%d-%m-%Y')
                else:
                    # Datum is een date
                    rec['DatumWeergave'] = rec['Datum'].strftime('%d-%m-%Y') if rec.get('Datum') else None
            except Exception:
                rec['DatumWeergave'] = None
        
        return transformed_records

    def transform_analytics_specific_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transformatie voor per-abonnement analytics (AbonnementStatistiekenSpecifiek).
        Verwacht records met structuur vergelijkbaar met analytics endpoints:
        - 'labels': lijst van datums (YYYY-MM-DD)
        - 'series'[0]['data']: lijst van aantallen
        - 'category': new|paused|active|expirations
        - 'payment_type_filter': ONCE|PERIODIC|PERIODIC_CUSTOM
        - 'membership_id': AbonnementId
        """
        transformed_records: List[Dict] = []

        # Groepeer per (datum, type, categorie, abonnement)
        aggregated: Dict[tuple, Dict] = {}

        for rec in raw_data:
            try:
                labels = rec.get('labels', [])
                series_list = rec.get('series', [])
                if not series_list:
                    continue
                data_points = series_list[0].get('data', [])

                # Bepaal mappings
                cat_map = {
                    'new': 'Nieuw',
                    'paused': 'Gepauzeerd',
                    'active': 'Actief',
                    'expirations': 'Verlopen'
                }
                pay_map = {
                    'ONCE': 'Eenmalig',
                    'PERIODIC': 'Periodiek',
                    'PERIODIC_CUSTOM': 'Periodiek_Aangepast'
                }

                category_raw = rec.get('category') or rec.get('endpoint_category')
                category = cat_map.get(category_raw, 'Onbekend')
                ptype_raw = rec.get('payment_type_filter')
                mapped_type = pay_map.get(ptype_raw, 'Onbekend')
                abonnement_id = rec.get('membership_id')

                # Als granularity WEEK en labels afwijkend zijn, dan vallen sommige endpoints terug op weeknummers of lege labels.
                # Gebruik in dat geval start_date_context en tel per punt +i dagen op (best effort), of sla over als er niets is.
                start_ctx = rec.get('start_date_context')
                gran = rec.get('granularity')

                for i, (lbl, val) in enumerate(zip(labels, data_points)):
                    datum = None
                    if isinstance(lbl, str) and lbl:
                        try:
                            datum = datetime.strptime(lbl, '%Y-%m-%d').date()
                        except Exception:
                            datum = None
                    if datum is None and gran == 'WEEK' and start_ctx:
                        try:
                            base = datetime.fromisoformat(start_ctx).date()
                            datum = base  # representatieve start van de week
                        except Exception:
                            datum = None
                    if datum is None:
                        continue

                    key = (datum, category, mapped_type, abonnement_id)
                    if key not in aggregated:
                        aggregated[key] = {
                            'Datum': datum,
                            'Categorie': category,
                            'Type': mapped_type,
                            'AbonnementId': abonnement_id,
                            'Aantal': 0,
                            'DatumLaatsteUpdate': datetime.now()
                        }
                    aggregated[key]['Aantal'] += int(val) if val is not None else 0

            except Exception as e:
                logging.warning(f"Fout bij transformeren van specifieke analytics record: {e}")

        transformed_records = list(aggregated.values())
        # Voeg weergave-datum toe (dd-mm-yyyy)
        for rec in transformed_records:
            try:
                if isinstance(rec.get('Datum'), datetime):
                    rec['DatumWeergave'] = rec['Datum'].strftime('%d-%m-%Y')
                else:
                    rec['DatumWeergave'] = rec['Datum'].strftime('%d-%m-%Y') if rec.get('Datum') else None
            except Exception:
                rec['DatumWeergave'] = None
        return transformed_records
    
    def transform_revenue_data(self, raw_data: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """
        Transformeer omzet gegevens naar Omzet en GrootboekRekening records
        
        Args:
            raw_data: Onbewerkte omzet gegevens van de analytics API
            
        Returns:
            Tuple van (omzet_records, grootboek_rekening_records)
        """
        omzet_records = []
        grootboek_records = []
        
        for raw_record in raw_data:
            # Extracteer dagelijkse omzet gegevens
            daily_revenue = raw_record.get('dailyRevenue', [])
            for revenue_item in daily_revenue:
                try:
                    omzet_record = {
                        'Datum': datetime.strptime(revenue_item['date'], '%Y-%m-%d').date(),
                        'GrootboekRekeningId': revenue_item['ledgerAccountId'],
                        'Type': revenue_item['type'],
                        'Omzet': Decimal(str(revenue_item['revenue'])),
                        'DatumLaatsteUpdate': datetime.now()
                    }
                    omzet_records.append(omzet_record)
                except Exception as e:
                    logging.warning(f"Fout bij transformeren van omzet record - revenue_item={revenue_item}, fout: {str(e)}")
            
            # Extracteer grootboekrekeningen
            ledger_accounts = raw_record.get('ledgerAccounts', [])
            for account in ledger_accounts:
                try:
                    grootboek_record = {
                        'Id': account['id'],
                        'Sleutel': account.get('key', ''),
                        'Label': account.get('label') or '',
                        'DatumLaatsteUpdate': datetime.now()
                    }
                    grootboek_records.append(grootboek_record)
                except Exception as e:
                    logging.warning(f"Fout bij transformeren van grootboekrekening - account={account}, fout: {str(e)}")
        
        return omzet_records, grootboek_records
    
    def transform_payouts_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transformeer uitbetalingen gegevens naar Uitbetalingen records
        
        Args:
            raw_data: Onbewerkte uitbetalingen gegevens van de payouts API
            
        Returns:
            Lijst van getransformeerde uitbetalingen records
        """
        uitbetalingen_records = []
        
        for raw_record in raw_data:
            try:
                # Extracteer payout data
                payout = raw_record.get('payout', {})
                summary = payout.get('summary', {})
                
                uitbetalingen_record = {
                    'UitbetalingID': payout.get('id'),
                    'Datum': datetime.fromisoformat(payout.get('date', '').replace('Z', '+00:00')),
                    'Betalingen': summary.get('paymentCount', 0),
                    'Chargebacks': summary.get('chargebackCount', 0),
                    'Refunds': summary.get('refundCount', 0),
                    'NettoBedrag': Decimal(str(summary.get('totalNetAmount', 0))),
                    'BrutoBedrag': Decimal(str(summary.get('totalGrossAmount', 0))),
                    'ChargebackBedrag': Decimal(str(summary.get('totalChargebackAmount', 0))),
                    'RefundBedrag': Decimal(str(summary.get('totalRefundAmount', 0))),
                    'CommissieBedrag': Decimal(str(summary.get('totalCommissionFeeAmount', 0))),
                    'Status': payout.get('status', ''),
                    'DatumLaatsteUpdate': datetime.now()
                }
                
                uitbetalingen_records.append(uitbetalingen_record)
                
            except Exception as e:
                logging.warning(f"Fout bij transformeren van uitbetalingen record - raw_record={raw_record}, fout: {str(e)}")
                continue
        
        logging.info(f"Uitbetalingen transformatie voltooid: {len(uitbetalingen_records)} records getransformeerd")
        return uitbetalingen_records
    
    def transform_product_verkopen_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transformeer product verkopen gegevens naar ProductVerkopen records
        
        Args:
            raw_data: Onbewerkte product verkopen gegevens van de daily_revenue API
            
        Returns:
            Lijst van getransformeerde product verkopen records
        """
        product_verkopen_records = []
        
        for raw_record in raw_data:
            try:
                # Extracteer daily revenue data
                daily_revenue = raw_record.get('dailyRevenue', {})
                sales_per_product = raw_record.get('SalesPerProduct', [])
                
                # Verwerk elke product verkoop
                for product in sales_per_product:
                    product_verkopen_record = {
                        'Datum': datetime.strptime(daily_revenue.get('date', ''), '%Y-%m-%d').date(),
                        'Product': product.get('name', ''),
                        'ProductID': product.get('id', ''),
                        'Aantal': product.get('sales', 0),
                        'DatumLaatsteUpdate': datetime.now()
                    }
                    
                    product_verkopen_records.append(product_verkopen_record)
                
            except Exception as e:
                logging.warning(f"Fout bij transformeren van product verkoop record - raw_record={raw_record}, fout: {str(e)}")
                continue
        
        logging.info(f"ProductVerkopen transformatie voltooid: {len(product_verkopen_records)} records getransformeerd")
        return product_verkopen_records
    
    def validate_transformed_data(self, table_name: str, data: List[Dict]) -> List[Dict]:
        """
        Valideer getransformeerde gegevens voor database invoer
        
        Args:
            table_name: Naam van de doeltabel
            data: Getransformeerde gegevens
            
        Returns:
            Geldige records (ongeldige records worden gelogd en gefilterd)
        """
        if not data:
            return []
        
        table_config = self.tables_config.get(table_name, {})
        valid_records = []
        
        for i, record in enumerate(data):
            try:
                # Basis validatie
                if not isinstance(record, dict):
                    raise ValidationError("Record must be a dictionary")
                
                # Controleer vereiste velden
                columns_config = table_config.get('columns', {})
                for source_field, field_config in columns_config.items():
                    target_column = field_config['target_column']
                    required = field_config.get('required', False)
                    
                    if required and target_column not in record:
                        raise ValidationError(f"Missing required field: {target_column}")
                    
                    if required and record.get(target_column) is None:
                        if not field_config.get('allow_null', False):
                            raise ValidationError(f"Required field {target_column} is null")
                
                # Controleer vereiste custom fields
                custom_fields = table_config.get('custom_fields', {})
                for field_name, field_config in custom_fields.items():
                    required = field_config.get('required', False)
                    
                    if required and field_name not in record:
                        raise ValidationError(f"Missing required custom field: {field_name}")
                    
                    if required and record.get(field_name) is None:
                        if not field_config.get('allow_null', False):
                            raise ValidationError(f"Required custom field {field_name} is null")
                
                valid_records.append(record)
                
            except Exception as e:
                logging.warning(f"Validatie mislukt voor record - tabel={table_name}, record_index={i}, fout: {str(e)}")
        
        logging.debug(f"Validatie voltooid voor tabel: {table_name}, input_records={len(data)}, valid_records={len(valid_records)}, invalid_records={len(data) - len(valid_records)}")
        
        return valid_records

    def extract_active_memberships(self, leden_data: List[Dict]) -> List[Dict]:
        """
        Extracteer actieve abonnementen naar aparte tabel

        Args:
            leden_data: Onbewerkte gebruikersgegevens van de API
            
        Returns:
            Lijst van ActieveAbonnementen records
        """
        memberships = []
        seen_combinations = set()  # Volg unieke combinaties om dubbele records te voorkomen
        
        for user in leden_data:
            user_id = user.get('id')
            if not user_id:
                continue
                
            active_memberships = user.get('activeMemberships', [])
            
            for membership in active_memberships:
                try:
                    # Handleer zowel string IDs als volledige objecten
                    if isinstance(membership, str):
                        # Als het alleen een string ID is, maak een minimale record
                        membership_id = membership
                        membership_record = {
                            'LedenId': user_id,
                            'AbonnementId': membership_id,
                            'AbonnementNaam': '',  # Unknown name
                            'Status': 'UNKNOWN',   # Unknown status
                            'DatumLaatsteUpdate': datetime.now()
                        }
                    elif isinstance(membership, dict):
                        # Als het een volledig object is, extract alle velden
                        membership_id = membership.get('id', '')
                        membership_record = {
                            'LedenId': user_id,
                            'AbonnementId': membership_id,
                            'AbonnementNaam': membership.get('name', ''),
                            'Status': membership.get('status', 'UNKNOWN'),
                            'DatumLaatsteUpdate': datetime.now()
                        }
                    else:
                        # Overslaan van onverwachte gegevenstypen
                        logging.warning(f"Onverwacht abonnement type - user_id={user_id}, membership_type={type(membership)}, membership={membership}")
                        continue
                    
                    # Check for duplicates using composite key (LedenId, AbonnementId)
                    combination = (user_id, membership_id)
                    if combination not in seen_combinations:
                        seen_combinations.add(combination)
                        memberships.append(membership_record)
                    else:
                        logging.debug(f"Overslaan van dubbele abonnement - user_id={user_id}, membership_id={membership_id}")
                    
                except Exception as e:
                    logging.warning(f"Fout bij het extraheren van abonnement - user_id={user_id}, membership={membership}, fout: {str(e)}")
        
        logging.info(f"Actieve abonnementen geëxtraheerd - totaal_records={len(memberships)}, unieke_combinaties={len(seen_combinations)}")
        
        return memberships