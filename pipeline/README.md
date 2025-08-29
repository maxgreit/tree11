# Tree11 Data Pipeline

Een geautomatiseerde data synchronisatie pipeline voor Tree11 Fitness die dagelijks data synchroniseert tussen Gymly API, Google Sheets en Microsoft SQL Server.

## 📋 Overzicht

Deze pipeline haalt automatisch data op van verschillende bronnen, transformeert deze naar een Nederlandse database structuur, en laadt de data in SQL Server tabellen voor rapportage en analyse.

### Data Bronnen
- **Gymly API**: Leden, abonnementen, lessen, omzet, en statistieken
- **Google Sheets**: Personal Training uren en sessies

### Database Tabellen
- `Leden` - Alle ledeninformatie (5.866+ records)
- `Abonnementen` - Abonnement types en prijzen (137 types)
- `AbonnementStatistieken` - Dagelijkse abonnement bewegingen (handmatig op te halen met --tables)
- `Lessen` - Les planning en bezetting (46+ per dag)
- `Omzet` - Dagelijkse omzet cijfers
- `GrootboekRekening` - Financiële categorieën
- `OpenstaandeFacturen` - Nog te betalen facturen
- `PersonalTraining` - PT sessies en uren
- `Uitbetalingen` - Uitbetalingen en transactie overzichten
- `ProductVerkopen` - Dagelijkse product verkopen per product

## 📝 Recente Wijzigingen

### Pipeline Optimalisatie (Laatste Update)
- **AbonnementStatistieken** is uit de standaard dagelijkse pipeline gehaald om de uitvoeringstijd te verkorten
- **Tijdsperioden aangepast** voor betere performance:
  - `Lessen`: van 1 dag terug + 7 dagen vooruit → 7 dagen terug
  - `LesDeelname`: van 7 dagen terug → 7 dagen terug (behouden)
  - `AbonnementStatistiekenSpecifiek`: van 30 dagen terug → 7 dagen terug
- **AbonnementStatistieken** kan nog steeds handmatig worden opgehaald met: `python main.py --tables AbonnementStatistieken`

### Nieuwe Functionaliteit (Laatste Update)
- **Uitbetalingen** tabel toegevoegd voor payout data van Gymly API
- **Nieuwe kolommen**: UitbetalingID, Datum, Betalingen, Chargebacks, Refunds, NettoBedrag, BrutoBedrag, ChargebackBedrag, RefundBedrag, CommissieBedrag, Status
- **Uitbetalingen** wordt standaard meegenomen in de dagelijkse pipeline
- **Handmatig ophalen** mogelijk met: `python main.py --tables Uitbetalingen`

- **ProductVerkopen** tabel toegevoegd voor dagelijkse product verkopen
- **Nieuwe kolommen**: Datum, Product, ProductID, Aantal
- **ProductVerkopen** wordt standaard meegenomen in de dagelijkse pipeline (afgelopen week)
- **Handmatig ophalen** mogelijk met: `python main.py --tables ProductVerkopen`

### Performance Optimalisaties (Laatste Update)
- **Database loading geoptimaliseerd** voor grote datasets (>1000 records)
- **Bulk operaties** implementeren TRUNCATE + BULK INSERT voor replace strategie
- **Chunked processing** met 5000 records per batch voor betere memory management
- **Index management** tijdelijk uitschakelen tijdens bulk loading voor snellere performance
- **Verwachte verbetering**: 3-5x snellere database loading voor grote tabellen

## 🚀 Snelle Start

### Vereisten
- Python 3.8+
- Microsoft SQL Server (2016+)
- Gymly API toegang
- Google Sheets API toegang (optioneel)

### 1. Installatie

```bash
# Clone de repository
git clone <repository-url>
cd tree11/pipeline

# Installeer dependencies
pip install -r requirements.txt
```

### 2. Configuratie

```bash
# Kopieer environment template
cp env_example.txt .env

# Bewerk .env met je credentials
nano .env
```

Vul minimaal in:
```
GYMLY_API_TOKEN=your_api_token
DB_SERVER=your_sql_server
DB_NAME=Tree11
DB_USERNAME=your_username
DB_PASSWORD=your_password
```

### 3. Database Setup

```bash
# Eenmalige database setup
python scripts/setup_database.py --config-dir config/

# Controleer setup
python scripts/setup_database.py --config-dir config/ --verbose
```

### 4. Test Run

```bash
# Test run zonder database wijzigingen
python main.py --dry-run --verbose

# Eerste echte run
python main.py --verbose
```

## 📁 Project Structuur

```
tree11/pipeline/
├── config/                          # Configuratie bestanden
│   ├── api_endpoints.json           # API endpoint configuraties
│   ├── schema_mappings.json         # Kolom mappings en transformaties
│   ├── database_config.json         # Database verbinding configuratie
│   └── pipeline_config.json         # Algemene pipeline instellingen
├── src/                             # Python source code
│   ├── __init__.py
│   ├── data_extractor.py            # API data extractie
│   ├── data_transformer.py          # Data transformatie en cleaning
│   ├── database_manager.py          # SQL Server database operaties
│   ├── google_sheets_manager.py     # Google Sheets integratie
│   ├── logger.py                    # Logging en monitoring
│   └── utils.py                     # Utility functies
├── sql/                             # SQL scripts
│   ├── create_tables.sql            # Database tabel definities
│   ├── stored_procedures.sql        # Stored procedures voor data loading
│   └── indexes.sql                  # Database indexes voor performance
├── logs/                            # Log bestanden (auto-created)
├── tests/                           # Unit tests
│   ├── __init__.py
│   ├── test_data_extractor.py
│   ├── test_data_transformer.py
│   └── test_database_manager.py
├── scripts/                         # Helper scripts
│   └── setup_database.py            # Eenmalige database setup
├── requirements.txt                 # Python dependencies
├── env_example.txt                  # Environment variables template
├── main.py                          # Entry point voor dagelijkse runs
└── README.md                        # Deze documentatie
```

## ⚙️ Configuratie

### API Endpoints (`config/api_endpoints.json`)
Configuratie voor alle Gymly API endpoints en Google Sheets integratie.

### Schema Mappings (`config/schema_mappings.json`)
Mapping van API velden naar Nederlandse database kolommen met type conversies.

### Database Config (`config/database_config.json`)
SQL Server verbinding parameters en performance instellingen.

### Pipeline Config (`config/pipeline_config.json`)
Algemene pipeline instellingen zoals schedule, data retention, en notifications.

## 🏃‍♂️ Gebruik

### Dagelijkse Run
```bash
# Standaard dagelijkse run (alle tabellen)
python main.py

# Specifieke tabellen
python main.py --tables Leden,Lessen

# Met uitgebreide logging
python main.py --verbose

# Dry run (geen database wijzigingen)
python main.py --dry-run
```

### Database Onderhoud
```bash
# Database setup
python scripts/setup_database.py

# Forceer herinstallatie
python scripts/setup_database.py --force
```

### Testing
```bash
# Run alle tests
python -m pytest tests/

# Specifieke test
python -m pytest tests/test_data_extractor.py -v

# Met coverage
python -m pytest tests/ --cov=src --cov-report=html
```

## 📊 Monitoring

### Logging
De pipeline gebruikt structured logging met verschillende levels:
- **INFO**: Normale operaties en voortgang
- **WARNING**: Problemen die geen failure veroorzaken
- **ERROR**: Fouten die manual interventie vereisen
- **DEBUG**: Gedetailleerde informatie voor troubleshooting

Log bestanden worden opgeslagen in `logs/`:
- `tree11_pipeline.log` - Hoofdlog file (rotating)
- `tree11_errors.log` - Alleen errors

### Database Monitoring
- `tree11.DataQualityLog` - Data quality checks

### Notifications
Ondersteuning voor:
- Email notifications (SMTP)
- Slack webhooks
- Custom webhooks

## 🔧 Data Quality

### Validatie
- Required field checks
- Data type validatie
- Business rule validatie
- Anomaly detection

### Error Handling
- Retry logic met exponential backoff
- Graceful degradation
- Partial failure recovery
- Automatic rollback bij kritieke fouten

## 📈 Performance

### Database Optimalisatie
- Bulk insert operaties
- Connection pooling
- Optimized indexes
- Query performance monitoring

### API Rate Limiting
- Respecteert Gymly API rate limits
- Automatic backoff bij rate limiting
- Request caching waar mogelijk

## 🔒 Security

### Data Protection
- Environment variables voor credentials
- Encrypted sensitive logs
- Personal data masking
- Audit logging

### Access Control
- Database role-based security
- API token rotation ondersteuning
- Minimal privilege principle

## 🚨 Troubleshooting

### Veel Voorkomende Problemen

#### Database Connectie Fouten
```bash
# Test database connectie
python -c "from src.database_manager import DatabaseManager; dm = DatabaseManager('config/database_config.json'); print('✓ Connection successful')"
```

#### API Token Problemen
```bash
# Test API token
curl -H "Authorization: Bearer YOUR_TOKEN" https://api.gymly.io/api/v2/businesses/YOUR_BUSINESS_ID/users?size=1
```

#### Google Sheets Toegang
1. Controleer credentials file path in `.env`
2. Verificeer Google Sheets API is enabled
3. Test met kleiner bereik eerst

### Log Analyse
```bash
# Bekijk recente errors
tail -f logs/tree11_errors.log

# Pipeline execution status
grep "Pipeline execution" logs/tree11_pipeline.log | tail -10

# Performance monitoring
grep "duration_seconds" logs/tree11_pipeline.log | tail -5
```

### Database Queries
```sql
-- Data quality issues
SELECT * FROM tree11.DataQualityLog 
WHERE CheckStatus != 'PASS' 
ORDER BY CheckTijd DESC;

-- Table record counts
SELECT 
    'Leden' as Tabel, COUNT(*) as Records FROM tree11.Leden
UNION ALL
SELECT 'Abonnementen', COUNT(*) FROM tree11.Abonnementen
UNION ALL
SELECT 'Lessen', COUNT(*) FROM tree11.Lessen;
```

## 🔄 Deployment

### Productie Deployment
1. Setup dedicated SQL Server database
2. Configure Windows Task Scheduler of cron job
3. Setup monitoring en alerting
4. Configure backup strategy
5. Setup log rotation

### Voorbeeld Cron Job
```bash
# Dagelijks om 02:00
0 2 * * * cd /path/to/tree11/pipeline && python main.py >> logs/cron.log 2>&1
```

### Windows Task Scheduler
- Program: `python`
- Arguments: `main.py`
- Start in: `C:\path\to\tree11\pipeline`
- Schedule: Daily at 02:00

## 📋 Changelog

### v1.0.0
- Initiële release
- Alle 8 data tabellen geïmplementeerd
- Volledige Gymly API integratie
- Google Sheets ondersteuning
- Comprehensive error handling
- Performance optimalisatie

## 🤝 Support

Voor vragen of problemen:
- **Email**: support@greit.nl
- **Documentation**: Deze README en inline code comments
- **Logs**: Controleer `logs/` directory voor troubleshooting

## 📄 Licentie

Copyright © 2025 Greit IT Consultancy. Alle rechten voorbehouden.

---

**Tree11 Data Pipeline** - Geautomatiseerde data synchronisatie voor betere bedrijfsinzichten. 