# Tree11 Data Pipeline - Bestandsstructuur Voorstel

## Mappenstructuur
```
tree11/
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
│   ├── pipeline_runner.py           # Main orchestrator
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
│   ├── setup_database.py            # Eenmalige database setup
│   ├── backfill_data.py             # Historische data laden
│   └── manual_run.py                # Handmatige pipeline runs
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment variables template
├── main.py                          # Entry point voor dagelijkse runs
└── README.md                        # Documentatie
```

## Bestand Details

### 1. Configuratie Bestanden

#### `config/api_endpoints.json`
- API URL templates met parameters
- Request configuraties (headers, pagination, etc.)
- Rate limiting instellingen
- Authentication configuratie

#### `config/schema_mappings.json`
- Kolom mappings van API naar database
- Type conversie regels
- Data validatie regels
- Transformatie functies per kolom

#### `config/database_config.json`
- SQL Server verbinding parameters
- Timeout instellingen
- Batch size configuraties
- Error handling opties

#### `config/pipeline_config.json`
- Schedule configuratie
- Data retention policies
- Update strategieën per tabel
- Feature flags

### 2. Python Source Code

#### `src/data_extractor.py`
- API client class voor Gymly
- Pagination handling
- Rate limiting en retry logic
- Data caching voor efficiency
- Error handling en logging

#### `src/data_transformer.py`
- Schema mapping implementatie
- Type conversie functies
- Data cleaning en validatie
- Date/time parsing
- JSON handling voor arrays

#### `src/database_manager.py`
- SQL Server connectie management
- UPSERT/INSERT/REPLACE operaties
- Bulk loading voor performance
- Transaction management
- Backup en recovery functies

#### `src/google_sheets_manager.py`
- Google Sheets API integratie
- PersonalTraining data extractie
- Authentication handling
- Data formatting

#### `src/logger.py`
- Structured logging setup
- Performance metrics
- Error tracking
- Data quality metrics
- Notification system (email/slack)

#### `src/pipeline_runner.py`
- Orchestration van alle stappen
- Dependency management tussen tabellen
- Error recovery en rollback
- Progress tracking
- Health checks

#### `src/utils.py`
- Common utility functies
- Date/time helpers
- Configuration loading
- Environment variable handling

### 3. SQL Scripts

#### `sql/create_tables.sql`
- Alle tabel definities met juiste types
- Primary keys en constraints
- Foreign key relationships
- Indexing strategy

#### `sql/stored_procedures.sql`
- Bulk UPSERT procedures
- Data validation procedures
- Cleanup en maintenance procedures
- Reporting procedures

#### `sql/indexes.sql`
- Performance indexes
- Query optimization
- Maintenance scripts

### 4. Entry Points

#### `main.py`
- Dagelijkse pipeline execution
- Command line interface
- Environment setup
- Error handling en notifications

#### `scripts/setup_database.py`
- Eenmalige database setup
- Tabel creatie
- Initial data loading
- Index creatie

#### `scripts/backfill_data.py`
- Historische data import
- Batch processing
- Progress tracking
- Resume functionaliteit

#### `scripts/manual_run.py`
- Ad-hoc pipeline runs
- Specific table updates
- Development en testing
- Data quality checks

### 5. Testing

#### Unit tests voor alle modules
- Mock API responses
- Database integration tests
- Schema validation tests
- Error scenario tests

### 6. Dependencies (requirements.txt)
```
requests>=2.31.0
python-dotenv>=1.0.0
pyodbc>=4.0.39
sqlalchemy>=2.0.0
pandas>=2.0.0
google-auth>=2.17.0
google-auth-oauthlib>=1.0.0
google-auth-httplib2>=0.1.0
google-api-python-client>=2.88.0
schedule>=1.2.0
structlog>=23.1.0
pydantic>=2.0.0
tenacity>=8.2.0
```

## Execution Flow

### Dagelijkse Pipeline
1. **Initialize** - Laden configuratie en setup logging
2. **Extract** - Data ophalen van alle API endpoints
3. **Transform** - Data cleaning en mapping naar Nederlandse schema
4. **Validate** - Data quality checks
5. **Load** - Bulk insert/update naar SQL Server
6. **Verify** - Data integrity checks
7. **Cleanup** - Temporary files en oude logs
8. **Report** - Success/failure notifications

### Error Handling
- Retry logic met exponential backoff
- Partial failure recovery
- Data quality alerts
- Automatic rollback bij kritieke fouten
- Notification system voor operations team

### Monitoring
- Pipeline execution metrics
- Data freshness tracking
- Error rate monitoring
- Performance metrics
- Data volume tracking

## Deployment Opties

### Option 1: Scheduled Script
- Windows Task Scheduler of cron job
- Local uitvoering op server
- Simple deployment

### Option 2: Docker Container
- Containerized deployment
- Kubernetes orchestration
- Cloud native approach

### Option 3: Azure Data Factory
- Cloud-based orchestration
- Built-in monitoring
- Enterprise features

## Security Consideraties

- API tokens in environment variables
- Database credentials encrypted
- Audit logging voor compliance
- Data masking voor PII
- Access control op database niveau 