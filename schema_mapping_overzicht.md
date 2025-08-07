# Tree11 Data Pipeline - Schema Mapping Overzicht

## Tabel 1: Leden
**Bron API:** `api.gymly.io_users` (5866 records, paginated)
**Database Tabel:** `Leden`

| Bron Kolom (API) | Type | Nederlandse Kolom | SQL Type | Transformatie |
|------------------|------|-------------------|----------|---------------|
| `id` | str | `Id` | NVARCHAR(50) | Direct |
| `accountId` | str | `AccountId` | NVARCHAR(50) | Direct |
| `businessId` | str | `BedrijfId` | NVARCHAR(50) | Direct |
| `primaryLocationId` | str | `PrimaireLocatieId` | NVARCHAR(50) | Direct |
| `activeMemberships` | array[str] | `ActieveAbonnementen` | NVARCHAR(MAX) | JSON.dumps |
| `active` | bool | `Actief` | BIT | Direct |
| `paymentCollectionStrategy` | str | `BetalingsInzamelStrategie` | NVARCHAR(20) | Direct |
| `customerNumber` | str | `KlantNummer` | NVARCHAR(50) | Direct |
| `createdAt` | str | `AangemaaktOp` | DATETIME2 | ISO to datetime |
| `updatedAt` | str | `GewijzigdOp` | DATETIME2 | ISO to datetime |
| `fullName` | str | `VolledigeNaam` | NVARCHAR(200) | Direct |
| `firstName` | str | `Voornaam` | NVARCHAR(100) | Direct |
| `lastName` | str | `Achternaam` | NVARCHAR(100) | Direct |
| `email` | str | `EmailAdres` | NVARCHAR(255) | Direct |
| `phoneNumber` | str | `Telefoonnummer` | NVARCHAR(20) | Direct |
| `address.street` | str | `Straat` | NVARCHAR(255) | Direct |
| `address.postalCode` | str | `Postcode` | NVARCHAR(10) | Direct |
| `address.city` | str | `Plaats` | NVARCHAR(100) | Direct |
| `address.country` | str | `Land` | NVARCHAR(100) | Direct |
| `gender` | str | `Geslacht` | NVARCHAR(10) | Direct |

## Tabel 2: Abonnementen
**Bron API:** `api.gymly.io_memberships` (137 records, array)
**Database Tabel:** `Abonnementen`

| Bron Kolom (API) | Type | Nederlandse Kolom | SQL Type | Transformatie |
|------------------|------|-------------------|----------|---------------|
| `id` | str | `Id` | NVARCHAR(50) | Direct |
| `name` | str | `Naam` | NVARCHAR(255) | Direct |
| `description` | str | `Beschrijving` | NVARCHAR(MAX) | Direct |
| `type` | str | `Type` | NVARCHAR(50) | Direct |
| `paymentType` | str | `BetalingsType` | NVARCHAR(50) | Direct |
| `amount` | float | `Bedrag` | DECIMAL(10,2) | Direct |
| `currency` | str | `Valuta` | NVARCHAR(3) | Direct |
| `expireDays` | int | `VerloopDagen` | INT | Direct |
| `activationStrategy` | str | `ActivatieStrategie` | NVARCHAR(50) | Direct |
| `accessCourseSubscriptionRequired` | bool | `ToegangsAbonnementVereist` | BIT | Direct |
| `consumptionMethod` | str | `ConsumptieMethode` | NVARCHAR(50) | Direct |
| `contractAutoRenewal` | bool | `ContractAutoVerlenging` | BIT | Direct |
| `ledgerGroupId` | str | `GrootboekGroepId` | NVARCHAR(50) | Direct |
| - | - | `Sectie` | NVARCHAR(100) | Afgeleid van Google Sheet |

## Tabel 3: AbonnementStatistieken
**Bron API:** Multiple analytics endpoints
**Database Tabel:** `AbonnementStatistieken`

| Bron Data | Type | Nederlandse Kolom | SQL Type | Transformatie |
|-----------|------|-------------------|----------|---------------|
| URL parameter | str | `Categorie` | NVARCHAR(50) | Extract from endpoint (new/paused/active/expired) |
| URL parameter | str | `Type` | NVARCHAR(20) | Extract PAYMENT_TYPE filter (ONCE/PERIODIC) |
| `labels[i]` | str | `Datum` | DATE | Parse date string |
| `series[0].data[i]` | int | `Aantal` | INT | Direct |

## Tabel 4: Lessen
**Bron API:** `api.gymly.io_activity_events` (46 records, array)
**Database Tabel:** `Lessen`

| Bron Kolom (API) | Type | Nederlandse Kolom | SQL Type | Transformatie |
|------------------|------|-------------------|----------|---------------|
| `id` | str | `Id` | NVARCHAR(50) | Direct |
| `name` | str | `Naam` | NVARCHAR(255) | Direct |
| `startAt` | str | `StartTijd` | DATETIME2 | ISO to datetime |
| `endAt` | str | `EindTijd` | DATETIME2 | ISO to datetime |
| `capacity` | int | `Capaciteit` | INT | Direct |
| `memberCount` | int | `LedenAantal` | INT | Direct |
| `trialCount` | int | `ProefledenAantal` | INT | Direct |
| `businessLocationId` | str | `BedrijfsLocatieId` | NVARCHAR(50) | Direct |
| `activities` | array[str] | `Activiteiten` | NVARCHAR(MAX) | JSON.dumps |
| `recurring` | bool | `Terugkerend` | BIT | Direct |
| `createdAt` | str | `AangemaaktOp` | DATETIME2 | ISO to datetime |
| `updatedAt` | str | `GewijzigdOp` | DATETIME2 | ISO to datetime |
| `teachers` | array[dict] | `Instructeurs` | NVARCHAR(MAX) | JSON.dumps |

## Tabel 5: Openstaande Facturen
**Bron API:** `api.gymly.io_invoices` (status=PENDING)
**Database Tabel:** `OpenstaandeFacturen`
**Opmerking:** API geeft momenteel error 400, moet onderzocht worden

| Verwachte Bron Kolom | Type | Nederlandse Kolom | SQL Type | Transformatie |
|----------------------|------|-------------------|----------|---------------|
| `id` | str | `Id` | NVARCHAR(50) | Direct |
| `number` | int | `Nummer` | INT | Direct |
| `numberFormatted` | str | `NummerFormatted` | NVARCHAR(50) | Direct |
| `status` | str | `Status` | NVARCHAR(20) | Direct |
| `type` | str | `Type` | NVARCHAR(50) | Direct |
| `year` | int | `Jaar` | INT | Direct |
| `businessLocationId` | str | `BedrijfsLocatieId` | NVARCHAR(50) | Direct |
| `totalAmount` | float | `TotaalBedrag` | DECIMAL(10,2) | Direct |
| `createdAt` | str | `AangemaaktOp` | DATETIME2 | ISO to datetime |

## Tabel 6: Omzet
**Bron API:** `analytics_revenue` daily breakdown
**Database Tabel:** `Omzet`

| Bron Kolom (API) | Type | Nederlandse Kolom | SQL Type | Transformatie |
|------------------|------|-------------------|----------|---------------|
| `dailyRevenue[i].date` | str | `Datum` | DATE | Parse date |
| `dailyRevenue[i].ledgerAccountId` | str | `GrootboekRekeningId` | NVARCHAR(50) | Direct |
| `dailyRevenue[i].type` | str | `Type` | NVARCHAR(50) | Direct |
| `dailyRevenue[i].revenue` | float | `Omzet` | DECIMAL(12,2) | Direct |

## Tabel 7: GrootboekRekening
**Bron API:** `analytics_revenue.ledgerAccounts`
**Database Tabel:** `GrootboekRekening`

| Bron Kolom (API) | Type | Nederlandse Kolom | SQL Type | Transformatie |
|------------------|------|-------------------|----------|---------------|
| `ledgerAccounts[i].id` | str | `Id` | NVARCHAR(50) | Direct |
| `ledgerAccounts[i].key` | str | `Sleutel` | NVARCHAR(50) | Direct |
| `ledgerAccounts[i].label` | str | `Label` | NVARCHAR(255) | Direct (null to empty string) |

## Tabel 8: PersonalTraining
**Bron API:** Google Sheets
**Database Tabel:** `PersonalTraining`

| Bron Kolom (Sheet) | Type | Nederlandse Kolom | SQL Type | Transformatie |
|--------------------|------|-------------------|----------|---------------|
| `Voornaam` | str | `Voornaam` | NVARCHAR(100) | Direct |
| `Achternaam` | str | `Achternaam` | NVARCHAR(100) | Direct |
| `Datum` | date | `Datum` | DATE | Direct |
| `Uren` | int | `Uren` | INT | Direct |

## Constanten en Filters

- **Tree11 Locatie ID:** `759cf904-4133-4fd8-af4c-ded2cedb6192`
- **Business ID:** `df5acf01-8dfd-476b-9ba3-1d939f73fe1e`
- **Standaard Granulariteit Analytics:** `DAY`
- **Date Range:** Laatste 30 dagen + vandaag voor historische data

## Type Conversie Mapping

| API Type | SQL Type | Python Conversion |
|----------|----------|-------------------|
| `str` | `NVARCHAR(x)` | `str(value) if value else ''` |
| `int` | `INT` | `int(value) if value else 0` |
| `float` | `DECIMAL(x,y)` | `float(value) if value else 0.0` |
| `bool` | `BIT` | `bool(value)` |
| `array[str]` | `NVARCHAR(MAX)` | `json.dumps(value) if value else '[]'` |
| `array[dict]` | `NVARCHAR(MAX)` | `json.dumps(value) if value else '[]'` |
| `datetime string` | `DATETIME2` | `datetime.fromisoformat(value.replace('Z', '+00:00'))` |
| `date string` | `DATE` | `datetime.strptime(value, '%Y-%m-%d').date()` |

## Data Update Strategieën

| Tabel | Update Methode | Frequency | Key Field |
|-------|----------------|-----------|-----------|
| `Leden` | UPSERT | Dagelijks | `Id` |
| `Abonnementen` | REPLACE | Wekelijks | `Id` |
| `AbonnementStatistieken` | INSERT/UPDATE | Dagelijks | `Datum` + `Categorie` + `Type` |
| `Lessen` | UPSERT | Dagelijks | `Id` |
| `OpenstaandeFacturen` | REPLACE | Dagelijks | `Id` |
| `Omzet` | INSERT/UPDATE | Dagelijks | `Datum` + `GrootboekRekeningId` + `Type` |
| `GrootboekRekening` | UPSERT | Wekelijks | `Id` |
| `PersonalTraining` | REPLACE | Wekelijks | Composite key | 