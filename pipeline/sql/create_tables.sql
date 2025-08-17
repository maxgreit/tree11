-- Tree11 Data Pipeline - Vereenvoudigde Database Schema Creation
-- SQL Server Database Tables for Tree11 Fitness Data (zonder Primary Keys, Indexes en Dependencies)

-- Enable SQL Server features if needed
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- Create database schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'tree11')
BEGIN
    EXEC('CREATE SCHEMA tree11');
END
GO

-- =====================================================================
-- 1. LEDEN (Members/Users)
-- Bron: api.gymly.io_users (5866 records)
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Leden' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.Leden (
        Id NVARCHAR(50) NOT NULL,
        Naam NVARCHAR(255) NULL,
        AccountId NVARCHAR(50) NULL,
        BedrijfId NVARCHAR(50) NOT NULL,
        PrimaireLocatieId NVARCHAR(50) NULL,
        Actief BIT NOT NULL DEFAULT 1,
        BetalingsVorm NVARCHAR(20) NULL,
        KlantNummer NVARCHAR(50) NULL,
        AangemaaktOp DATETIME2 NOT NULL,
        GewijzigdOp DATETIME2 NOT NULL,
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.Leden aangemaakt';
END
GO

-- =====================================================================
-- 2. ACTIEVE ABONNEMENTEN (Active Memberships)
-- Bron: Afgeleid van api.gymly.io_users.activeMemberships
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ActieveAbonnementen' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.ActieveAbonnementen (
        LedenId NVARCHAR(50) NOT NULL,
        AbonnementId NVARCHAR(50) NOT NULL,
        AbonnementNaam NVARCHAR(255) NULL,
        Status NVARCHAR(20) NULL,
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.ActieveAbonnementen aangemaakt';
END
GO

-- =====================================================================
-- 3. ABONNEMENTEN (Memberships)
-- Bron: api.gymly.io_memberships (137 records)
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Abonnementen' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.Abonnementen (
        AbonnementId NVARCHAR(50) NOT NULL,
        Naam NVARCHAR(255) NOT NULL,
        Beschrijving NVARCHAR(MAX) NULL,
        Type NVARCHAR(50) NULL,
        BetalingsType NVARCHAR(50) NULL,
        Bedrag DECIMAL(10,2) NULL,
        Valuta NVARCHAR(3) NULL DEFAULT 'EUR',
        VervalPeriode INT NULL,
        ActivatieStrategie NVARCHAR(50) NULL,
        AbonnementVereist BIT NULL DEFAULT 0,
        ConsumptieMethode NVARCHAR(50) NULL,
        AutoVerlenging BIT NULL DEFAULT 0,
        GrootboekGroepId NVARCHAR(50) NULL,
        Sectie NVARCHAR(100) NULL,
        ContractDuur NVARCHAR(20) NULL, -- NIEUW: MONTH, YEAR, etc.
        ContractPeriode INT NULL, -- NIEUW: 1, 6, 12, etc.
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.Abonnementen aangemaakt';
END
GO

-- =====================================================================
-- 4. ABONNEMENT STATISTIEKEN
-- Bron: Multiple analytics endpoints (new/paused/active/expired)
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AbonnementStatistieken' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.AbonnementStatistieken (
        Datum DATE NOT NULL,
        Categorie NVARCHAR(50) NOT NULL,
        Type NVARCHAR(20) NOT NULL,
        Aantal INT NOT NULL DEFAULT 0,
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.AbonnementStatistieken aangemaakt';
END
GO

-- =====================================================================
-- 5. LESSEN (Activity Events)
-- Bron: api.gymly.io_activity_events (46 records per dag)
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Lessen' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.Lessen (
        Id NVARCHAR(50) NOT NULL,
        Naam NVARCHAR(255) NULL,
        StartTijd DATETIME2 NOT NULL,
        EindTijd DATETIME2 NOT NULL,
        Capaciteit INT NULL DEFAULT 0,
        LedenAantal INT NULL DEFAULT 0,
        ProefledenAantal INT NULL DEFAULT 0,
        BedrijfsLocatieId NVARCHAR(50) NULL,
        Activiteiten NVARCHAR(MAX) NULL,
        Terugkerend BIT NULL DEFAULT 0,
        AangemaaktOp DATETIME2 NOT NULL,
        GewijzigdOp DATETIME2 NOT NULL,
        Instructeurs NVARCHAR(MAX) NULL,
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.Lessen aangemaakt';
END
GO

-- =====================================================================
-- 6. LES DEELNAME (Course Members)
-- Bron: api.gymly.io_courses/{courseId}/members
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'LesDeelname' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.LesDeelname (
        LesId NVARCHAR(50) NOT NULL, -- CourseId uit Lessen tabel
        AccountId NVARCHAR(50) NOT NULL, -- id uit API response
        Type NVARCHAR(50) NULL, -- type uit API response
        Status NVARCHAR(50) NULL, -- status uit API response
        Naam NVARCHAR(255) NULL, -- fullName uit API response
        Aanwezig BIT NULL, -- attended uit API response
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.LesDeelname aangemaakt';
END
GO

-- =====================================================================
-- 7. GROOTBOEK REKENING
-- Bron: api.gymly.io_analytics_revenue (grootboek groepen)
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'GrootboekRekening' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.GrootboekRekening (
        GrootboekGroepId NVARCHAR(50) NOT NULL,
        Naam NVARCHAR(255) NOT NULL,
        Beschrijving NVARCHAR(MAX) NULL,
        Type NVARCHAR(50) NULL,
        Actief BIT NOT NULL DEFAULT 1,
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.GrootboekRekening aangemaakt';
END
GO

-- =====================================================================
-- 8. OMZET
-- Bron: api.gymly.io_analytics_revenue (dagelijkse omzet data)
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Omzet' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.Omzet (
        Datum DATE NOT NULL,
        GrootboekRekeningId NVARCHAR(50) NOT NULL,
        Type NVARCHAR(50) NOT NULL,
        Omzet DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.Omzet aangemaakt';
END
GO

-- =====================================================================
-- 9. OPENSTAANDE FACTUREN
-- Bron: api.gymly.io_invoices_pending
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'OpenstaandeFacturen' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.OpenstaandeFacturen (
        FactuurId NVARCHAR(50) NOT NULL,
        Nummer INT NULL,
        LedenId NVARCHAR(50) NULL,
        Bedrag DECIMAL(10,2) NOT NULL,
        Valuta NVARCHAR(3) NULL DEFAULT 'EUR',
        Status NVARCHAR(50) NULL,
        Vervaldatum DATE NULL,
        AangemaaktOp DATETIME2 NOT NULL,
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.OpenstaandeFacturen aangemaakt';
END
GO

-- =====================================================================
-- 10. PERSONAL TRAINING
-- Bron: Google Sheets
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PersonalTraining' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.PersonalTraining (
        Id NVARCHAR(50) NOT NULL,
        LedenId NVARCHAR(50) NULL,
        TrainerId NVARCHAR(50) NULL,
        TrainerNaam NVARCHAR(255) NULL,
        Datum DATE NOT NULL,
        StartTijd TIME NULL,
        EindTijd TIME NULL,
        Type NVARCHAR(100) NULL,
        Status NVARCHAR(50) NULL,
        Opmerkingen NVARCHAR(MAX) NULL,
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.PersonalTraining aangemaakt';
END
GO

-- =====================================================================
-- X. ABONNEMENT STATISTIEKEN SPECIFIEK (per Abonnement)
-- Bron: analytics/memberships (new/paused/active/expirations) met filter.MEMBERSHIP
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AbonnementStatistiekenSpecifiek' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.AbonnementStatistiekenSpecifiek (
        Datum DATE NOT NULL,
        Categorie NVARCHAR(50) NOT NULL,
        Type NVARCHAR(20) NOT NULL,
        AbonnementId NVARCHAR(50) NOT NULL,
        Aantal INT NOT NULL DEFAULT 0,
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.AbonnementStatistiekenSpecifiek aangemaakt';
END
GO

-- =====================================================================
-- 11. UITBETALINGEN (Payouts)
-- Bron: api.gymly.io_payouts
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Uitbetalingen' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.Uitbetalingen (
        UitbetalingID NVARCHAR(50) NOT NULL,
        Datum DATETIME2 NOT NULL,
        Betalingen INT NOT NULL DEFAULT 0,
        Chargebacks INT NOT NULL DEFAULT 0,
        Refunds INT NOT NULL DEFAULT 0,
        NettoBedrag DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        BrutoBedrag DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        ChargebackBedrag DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        RefundBedrag DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        CommissieBedrag DECIMAL(10,2) NOT NULL DEFAULT 0.00,
        Status NVARCHAR(50) NOT NULL,
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    PRINT 'Tabel tree11.Uitbetalingen aangemaakt';
END
GO

-- =====================================================================
-- 12. PRODUCTVERKOPEN (Product Sales)
-- Bron: api.gymly.io_daily_revenue
-- =====================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ProductVerkopen' AND schema_id = SCHEMA_ID('tree11'))
BEGIN
    CREATE TABLE tree11.ProductVerkopen (
        ProductVerkopenID INT IDENTITY(1,1) NOT NULL,
        Datum DATE NOT NULL,
        Product NVARCHAR(255) NOT NULL,
        ProductID NVARCHAR(50) NOT NULL,
        Aantal INT NOT NULL DEFAULT 0,
        DatumLaatsteUpdate DATETIME2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_ProductVerkopen PRIMARY KEY (ProductVerkopenID),
        CONSTRAINT IX_ProductVerkopen_Datum_Product UNIQUE (Datum, ProductID)
    );
    
    PRINT 'Tabel tree11.ProductVerkopen aangemaakt';
END
GO

PRINT 'Alle tabellen succesvol aangemaakt!'; 