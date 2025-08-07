# Tree11 Data Integratie Project
## Overzicht Geautomatiseerde Data Synchronisatie

*Rapport opgesteld op: Januari 2025*  
*Voor: Tree11 Fitness*  
*Door: Greit*

---

## Samenvatting

Een geautomatiseerde data synchronisatie systeem dat dagelijks alle belangrijke bedrijfsgegevens van Tree11 uit verschillende bronnen haalt en overzichtelijk opslaat in een centrale database. Dit systeem zorgt ervoor dat Tree11 altijd actuele managementinformatie heeft voor betere bedrijfsbeslissingen.

## Wat Doet Het Systeem?

Het systeem werkt volledig automatisch en voert elke dag de volgende stappen uit:

1. **Data Ophalen** - Haalt automatisch de nieuwste gegevens op uit het Gymly systeem
2. **Data Verwerken** - Zet de gegevens om naar een duidelijke, Nederlandse structuur  
3. **Data Opslaan** - Bewaart alles in een gestructureerde database
4. **Rapportage** - Houdt bij of alles goed is verlopen
5. **Visualisatie** - De data wordt gebruikt om bedrijfsinzichten te genereren

## Welke Gegevens Worden Gesynchroniseerd?

### 1. **Leden**
- **Wat:** Alle gegevens van uw leden
- **Hoeveel:** Momenteel circa 5.866 leden
- **Bijgewerkt:** Dagelijks
- **Bevat:** Abonnementen, betalingsinformatie en geanonimiseerde id's

### 2. **Abonnementen**  
- **Wat:** Alle beschikbare abonnement types
- **Hoeveel:** Momenteel 137 verschillende abonnementen
- **Bijgewerkt:** Wekelijks
- **Bevat:** Prijzen, voorwaarden, looptijden, abonnement categorieën

### 3. **Abonnement Statistieken**
- **Wat:** Dagelijkse cijfers over abonnement bewegingen
- **Bijgewerkt:** Dagelijks  
- **Bevat:** Nieuwe abonnementen, gepauzeerde abonnementen, verlopen abonnementen en actieve abonnementen

### 4. **Lessen**
- **Wat:** Alle geplande lessen en deelnemers
- **Bijgewerkt:** Dagelijks
- **Bevat:** Lessentijden, capaciteit, aantal deelnemers

### 5. **Openstaande Facturen**
- **Wat:** De openstaande facturen
- **Bijgewerkt:** Dagelijks
- **Bevat:** Openstaande facturen, op te delen per sectie

### 6. **Omzet**
- **Wat:** Omzet gegevens
- **Bijgewerkt:** Dagelijks
- **Bevat:** Dagelijkse omzet per grootboekrekening

### 7. **Grootboekrekeningen**
- **Wat:** Alle grootboekrekeningen waar omzet op geboekt wordt
- **Bijgewerkt:** Dagelijks
- **Bevat:** De namen en id's van de omzet grootboekrekeningen

### 8. **Personal Training**
- **Wat:** Personal training sessies en uren
- **Bron:** Google Sheets administratie
- **Bijgewerkt:** Wekelijks
- **Bevat:** Trainer namen, data, gewerkte uren

## Hoe Vaak Wordt Alles Bijgewerkt?

| Gegevenstype | Update Frequentie | Reden |
|--------------|-------------------|-------|
| Leden | Dagelijks | Voor actuele ledenadministratie |
| Abonnementen | Wekelijks | Wijzigen niet vaak |
| Abonnement Statistieken | Dagelijks | Voor sales tracking |
| Lessen | Dagelijks | Voor planning en bezettingsgraad |
| Openstaande Facturen | Dagelijks | Voor financiële aansturing |
| Omzet | Dagelijks | Voor financiële monitoring |
| Grootboekrekeningen | Dagelijks | Voor benamingen omzetcategoriën |
| Personal Training | Wekelijks | Handmatige invoer in Google Sheets |

## Technische Architectuur (Eenvoudig Uitgelegd)

Het systeem bestaat uit verschillende onderdelen die samenwerken:

### **Data Bronnen**
- **Gymly API** - Het hoofdsysteem met alle leden en financiële data
- **Google Sheets** - Voor Personal Training administratie

### **Verwerkingsstation**  
- **Automatische Scripts** - Software die elke dag draait
- **Data Transformatie** - Zet Engelse velden om naar Nederlandse namen en geeft juist datatypes mee
- **Validatie** - Controleert of alle gegevens correct zijn

### **Database**
- **Microsoft SQL Server** - Centrale opslagplaats
- **8 Tabellen** - Georganiseerd per gegevenstype
- **Backup & Beveiliging** - Veilige opslag van alle informatie

## Database Structuur (Voor Uw Administratie)

Alle gegevens worden opgeslagen in duidelijk benoemde tabellen:

| Database Tabel | Beschrijving | Aantal Records (geschat) |
|----------------|--------------|-------------------------|
| **Leden** | Alle ledeninformatie | 5.866 |
| **Abonnementen** | Abonnement types en prijzen | 137 |
| **AbonnementStatistieken** | Dagelijkse abonnement bewegingen | 365+ per jaar |
| **Lessen** | Les planning en bezetting | 16.790+ per jaar |
| **Omzet** | Dagelijkse omzet cijfers | 365+ per jaar |
| **GrootboekRekening** | Financiële categorieën | ~20 |
| **OpenstaandeFacturen** | Nog te betalen facturen | Variabel |
| **PersonalTraining** | PT sessies en uren | Variabel |

## Onderhoud & Monitoring

### **Automatische Controles**
- Dagelijkse health check van het systeem
- Automatische error detectie en reporting
- Backup verificatie

### **Logging & Rapportage**
- Uitgebreide logs van alle activiteiten
- E-mail notificaties bij problemen

### **Data Kwaliteit**
- Automatische validatie van alle gegevens
- Detectie van ongewone data patronen
- Consistentie checks tussen verschillende tabellen

## Volgende Stappen

1. **Start** van de ontwikkeling van bovenstaande systeem
2. **Ophalen** van data en beginnen met transformeren en verwerken
3. **Testen** en fine-tunen van het systeem
4. **Opzetten** van de PowerBI omgeving en het dashboard
5. **Feedback** verwerken van Tree11
6. **Go-live** van het systeem

---