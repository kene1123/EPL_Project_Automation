# EPL Data ETL Automation Pipeline

An end-to-end data engineering project that collects live English Premier League (EPL) data, processes it using Python, stores it in PostgreSQL (on-prem and cloud), automates updates, and visualizes insights in Power BI.

---

## Project Overview

This project builds a complete **automated data pipeline** for EPL data including:

- League standings  
- Fixtures  
- Top goal scorers  

The system extracts live data from an external API, transforms it into structured formats, loads it into PostgreSQL databases, and keeps everything updated automatically using scheduled scripts.

---

## Objectives

- Build a real-time data ingestion pipeline for football data  
- Design and manage both local and cloud PostgreSQL databases  
- Automate daily data updates using Python and GitHub Actions  
- Ensure data consistency across environments  
- Enable reporting via Power BI dashboards  

---

## Tech Stack

- Python  
- REST API (Football Data Source)  
- PostgreSQL (On-Prem + Neon Cloud DB)  
- Git & GitHub  
- GitHub Actions (Automation)  
- Power BI (Reporting)  

---

## Data Source

Live EPL data is fetched from:

- football-data.org

Data endpoints used:
- Standings  
- Fixtures  
- Scorers  

---

## System Architecture

1. **Data Extraction**
   - API calls made using Python
   - JSON responses retrieved from football data source

2. **Data Transformation**
   - Cleaning and filtering raw API responses
   - Structuring data into relational format

3. **Data Loading**
   - Inserted into PostgreSQL (local database)
   - Migrated to cloud PostgreSQL (Neon)

4. **Automation**
   - Python scripts scheduled for daily execution
   - GitHub Actions used for cloud automation

5. **Reporting**
   - Power BI connected directly to PostgreSQL
   - Live dashboards updated via refresh

---

## Database Migration

Data was migrated from an on-prem PostgreSQL database to a cloud database using `dump.sql`.

### Validation checks:

| Table      | On-Prem DB        | Cloud DB          | Status |
|------------|------------------|------------------|--------|
| Standings  | 20 rows, 12 cols | 20 rows, 12 cols | ✅     |
| Scorers    | 10 rows, 6 cols  | 10 rows, 6 cols  | ✅     |
| Fixtures   | 380 rows, 8 cols | 380 rows, 8 cols | ✅     |

---

## Automation

### On-Prem Automation
- Python script runs scheduled updates locally
- Updates database daily with latest EPL data

### Cloud Automation
- GitHub Actions workflow runs once daily
- Secure environment variables used for database credentials
- Automatically updates Neon PostgreSQL database

---

## Reporting

Power BI is connected directly to the PostgreSQL database.

Dashboards include:
- EPL Standings  
- Top Scorers  
- Match Fixtures  

Data refresh is handled simply by reloading the dashboard.

---

## Security

- API keys stored in environment variables  
- Sensitive credentials excluded from GitHub using `.gitignore`  
- No hardcoded secrets in source code  

