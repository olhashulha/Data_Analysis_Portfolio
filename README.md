# Olha Shulha - Data Analyst Portfolio
## About
Hi, I am Olha! 

I am an aspiring data analyst with a foundation in SQL, Python, and Power BI. 
I worked on real-world projects, including data processing, verification, analysis, and visualization. 

My background includes working with statistical and laboratory data, performing validation, verification, and uncertainty analysis.

## Table of Contents
- [About](#about)
- [Portfolio Projects](#portfolio-projects)
  - Python
    - [Mobile configuration data analysis](https://github.com/olhashulha/Data_Analysis_Portfolio/blob/main/Mobile_statistics.ipynb)
  - SQL
    - [Mobile traffic distribution by region](https://github.com/olhashulha/Data_Analysis_Portfolio/blob/main/Mobile_statistics_calculation.sql)
  - Power BI
    - [Mobile statistics dashboard](https://app.powerbi.com/view?r=eyJrIjoiY2Q1MTI1ZWQtODJmYS00ZDA0LTlmMGYtOTAyNzkxZDE0ZWM1IiwidCI6IjZlYWE3ZDIyLWQyNzctNGFmOC05MzUzLTZlNzU3YTY5OTMwMSIsImMiOjl9&pageName=35121fd9a356635fbcb9)


## Portfolio Projects
## Mobile configuration data analysis
Code: [Mobile statistic.ipynb](Mobile_statistics.ipynb)

### 🎯 Goal:
To analyze mobile device configuration data in order to identify patterns, detect anomalies, and prepare clean, structured data for visualization.

### 🔧 Key Skills Demonstrated:
- Data Cleaning & Preprocessing:
    - Loaded and explored data from Excel files.
    - Handled missing values by imputation (mean/median) or removal.
    - Converted data types to ensure consistency and correct analysis.
- Data Validation & Transformation:
    - Validated value ranges for KPIs.
    - Flagged and handled outliers.
    - Created new calculated columns to enrich the dataset.
    - Built informative charts and plots to explore distributions and trends.
- Geocoding:
    - Used a geolocation API to convert coordinates into city names for regional analysis.

### 🛠️ Tools & Libraries:
`Python`, `Pandas`, `NumPy`, `Matplotlib`, `Seaborn`, `requests + external API`, `Jupyter Notebook`


## Mobile traffic distribution by region
Code: [Mobile traffic distribution by region](Mobile_statistics_calculation.sql)

### 🎯 Goal:
This SQL script analyzes mobile internet traffic across regions and calculates each region’s daily traffic share as a percentage of the total usage. It combines multiple data sources and applies advanced SQL techniques for analytical reporting.

### 🔧 Key Skills Demonstrated:
 - SQL data modeling and joining normalized tables\
 - Aggregation and grouping for time-series analysis\
 - Analytical window functions for percentage-of-total metrics
 - Use of a custom SQL function (f_div) for safe arithmetic
 - Clean formatting for presentation-ready outputs

### 🛠️ Tools Used:
`PostgreSQL`, `CTEs`, `JOIN`, `Aggregation`, `Grouping`, `Window functions`, `SQL view`, `SQL function`

## Mobile statistics dashboard
File: [Mobile statistic dashboard](Mobile_statistics_dashboard.pbix)

Web View: [Open in Power BI](https://app.powerbi.com/view?r=eyJrIjoiY2Q1MTI1ZWQtODJmYS00ZDA0LTlmMGYtOTAyNzkxZDE0ZWM1IiwidCI6IjZlYWE3ZDIyLWQyNzctNGFmOC05MzUzLTZlNzU3YTY5OTMwMSIsImMiOjl9&pageName=35121fd9a356635fbcb9)

### 🎯 Goal:
To build an interactive dashboard that visualizes mobile configuration and usage data, allowing users to explore trends, popular device models, and regional differences.

### 🔧 Key Skills Demonstrated:
- Data Modeling:
    - Structured data tables and defined relationships between them.
    - Created date and geography hierarchies to enable drill-down analysis.
- Data Transformation:
    - Cleaned and prepared raw data in Power Query Editor.
    - Applied filters, renamed fields, and created calculated columns.
- DAX Measures:
    - Created dynamic KPIs and measures to track:
    - Most popular mobile models and configurations
    - Regional device usage distribution
    - Device trends over time
    - Summary statistics and outlier insights
- Dashboard Design & Visualization:
    - Built a clean, interactive dashboard with:
    - Slicers (filters) for device, region, and time
    - Bar charts, line graphs, and KPI cards
    - Conditional formatting and tooltips for better context
- Interactivity:
    - Enabled dynamic filtering and cross-filtering for better data exploration.
    - Designed views for both overview and deep-dive insights.

### 🛠️ Tools Used:
`Power BI Desktop`, `Power Query`, `DAX`, `Data Modeling`, `Interactive Visuals`


## Certificates
- [Data Analytics Pro](https://certificate.ithillel.ua/view/91328559)

  Hillel IT School - Jan 2025

  ID: 91328559
