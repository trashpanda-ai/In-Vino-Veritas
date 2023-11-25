# In Vino Veritas
University project on data engineering with Airflow.

# Current ToDos (sorted by priority):
1. Dockerize Airflow and Databases
    1. Design Airflow pipeline incl Docker and Postgres
    1. Include Scraper notebook as papermill operator
    1. Add offline fallbacks (If offline: load Parquet Files with the dedicated data; Else: API Call/Scrape)
       
1. Build methods to get data based on (Year+X)-Tupels:
    1. Method to generate Weather features based on (Year+Region)
       
1. Adapt new datamodel based on Ricardos Input (Star Schema? Ingestion at startup?)
       
1. Make Visualizations in Frontend Notebook
    1. Fix ANOVA Plot design and y-axis (F-score always 0-1 range?)
    1. Correlation heatmap between enrichment features and rating
    1. ANOVA between enrichment features and rating
    1. Combine all visualizations to a clean notebook with ToC (maybe automatically run?)
  
1. In depth Report/readme why architecture (expensive API calls etc)
1. Clean up folder structure incl outputs etc
1. Compare weather APIs (by number of NaNs and ANOVA): meteostat vs open meteo



# Workflow:

![alt text](https://github.com/trashpanda-ai/In_vino_veritas/blob/main/ressources/Flow%20Diagram.png?raw=true)

