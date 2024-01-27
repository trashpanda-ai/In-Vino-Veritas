<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/c139dcc66ea1348046c30373337151e14888f932/assets/header.png'>


# In Vino Veritas
An university project on data engineering with Airflow based on wine data 

# Current ToDos (sorted by priority):
1. Let it run online and pull a clean project and let it run offline

## Table of contents
- [Introduction](#introduction)
- [Data Sources](#data-sources)
  - [Main Data](#main-data)
  - [Enrichment Data](#enrichment-data)
- [Pipeline](#pipeline)
  - [Ingestion](#ingestion)
  - [Staging](#staging)
    - [Cleansing](#cleansing)
    - [Enrichments](#enrichments)
  - [Production](#production)
    - [Queries](#queries)
- [Future developments](#future-developments)
- [Results and Conclusion](#results-and-conclusion)
- [How to run?](#how-to-run)


# Introduction
In 'Vino Veritas' is an old Latin phrase that means 'in wine, there is truth'. And we would like to obtain said truth in wine. What is (subjectively/objectively)good wine? Do superstitions hold true? E.g., Are there countries with better wine? Are expensive wines better?	Are old wines better?	How does good wine taste and what chemical characteristics does it have?
Big data is already an integral part in the wine industry and readily used to gain insights. Our ultimate goal is to achieve this by obtaining and transforming relevant data in an efficient and coordinated manner before we analyze the data.
# Data Sources

## Main Data
For our objective perspective on data we utilize a static wine quality data set from 2009 which can be download as CSV or through an API.
Its target variable ```quality``` is judged through blind tasting by professional sommeliers and the wines underlying chemical properties are analyzed rendering the following features:

```
'fixed_acidity’, 'volatile_acidity’, 'citric_acid’,  'pH',  
'sulphates’,  'free_sulfur_dioxide',  'total_sulfur_dioxide’, 
'residual_sugar’, 'density’,  'alcohol', 'chlorides’
 ```

However, the backbone of our data sources is [vivino.com](vivino.com) -- A website based on users' ratings and entries. Including different dimensions from rating, region, vintage, grape variety, and taste profile in the ranges:
- Light – Bold
- Smooth – Tannic
- Dry – Sweet
- Soft – Acidic 
 The data is scraped with Python, thus rendering an already structured albeit uncleaned output.

## Enrichment Data

To enrich our data from with vivino we have multiple data sets. The first is global harvest data provided by the Food and Agriculture Organization Corporate Statistical Database (FAOSTAT). It can be downloaded as CSV or called through an API. The data is lagging 2 years, which means it currently ends at 2021. We assume the amount of (grape and wine) harvest is a strong indicator of agricultural conditions, but weather it also is a indicator for a subjectively good wine year is to be analyzed because quantity != quality.


Our second enrichment data leverages Google trends -- an analysis tool visualizing the popularity of search queries in Google Search. The reason we include it is the so called 'Sideways' effect:

<img src='https://i.imgur.com/KpG7hK7.png' width='550'>

In the 2005 movie 'Sideways', the grape variety 'Pinot Noir' was continuously presented as good wine, while the protagonist announced to not drink any more f****ng 'Merlot'. This resulted in a shift in consumer behavior. 
To show which wine was trending we utilize an unofficial Python API.

For the last dimension to enrich our vivino data, we obtain weather data based on the wines exact region. We follow a matryoshka approach of nesting an API in an API. The GPS location is extracted from the vivino.com region via Geopy. And then we can obtain a weather time series based on the GPS via meteostat. But a time series != feature. So we need to leverage expert domain knowledge to turn our raw time series in substantial features. Our feature engineering approach is based on [idealwine](https://www.idealwine.info/conditions-for-great-wine/) [.info](https://www.idealwine.info/conditions-necessary-great-wine-part-12/) for the growth period (March 11th -- September 20th) of the wines [Source](https://en.wikipedia.org/wiki/Harvest_(wine)):
> A fairly cold winter, a spring with alternating periods of sunshine and occasional rain, a fairly hot and dry summer with the odd shower to ward off water stress, and rain-free harvesting… conditions like these will virtually guarantee a good year.

> At temperatures below 10°C and above 35°C, photosynthesis will be disrupted, and vines will not grow properly. 

> Vines need between 400 and 600 mm of rain per year. […] A regular supply of water throughout the growth cycle is needed for a high-quality crop.

> Too much rain and damp during the May-July period can lead to the appearance of diseases such as mildew or oidium, which are caused by the growth of tiny fungi.

> If there is rain or […] strong wind during flowering, the pollen will be unable to achieve its task of pollinating the flowers. This is known as 'coulure'. 



# Pipeline
The overall pipeline is implemented in Apache Airflow and can be separated in three sections: Ingestion, Staging and Production.

<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/c2be436b872a6aac46134e6dec98e6b27f72358b/assets/Flow%20Diagram.png' width='750'>


Below is the detailed description of each step.
Each main task generates their own tables if they dont exist yet:
1. ```show_params```: Debug steps to later easily examine the given parameters to a certain run. 

1. ```pm_scrape```: Using the papermill operator and a jupyter notebook source file, the vivino website is scraped. The resulting pandas dataframe is converted into a parquet file and stored locally. In case of offline_mode usage, this step reads a predetermined .csv file instead scraping the website.

1. ```pm_clean```: With the papermill operator, the raw scraped data is passed through several steps where the dataframe is pruned from missing or incorrect data. Among these steps are:
    1. Dropping invalid regions. Based on a whitelist-blacklist system.
    1. Dropping entries with no vintage given.
    1. Changing column types to more appropiate ones.
1. ```postgres_connect```: this step ensures that a postgres type connection is added to the airflow connections list in case other scripts would like to connect to it (and if they need such method for that)
1. ```upload_postgres```: the cleaned data is read, and then line-by-line inserted into the database. If the wine already exists, no duplicate will be inserted.

The enrichment steps:

From the production database, 3 datastreams read their data for getting trend/harvest/weather data respectively.

1. ```get_region_and_year```: Retrieves every unique region-vintage pair from the ddatabase, then writes it into a parquet file for subsequent use by the enrich_weather task.
1. ```get_grape_and_year```: Similarly retrieves every grape_type-vintage combination from the database for use by the enrich_trends task.
1. ```get_country_and_year```: Every country-vintage pair for enrich_harvest.
1. ```enrich_weather```: Row-by-row inserts every region-year pair, then checks if they have any missing weather attributes. If yes, then it will try to update the cells with missing values. **If geopy cant fetch the location data for a region then it will be marked as a potentially invalid region**
1. ```enrich_trends```: Through the use of user cookies, google trends is queried for each grape_type-year pair where a value is missing for mean and median search trends. The success rate for this task varies a lot, depending on factors such as network traffic.
1. ```enrich_trends```: The faostat database is queried for each country-year pair in order to get the area of grape harvest, and the amount grape and wine made. There is no pre-check for existing values.
1. examine_regions: Regions marked as invalid are checked in the weather table to see if they have valied cells for other years. If yes, the mark is removed. This step is highly dependent on past data,and some wine regions have no clear association with weather stations or they do not posses an easily identifiable latitude/longitude.
1. ```region_cleaning```: Those rows in both weather and wines where no valid data is found the given regions are deleted.
1. ```finale```: If none of the tasks failed, the job is marked as a success.




## Ingestion
For ingestion, we utilize a Jupyter notebook within a PapermillOperator to save the scraped data as Parquet file as advised during our initial presentation. The scraping itself is based on the vivino's 'explore' section to find new wines. Since it would converge to 2000 items found, we included random restarts with additional parameters. One of the main parameters to maximize our results is the grape variety. It's saved as an integer ID whose distribution is non-linear. To adapt the notebook contains a number generator producing fitting ID's to scrape vivino. The amount of data to be scraped is parameterized in the notebook.

**The wine quality data set is already cleaned and readily available and only needs to be loaded and saved in a Postgres database.??????**

## Staging
The Staging area includes two main tasks: cleansing and enrichment.

### Cleansing
 Upon scraping, duplicates are removed, and wine entries with missing data in key areas dropped. Region data is examined and then the discarding happens based on a whitelist-blacklist system, where regions which might end up on the blacklist due to programming logic may still be included. This is necessary because the following steps in our pipeline can be very resource-intensive, rendering it necessary to reduce the amount of new tuples as much as possible.
  The scraped data is loaded from the Parquet file and entry-by-entry checked against the production data, inserting only those into production whose id is not already present there. 
 All production data itself has to be cleaned as well, since the list of rules is lagging by nature (based on Log files). This happens after all enrichment processes have concluded. Based on the feedback during our presentation, the database is now implemented in Postgres instead of MongoDB as the data is already structured.

### Enrichments
For the Google trends, our main obstacle is the unofficial API: basically a URL generator pasting the keyword and year into a URL template and using the ```request``` Python package to parse the result. The problem is the uninspired implementation of the latter, because one is immediately identified as a parser. Thus we had to come up with a fix: A connection set-up through manually created cookies
1. Login with credentials on [Google Trends](https://trends.google.com/trends/)
1. Activate network on browser in developer view 
1. Search for a keyword
1. Search for get GET package to that site and right-click ```copy as cURL```
1. Paste on website [curlconverter](https://curlconverter.com/python/) to get cookies and headers for Python ```request``` package
1. Paste in dedicated section in code



This could improve our success-rate in terms of stable connections from ~1% to ~25% - 60%. So we have to re-try for the tuples that can't be found. The API calls are still very slow, which is the reason we try to avoid it as much as possible. This is also the reason our parser tries to find only very similar wines in every execution, so the API calls can be minimal.
We obtain the unique (Grape + Year)-Tuples from our recently cleaned new scraped data and for each of those we generate the mean and median search frequency, which is then appended to the respective table for the production data.  

While wine can be consumed long after the year of the obtained trend, the vast majority of consumers follow a buy-and-drink approach instead of collecting [Source](http://winegourd.blogspot.com/2021/01/how-soon-is-wine-consumed-after-purchase.html). This is why we decided to match the trend with the production year. An easier and less data-intensive alternative would be to only include the current trend data. But since your ultimate goal is to be able to design and implement complex data pipelines we opted for the more difficult architecture.

  For the harvest data our approach is very straight forward: We obtain the unique (Country + Year)-Tuples from the production data and for each of those we retrieve the Grape Production Area and Amount and the Wine Production Amount. These three features are then appended to the respective table for the production data.

  The last enrichment data is the weather data: Since we aim for the exact weather data, the GPS location is extracted via ```Geopy``` from the vivino.com region instead of country. The nature of a user-based website leads unfortunately to inaccuracies. If a region cannot be found, we log the region to later include in the cleaning process (often the grape type is entered as region on vivino). 

  Due to the nature of Geopy, some regions may be deemed invalid even though they are valid wine regions, for this reason a whitelist is maintained, which collects regions for which geopy cannot find the location, but are otherwise valid wine regions.

 For the desired weather data, we average the $n$ closest stations' data for a more robust and granular time series.
  We obtain the unique (Region + Year)-Tuples from the wine production data and for each of those we generate the following specifically engineered features for the growth period (March 11th -- September 20th) of the wines:
- ```Vola_Temp```: Volatility of temperature
- ```Vola_Rain```: Volatility of rain
- ```Longest_Dry```: Longest period without rain
- ```Longest_Wet```: Longest period with consecutive rain
- ```Avg_Rain```: Average rain fall
- ```Count_above35```: Number of days with temperature above 35 degrees
- ```Count_under10```: Number of days with temperature below 10 degrees
- ```Count_under0 ```: Number of days with temperature below 0 degrees
- ```Coulure_Wind```: Windspeed during the flowering
- ```June_Rain```: Rain during the May-July period leading to mildew or oidium

These $10$ features are then appended to the respective table for the production data (the weather table).

## Production
Based on the input during our presentation, we switched from Neo4j to a more suitable star schema on Postgres where the vivino data is our fact table and the enrichment data are dimension tables:

<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/main/assets/Schema.png' width='550'>

### Queries
The queries are designed to leverage our star schema and combine the fact table with one of the dimension tables each to generate the necessary data for our insights. Since we can actually use and apply all the data and need to apply various transformations, we will not merge it immediately via SQL, but within the jupyter notebook to transform each dataframe independently. One of the first analysis renders various boxplots showing the connection between vintage, price and rating. We can see how the price and rating grows with vintage and also loses its variance. 

<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/plots/PNG/Price%20by%20Year.png' width='550'>
<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/plots/PNG/Rating%20by%20Year.png' width='550'>

We can also see that Rosé and White wine are significantly cheaper. We also see that the new world is leading the rankings .. on a US based website... ;)

<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/plots/PNG/Price%20by%20Wine%20Type.png' width='550'>
Notable: The outliers of sparkling wine (Champagne)!
<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/plots/PNG/Rating%20by%20Wine%20Type.png' width='550'>
<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/plots/PNG/Rating%20by%20Country.png' width='550'>



Furthermore the question how price and rating are depending on each other is interesting, because it depends on the kind of wine: For red wine it's a clear heteroscadacity without direct correlation, but sparkling wine shows a linear connection between price and ratings!

<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/plots/PNG/Rating%20by%20Price%20per%20Category.png' width='550'>

We dived deeper into our data and used the taste dimensions of our wines to find correlations by means of a heatmap:

<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/plots/PNG/Heatmap.png' width='550'>

To obtain more insights we also conduct a ANOVA test to see which variable explains the most variance in our data (here comparing red and sparkling wine):
<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/plots/PNG/ANOVA%20Rating%20Red%20Wine.png' width='550'>
This means, while for red wine the ```dry-sweet``` dimension is highly important, for the sparkling wine the price and number of reviews are relevant!
<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/plots/PNG/ANOVA%20Rating%20Sparkling%20Wine.png' width='550'>


We followed the same combined approach of heatmap and ANOVA test for our enrichment data:

<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/plots/PNG/Heatmap%20Enrichment.png' width='550'>


<img src='https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/plots/PNG/ANOVA%20Rating%20Enrichment.png' width='550'>

# Future developments
1. One can parse [lists](https://www.winespectator.com/vintage-charts) of 'officially good' wine years to select best weather features in terms of predictive modelling.
1. One can compare weather APIs (by number of NaNs and predictive qualities): meteostat vs open-meteo


# Results and Conclusion

The [Jupyter notebook](https://github.com/trashpanda-ai/In_vino_veritas/blob/92aa1b6e915e38e72849a14b9e039f30a75336fb/In%20vino%20veritas.ipynb) shows extremely interesting results! And even though the amount of data, we scraped is still not in the desired area of _Big Data_, we can conduct some insightful analysis.

Obstacles: 
- Data needs to be either cleaned iteratively by hand (manual lists of regions to be changed) or we have to make rules that lead to some data loss
- Parsing often ends in duplicates (resulting in constant database queries)
- High dimensional data and many features
- Obtaining sound and reliable weather data 

# How to run?
 Since the dockerization is mostly based on the default docker compose yaml file, the setup is quite similar. Run these commands in the folder with the ```docker-compose.yaml``` file
 1. ```docker compose up airflow-init``` (first time only)
 2. ```docker compose up```

 Then, connect to to the airflow localhost web UI through the appropriate port ```(8080)```, use the following credentials  and launch the dag:
| Login      | Password               |
| ---------- | ---------------------- |
| airflow    | airflow                |


 If you want direct acces to the database:
``` docker exec -it in_vino_veritas-postgres-1 psql -U airflow -d postgres```

Offline mode:
A static ```.csv``` file is included which can be read by enabling the ```offline_mode``` option when *manually triggering* the dag. This does not work on scheduled runs, as they will run with default settings.

Ports:

| Service    | URL                    |
| ---------- | ---------------------- |
| Airflow    | http://localhost:8080/ |
| PostGres   | http://localhost:5432/  |
