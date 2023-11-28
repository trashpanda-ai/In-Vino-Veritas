# In Vino Veritas
An university project on data engineering with Airflow based on wine data 

# Current ToDos (sorted by priority):
1. Dockerize Airflow and Databases
    1. Design Airflow pipeline incl Docker and Postgres
    1. Add offline fallbacks (If offline: load Parquet Files with the dedicated data; Else: API Call/Scrape)
       
1. Adapt new datamodel based on Ricardos Input (Star Schema + Permanent Production Data)
       
1. Make Visualizations in Frontend Notebook
    1. Fix ANOVA Plot design and y-axis (F-score always 0-1 range?) (+ make sure no redundant code)
    1. Fix sorting of all boxplots (always high to low)
    1. Correlation heatmap between enrichment features and rating 
    1. ANOVA between enrichment features and rating
    1. Quick boxplot among wine categories (red, white, sparkling, rosé, dessert, port)
    1. Quick single scatter plot only for red-wine
    1. Combine all visualizations to a clean notebook with ToC (maybe automatically run?)
  
1. Finalize Report/readme with graphs and schemas
1. Add easy startup routine and put in [How to run?](#how-to-run)
1. Clean up folder structure incl outputs etc


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

TBD: Picture will be changed after data model is finalized
![alt text](https://github.com/trashpanda-ai/In_vino_veritas/blob/main/ressources/Flow%20Diagram.png?raw=true)

## Ingestion
For ingestion, we utilize a Jupyter notebook within a PapermillOperator to save the scraped data as Parquet file. The scraping itself is based on the vivino's 'explore' section to find new wines. Since it would converge to 2000 items found, we included random restarts with additional parameters. One of the main parameters to maximize our results is the grape variety. It's saved as an integer ID whose distribution is non-linear. To adapt the notebook contains a number generator producing fitting ID's to scrape vivino. The amount of data to be scraped is parameterized in the notebook.

The wine quality data set is already cleaned and readily available and only needs to be loaded and saved in a Postgres database.
## Staging
The Staging area includes two main tasks: cleansing and enrichment.

### Cleansing
The newly scraped data is loaded from the Parquet file and by application of a left join with the production data, we remove the already existing wines. This is necessary because the following steps in our pipeline can be very resource-intensive, rendering it necessary to reduce the amount of new tuples as much as possible.
The 'new' wines are then cleaned with a growing list of rules.
This cleaned data can then be appended to the production data. All production data itself has to be cleaned as well, since the list of rules is lagging by nature (based on Log files).

### Enrichments
For the Google trends, our main obstacle is the unofficial API: basically a URL generator pasting the keyword and year into a URL template and using the ```request``` Python package to parse the result. The problem is the uninspired implementation of the latter, because one is immediately identified as a parser. Thus we had to come up with a fix: A connection set-up through manually created cookies
1. Login with credentials on [Google Trends](https://trends.google.com/trends/)
1. Activate network on browser in developer view 
1. Search for a keyword
1. Search for get GET package to that site and right-click ```copy as cURL```
1. Paste on website [curlconverter](https://curlconverter.com/python/) to get cookies and headers for Python ```request``` package
1. Paste in dedicated section in code



This could improve our success-rate in terms of stable connections from ~5% to ~80%. The API calls are still very slow, which is the reason we try to avoid it as much as possible. This is also the reason our parser tries to find only very similar wines in every execution, so the API calls can be minimal.
We obtain the unique (Grape + Year)-Tuples from our recently cleaned new scraped data and for each of those we generate the mean and median search frequency, which is then appended to the respective table for the production data.

While wine can be consumed long after the year of the obtained trend, the vast majority of consumers follow a buy-and-drink approach instead of collecting [Source](http://winegourd.blogspot.com/2021/01/how-soon-is-wine-consumed-after-purchase.html). This is why we decided to match the trend with the production year. An easier and less data-intensive alternative would be to only include the current trend data. But since your ultimate goal is to be able to design and implement complex data pipelines we opted for the more difficult architecture.

For the harvest data our approach is very straight forward: We obtain the unique (Country + Year)-Tuples from our recently cleaned new scraped data and for each of those we retrieve the Grape Production Area and Amount and the Wine Production Amount. These three features are then appended to the respective table for the production data.

The last enrichment data is the weather data: Since we aim for the exact weather data, the GPS location is extracted via Geopy from the vivino.com region instead of country. The nature of a user-based website leads unfortunately to inaccuracies. If a region cannot be found, we log the region to later include in the cleaning process (often the grape variety is entered as region on vivino). Another failsafe against wrong coordinates is a check whether the found location matches the regions' dedicated country. This way our location results are very robust. For the desired weather data, we average the $n$ closest stations' data for a more robust and granular time series.
We obtain the unique (Region + Year + Country)-Tuples from our recently cleaned new scraped data and for each of those we generate the following specifically engineered features for the growth period (March 11th -- September 20th) of the wines:
- ```Vola_Temp```: Volatility of temperature
- ```Vola_Rain ```: Volatility of rain
- ```Longest_Dry ```: Longest period without rain
- ```Longest_Wet ```: Longest period with consecutive rain
- ```Avg_Rain```: Average rain fall
- ``` Count_above35```: Number of days with temperature above 35 degrees
- ```Count_under10 ```: Number of days with temperature below 10 degrees
- ```Count_under0 ```: Number of days with temperature below 0 degrees
- ``` Coulure_Wind```: Windspeed during the flowering
- ```June_Rain ```: Rain during the May-July period leading to mildew or oidium

These $10$ features are then appended to the respective table for the production data.

## Production
We opted for a star schema where the vivino data is our fact table and the enrichment data are dimension tables.
TBD insert Picture 

<img src='https://upload.wikimedia.org/wikipedia/commons/b/bb/Star-schema.png' width='550'>

### Queries
The queries are designed to leverage our star schema and combine the fact table with one of the dimension tables each to generate the necessary data for our insights. 
# Future developments

Use (and parse) lists of “officially good” wine years (e.g., https://www.winespectator.com/vintage-charts) to select best  weather features in terms of predictive models

- Compare weather APIs (by number of NaNs and ANOVA): meteostat vs open meteo






# Results and Conclusion

The [Jupyter notebook]() shows: ...


Data needs to be cleaned iteratively and parsing can easily end in duplicates

High dimensional data and many features

Obtaining sound and reliable weather data is more expensive than financial data?

# How to run?
TBD Marcell

```s
./start.sh
```

Ports:

| Service    | URL                    |
| ---------- | ---------------------- |
| Airflow    | http://localhost:8080/ |
| PostGres   | http://localhost:TBD/  |
