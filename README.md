# Title: Multinational Retail Data Centralisation

### Brief Description: 
In this project, I assume the role of a Data Analyst working for a multinational company that sells various goods across the globe. Their sales data was spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, it is imperative to make its sales data accessible from one centralised location. 

And so, my first goal was to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. Then I queried the database to get up-to-date metrics for the business.


### Extract and clean data from data sources:

- Create data_extraction.py script containing DataExtractor class. This works as a utility class by creating methods to help extract data from different data sources.

Create database_utils.py script containing DatabaseConnector class. This class is used to connect with and upload data to the database

Create data_cleaning.py script containing DataCleaning class to clean data of each data sources.
