# Zomato Dataset ETL and Predictive Analysis

## Overview

This repository contains the code and documentation for performing the Extract, Transform, Load (ETL) process and predictive analysis on the Zomato dataset. The dataset includes comprehensive restaurant information gathered from multiple sources and enriched with additional features like latitude and longitude.

## Project Structure

- **Data**: Directory containing both raw and processed datasets.
- **merge_csv.py**: Script to merge multiple datasets from different directories into a single file.
- **Notebooks & Web Scraping Files**:
  - **scraping.ipynb**: Jupyter notebook that uses BeautifulSoup for web scraping, enriching the dataset with extra features such as latitude and longitude.
    - **data_enrichment.py**: Suitable for small datasets. For larger datasets, scraping each data point can be time-consuming.
    - **data_enrichment_with_save.py**: Optimized for larger datasets but may result in missing values in the scraped columns.
  - **Transform_Load&EDA.ipynb**: Handles data transformation, exploratory data analysis (EDA), and the creation of reporting tables in MySQL. The cleaned data is loaded into Power BI for dashboard visualization.
  - **predictive_analysis.ipynb**: Notebook that conducts predictive analysis on the dataset, including feature selection, preprocessing, model training (Linear Regression, Decision Tree Regression, Random Forest Regression), evaluation, and model selection.

- **Modular Code**:
  - **etl_pipeline.py**: Streamlined ETL pipeline. Run this script to execute the entire ETL process.
  - **extract.py**: Extracts data from flat files and APIs.
  - **staging.py**: Loads extracted data into the staging area in a MySQL database.
  - **transform.py**: Performs data transformation and creates the star schema needed for loading the data.
  - **load.py**: Loads the transformed data into MySQL for further analysis, including Power BI visualizations and predictive modeling.

## Workflow

1. **Data Collection**: Raw data is gathered from various sources and consolidated into a single dataset.
2. **Staging**: The raw dataset is cleaned, validated, and loaded into a staging table in a MySQL database.
3. **Transformation**: Data transformations and exploratory data analysis (EDA) are performed to extract insights. Fact and dimension tables are created using a star schema approach and stored in MySQL.
4. **Loading**: The processed data is loaded into Power BI for dashboard creation and analysis.
5. **Predictive Analysis**: The final phase involves building predictive models to forecast restaurant ratings. After testing different models (Linear Regression, Decision Tree Regression, Random Forest Regression), Decision Tree Regression is selected as the best-performing model.

## Conclusion

- The ETL process ensures that the data is clean, structured, and ready for analysis.
- Exploratory data analysis (EDA) provides valuable insights to guide decision-making.
- The predictive analysis helps in developing a robust model for restaurant rating prediction, with Decision Tree Regression selected as the optimal model.



