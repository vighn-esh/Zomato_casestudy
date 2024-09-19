# Zomato Dataset ETL and Predictive Analysis


## Workflow

1. **Data Collection**: Raw data is gathered from various sources and consolidated into a single dataset.
2. **Staging**: The raw dataset is cleaned, validated, and loaded into a staging table in a SQLite database.
3. **Transformation**: Data transformations and exploratory data analysis (EDA) are performed to extract insights. Fact and dimension tables are created using a star schema approach and stored in MySQL.
4. **Loading**: The processed data is loaded into Power BI for dashboard creation and analysis.
5. **Predictive Analysis**: The final phase involves building predictive models to forecast restaurant ratings.
   
## Project Structure

- **Modular Code**:
  - **etl_pipeline.py**: Streamlined ETL pipeline. Run this script to execute the entire ETL process.
  - **extract.py**: Extracts data from flat files and APIs.
  - **staging.py**: Loads extracted data into the staging area in a SQLite database.
  - **transform.py**: Performs data transformation and creates the star schema needed for loading the data.
  - **load.py**: Loads the transformed data into SQLite for further analysis, including Power BI visualizations and predictive modeling.

   
## Tableu  Dash board
![Dashboard](https://github.com/user-attachments/assets/87a600fb-1e29-470a-8ed4-eb005b011856)






