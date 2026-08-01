ST5011CEM Big Data Programming Project

Project Title

Large-Scale Timetable Consistency Analysis and Segment Runtime Anomaly Detection for Stagecoach South East Using PySpark

Module

Module name: Big Data Programming Project

Module code: ST5011CEM

Assessment type: Individual coursework

Project Overview

This project develops a complete predictive analytics system for analysing scheduled bus journey segments at large scale.

Stagecoach South East timetable data was extracted from TransXChange XML files published through the Bus Open Data Service (BODS). Official NaPTAN stop data was joined to the timetable records to add stop names and geographic coordinates.

PySpark was used for ingestion, cleaning, Spark SQL analysis, feature engineering and machine-learning preparation. Four regression models were compared to predict scheduled segment runtime. Large prediction residuals were then used to flag timetable-consistency anomalies.

The anomaly flags do not represent confirmed delays or real operational failures. They identify scheduled segment runtimes that differ substantially from the modelled expectation.

Main Results

169 timetable XML files processed

445,788 raw segment records extracted

375,668 cleaned and enriched segment records

5,143 timetable stops matched with NaPTAN coordinates

207 service lines represented

Four regression models compared

Gradient-Boosted Trees selected as the best model

Best-model RMSE: 0.668221 minutes

Best-model MAE: 0.414582 minutes

Best-model R²: 0.458255

72,832 test predictions generated

5,105 residual-based timetable anomaly flags

9 automated end-to-end system tests passed

Technologies Used

Python 3.11.9

Java 11

Apache Spark and PySpark 3.5.1

PySpark DataFrames

Spark SQL

PySpark MLlib

Pandas

PyArrow

Matplotlib

SQLite

Streamlit

Git and GitHub

Visual Studio Code

Data Sources

BODS timetable data

Stagecoach South East TransXChange timetable files were used to extract vehicle journeys, timing links, service-line references, stop references, departure times and scheduled segment runtimes.

NaPTAN stop data

Official NaPTAN data was used to add stop names, latitude and longitude for route and map visualisations.

No personal passenger information was used.

Project Workflow

Configure and verify the Spark environment.

Download and inspect timetable source files.

Extract vehicle-journey timing links from XML.

Ingest the extracted data using PySpark DataFrames.

Clean invalid and zero-runtime records.

Perform exploratory analysis with Spark SQL.

Join timetable stop references with NaPTAN data.

Engineer time, route and distance features.

Create a leakage-safe train/test split by segment key.

Train and compare four regression models.

Generate predictions and residual-based anomaly flags.

Store processed data and results in SQLite.

Demonstrate secure parameterised SQL queries.

Present findings through a Streamlit dashboard.

Run automated end-to-end system tests.

Machine-Learning Models

The following regression models were evaluated:

Linear Regression

Decision Tree Regressor

Random Forest Regressor

Gradient-Boosted Tree Regressor

Gradient-Boosted Trees achieved the lowest RMSE and was therefore selected as the best model.

Database

The SQLite database contains four main tables:

stops

segments

model_results

predictions

The database includes foreign-key constraints, indexes, integrity checks and parameterised queries. A SQL-injection-style test input returned zero unintended records and did not modify the database.

The generated database file is excluded from Git because it is a large local output. Reproducible schema and query files are stored in the repository.

Dashboard

The Streamlit dashboard provides:

project overview metrics;

route-level runtime exploration;

anomaly filtering;

stop and segment maps;

model comparison;

secure parameterised query demonstration.

Run the dashboard with:

python -m streamlit run .\src\dashboard_app.py

Project Structure

ST5011CEM_Bus_Compliance_Project/
├── config/                 Configuration templates
├── data/                   Raw, interim, processed and sample data
├── database/               SQLite schema, exports and local database
├── docs/                   Screenshots, diagrams and report materials
├── models/                 Model-related outputs
├── outputs/                Figures, metrics and predictions
├── sql/                    Parameterised SQL examples
├── src/                    Main Python and PySpark source files
├── tests/                  Automated system tests
├── .gitignore
├── README.md
└── requirements.txt

Environment Setup

Create the virtual environment once:

python -m venv .venv

Activate the existing environment:

.\.venv\Scripts\Activate.ps1

Install the required packages:

python -m pip install -r requirements.txt

Important Source Files

src/extract_timetable_segments.py

src/clean_segments.py

src/verify_cleaned_data.py

src/eda_and_sql_analysis.py

src/extract_stop_catalogue.py

src/download_naptan_coordinates.py

src/join_stops_and_engineer_features.py

src/train_regression_models.py

src/build_database.py

src/dashboard_app.py

tests/test_system.py

Testing

Run the automated tests with:

python -m unittest discover -s tests -v

The final system passed all nine automated tests.

Limitations

The project uses scheduled timetable data rather than observed vehicle arrival data.

Residual anomaly flags indicate timetable inconsistency, not confirmed delay.

Geographic distance is calculated between stop coordinates and does not represent exact road distance.

Model performance is moderate, so predictions should support analysis rather than replace expert timetable planning.

The dataset represents Stagecoach South East and may not generalise directly to other operators or regions.

Ethics and Data Protection

The project uses public transport timetable and stop-location data. It does not process names, contact details, payment information or other personal passenger data.

Results are presented carefully to avoid treating model anomalies as proven service failures. Model outputs should be interpreted alongside transport-planning knowledge and additional operational evidence.