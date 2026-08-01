Final Project Definition

Project Title

Large-Scale Timetable Consistency Analysis and Segment Runtime Anomaly Detection for Stagecoach South East Using PySpark

Problem Statement

Large public transport timetables contain thousands of journeys and timing links. Manually checking whether scheduled segment runtimes are consistent across routes, stops and operating periods is difficult.

The available Stagecoach South East timetable data contains planned journey information rather than observed bus-arrival data. Therefore, the project does not attempt to predict confirmed delay or service non-compliance. Instead, it investigates whether large-scale timetable data can be used to model expected scheduled segment runtime and identify records that differ substantially from normal timetable patterns.

Project Aim

The aim of this project is to design, implement and evaluate a PySpark-based predictive analytics system that models scheduled bus-segment runtime and identifies timetable-consistency anomalies from large-scale public transport data.

Project Objectives

Collect suitable BODS timetable data and official NaPTAN stop data.

Extract and construct a dataset containing more than 100,000 records.

Clean, transform and validate the data using PySpark DataFrames.

Use Spark SQL to analyse runtime, route and time-of-day patterns.

Join stop references with NaPTAN coordinates using an efficient Spark join strategy.

Engineer route, time and geographic features for predictive modelling.

Create a leakage-safe train/test split based on segment identity.

Train and compare at least three regression models.

Evaluate the models using RMSE, MAE and R².

Use prediction residuals to identify unusual scheduled segment runtimes.

Store processed records, model results and predictions in a relational database.

Demonstrate secure parameterised database queries.

Present the results through charts and an interactive dashboard.

Validate the completed system using automated tests.

Stakeholders

The main stakeholders are bus operators, local transport authorities, timetable planners and transport-data analysts.

They could use the system to identify segments that may require closer timetable review. The output should be treated as analytical support rather than proof of an operational problem.

Research Question

How effectively can route, time and geographic features predict scheduled bus-segment runtime, and how can prediction residuals be used to identify timetable-consistency anomalies?

Predictive Target

The target variable is:

runtime_minutes

This represents the scheduled runtime, in minutes, between two consecutive timetable stops. Regression is used because the target is continuous.

Anomaly Definition

After selecting the best regression model, the absolute prediction error is calculated for each test record.

A segment is flagged as a model-based anomaly when its absolute residual exceeds the threshold derived from the 95th percentile of the training-set absolute errors.

The anomaly label means that the scheduled runtime is unusual relative to the modelled expectation. It does not mean that the bus was delayed or that the timetable is objectively incorrect.

Data Scope

The project uses Stagecoach South East TransXChange timetable XML files from BODS and official NaPTAN stop data.

The final cleaned and enriched dataset contains 375,668 segment records.

Technical Scope

The project includes:

XML data extraction;

PySpark DataFrames;

Spark SQL;

at least four Spark partitions;

caching and persistence;

repartitioning where appropriate;

broadcast joins;

exploratory data analysis;

regression modelling;

model comparison;

residual-based anomaly detection;

Parquet outputs;

relational database design;

parameterised SQL queries;

Streamlit visualisation;

automated testing;

Git and GitHub version control.

Out of Scope

The project does not include:

live bus tracking;

real-time prediction;

actual arrival or departure observations;

passenger demand forecasting;

confirmed service-delay classification;

personal passenger information;

nationwide operator coverage.

Success Criteria

The project is considered successful when it:

processes more than 100,000 records;

uses PySpark for the main big-data pipeline;

demonstrates Spark SQL, partitioning, caching and join optimisation;

compares at least three machine-learning models;

reports RMSE, MAE and R²;

produces a reproducible database design;

uses secure parameterised queries;

provides visual evidence through charts and a dashboard;

passes automated system tests;

clearly explains the limitations of residual anomaly flags.

Final System Outcome

The completed system processes 445,788 raw timing-link records and produces 375,668 cleaned and enriched segment records.

Four regression models are compared. Gradient-Boosted Trees provides the best result with an RMSE of 0.668221 minutes, an MAE of 0.414582 minutes and an R² of 0.458255.

The selected model generates 72,832 test predictions. Using the training-derived residual threshold, 5,105 test records are flagged for further timetable-consistency review.

Ethical and Practical Considerations

The project uses public transport data and does not contain personal passenger information.

Model outputs may reflect existing timetable structures and route characteristics. They must not be presented as confirmed evidence of poor operator performance. Any flagged record should be reviewed using additional timetable-planning or operational information before decisions are made.