# Apache Airflow with Dbt Installation and Running Guide

## Project Description
This final project aims to build a comprehensive end-to-end data analytics pipeline utilizing 
NYC Taxi Trip Data. The pipeline is designed to demonstrate proficiency in workflow orchestration, data transformation, 
and generating insightful business intelligence through an interactive dashboard.

### Core Objectives
The analytical pipeline leverages a set of industry-standard tools to accomplish three main functions:

1. Data Workflow Orchestration (with Apache Airflow):
   - Utilizing Apache Airflow to define, schedule, and monitor the entire data workflow (DAG) from data extraction to transformation.
2. Data Transformation (with dbt - Data Build Tool):
   - Employing dbt (Data Build Tool) to build tested, reliable, and analytical-ready data models, turning raw data into structured business insights.
3. Data Visualization & Business Insights:
   - Developing an interactive dashboard to visualize the transformed data, enabling data exploration and the extraction of relevant business insights (e.g., trip pattern analysis, demand forecasting, or revenue analysis).

## Tech Stack

| Technology               | Version  |
|--------------------------|----------|
| Python                   | 3.11     |
| Dbt Duck DB              | 1.9.6    |
| Pandas                   | 2.2.2    |
| Dash                     | 3.2.0    |
| Dash Bootstrap Component | 2.0.4    |
| Pyarrow                  | 22.0.0   |
| Python Dotenv            | 1.2.1    |

## Installation

| <span style="color: #c42333; font-size: 25px;">⚠️  DANGER</span>                   |
|------------------------------------------------------------------------------------|
| Make sure you are in the root folder and please use python 3 <br> for installation |

Run this command to install .venv
```bash
python -m venv .venv
```

Now activate the .venv using this command, make sure you are using linux or macos, if you are using windows, 
don't forget to install wsl or any linux terminal such like ```git bash```
```bash
source .venv/bin/activate
```

Run this command on your terminal or command prompt to install all the requirements:
```bash
pip install -r requirements
```

Now copy the ```.env.example``` file into ```.env``` with this command
```bash
cp .env.example .env
```
and fill it with your own values, or keep the default values as it is


| <span style="color: #FFF58A; font-size: 25px;">⚠️  WARNING</span>                                   |
|-----------------------------------------------------------------------------------------------------|
| Make sure you set absolute path into .env file for path value and set schedule in .env as your wish |

## Server
Run this command to start the server:
```bash
airflow api-server -p 8080
```

and then open ```http://localhost:8080``` to access the airflow dashboard, and fill it with your username and password


Now run this command to make your dags available in the dashboard
```bash
airflow dag-processor
```

After you run ```dag-processor``` you need to run this command to start your scheduler
```bash
airflow scheduler
```

And now you can trigger your dags

## Dashboard
Open terminal in pages folder and run this command:
```bash
python app.py
```

And then open ```http://localhost:8050``` to access the dashboard. But remember dont deactive the venv to run that command