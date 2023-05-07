# NUFORC Airflow DAG

This project demonstrates how to use Apache Airflow to automate data extraction from the National UFO Reporting Center (NUFORC) API.

## Tools Used

This project uses the following tools:

- `Metabase`: An open-source business intelligence and analytics platform that allows you to visualize and analyze your data.
- `Amazon S3`: A cloud storage service provided by Amazon Web Services (AWS).
- `Apache Airflow`: A platform to programmatically author, schedule, and monitor workflows.
- `Docker`: A tool used to create and manage containers, which are lightweight, standalone executables that contain everything needed to run an application.
- `Postgres`: A popular open-source relational database management system.

## Setup

1. Rename the .env-template file to .env and fill in the environment variables with your AWS credentials and region name, as well as the name of the S3 bucket you want to use for storing data.

2. Build the Docker containers by running docker-compose build.

3. Start the containers by running docker-compose up -d.

4. Navigate to `http://localhost:8080` in your web browser to access the Airflow web interface.

5. From the Airflow web interface, enable the DAG named `nuforc_dag` by toggling the DAG switch to the "On" position.