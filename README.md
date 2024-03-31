# Objective

Accomplish the below two tasks:
- Task 1:
  - Fetch given page for a random number of files from a particular year.
  - Zip the randomly selected files to your work directory.
  - Delete unnecessary csv files and the html file.
- Task 2:
  - Unzip the file from task 1.
  - Calculate the monthly averages of Hourly data present in the csv files.
  - Using geopandas to visulise the calculated monthly averages of three fields.
  - Delete the csv files that were unzipped.

# Setup Instructions
- Make a new python environment.
- Install apache-airflow, apache-beam, pandas.
- Use conda to install geopandas.
- Run 'airflow scheduler' and 'airflow webserver' in two different terminal windows after you activate the created python environment.
- Type `http://localhost:8080/` into the web browser to run the dag files.
