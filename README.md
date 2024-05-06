# ETL CSV to SQL Project

This project utilizes an ETL (Extract, Transform, Load) process to import data from CSV files into SQL Server, using Docker to orchestrate the database environment.

## Data Processing
- **TotalRecords:** 29,889(This represents the total number of records stored in the database.)
- **Duplicates:** 111
- **Errors:** 49  (These represent the number of records containing uninitialized fields, which were initialized with default values.)

## Recommendations

- Using uninitialized values can lead to future data integrity issues. It is recommended to implement validations for data validity and timeliness.
- By adding a validation for passenger count greater than one, the number of records with errors increases to 511.

## Docker Setup

### Running SQL Server Docker Image:

Execute the following command to create a Docker container with SQL Server, ready to use on port 1433.

```sh
docker run -d --name my-mssql-server -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=PASSWORD' -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest
```

## Importing Database Schema and Scripts

Use the following commands to manage the database schema:

### Creating the Database and Table
Run this command to import the database schema and create the necessary tables:
```sh
sqlcmd -S localhost,1433 -U sa -P PASSWORD -i "C:\\TestTask\\ETLProjectCSVToSQL\\Database\\Schema.sql"
```
### Optimizations for Handling Large CSV Files in My ETL Project

#### Streaming Data Instead of Loading Everything into Memory

- In my current project, I've identified that reading the entire CSV file into memory before processing it can be highly inefficient, especially with very large files. To improve this, I'm considering implementing data processing in batches. By using `IDataReader`, I can stream data directly from the CSV and pass it straight into `SqlBulkCopy`, significantly reducing memory usage.

#### Optimizing SqlBulkCopy

- I'm planning to use options such as `SqlBulkCopyOptions.TableLock`. This option locks the table during the bulk insert operation, which boosts performance considerably.
- I will adjust the `BatchSize` setting in `SqlBulkCopy` to better manage the number of rows processed simultaneously. This adjustment should help in minimizing transaction times and reducing the load on the SQL Server.

