using ETLProjectCSVToSQL.Services;
using Microsoft.Extensions.Configuration;
using System.IO;

class Program
{
    static void Main()
    {
        string configPath = Path.Combine("C:\\TestTask\\ETLProjectCSVToSQL\\config", "appsettings.json");

        var builder = new ConfigurationBuilder()
                            .SetBasePath(Path.GetDirectoryName(configPath))
                            .AddJsonFile(Path.GetFileName(configPath), optional: true, reloadOnChange: true);

        IConfiguration config = builder.Build();

        string connectionString = config["ConnectionString"];
        string inputCsvPath = config["CsvFilePath"];
        string duplicatesCsvPath = config["DuplicatesCsvPath"];
        string errorCsvPath = config["ErrorCsvPath"];

        var csvProcessor = new CsvProcessor();
        var trips = csvProcessor.ReadCsv(inputCsvPath, duplicatesCsvPath, errorCsvPath);

        var sqlInserter = new SqlDataInserter(connectionString);
        sqlInserter.BulkInsertTaxiTrips(trips);

        Console.WriteLine($"Total records inserted: {trips.Count}");
    }
}
