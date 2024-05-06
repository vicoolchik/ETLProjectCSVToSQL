using CsvHelper;
using System.Globalization;
using ETLProjectCSVToSQL.Models;
using ETLProjectCSVToSQL.Helpers;

namespace ETLProjectCSVToSQL.Services;

public class CsvProcessor
{
    public List<TaxiTrip> ReadCsv(string inputPath, string duplicatesOutputPath, string errorOutputPath)
    {
        var taxiTrips = new List<TaxiTrip>();
        var errorRows = new List<(int RowNumber, string Record)>();
        var duplicates = new List<TaxiTrip>();
        var cleaner = new DataCleaner();
        var seenRecords = new HashSet<string>();

        using (var reader = new StreamReader(inputPath))
        using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
        {
            csv.Read();
            csv.ReadHeader();
            int rowNumber = 1;

            while (csv.Read())
            {
                rowNumber++;
                var parseResult = TryParseTrip(csv, cleaner);

                if (parseResult.IsSuccess)
                {
                    var trip = parseResult.Trip;
                    var uniqueKey = $"{trip.PickupDateTime}-{trip.DropoffDateTime}-{trip.PassengerCount}";
                    if (seenRecords.Contains(uniqueKey))
                    {
                        duplicates.Add(trip);
                    }
                    else
                    {
                        seenRecords.Add(uniqueKey);
                        taxiTrips.Add(trip);
                    }
                }
                else
                {
                    errorRows.Add((rowNumber, $"{csv.Parser.RawRecord}"));

                    var trip = CreateTripWithDefaults(csv, cleaner);
                    taxiTrips.Add(trip);
                }
            }
        }

        WriteErrors(errorRows, errorOutputPath);
        WriteDuplicates(duplicates, duplicatesOutputPath);

        return taxiTrips;
    }

    private (bool IsSuccess, TaxiTrip Trip) TryParseTrip(CsvReader csv, DataCleaner cleaner)
    {
        try
        {
            var estZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            var formats = new[] { "MM/dd/yyyy hh:mm:ss tt" };

            if (!DateTime.TryParseExact(csv.GetField("tpep_pickup_datetime"), formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var estPickupDateTime) ||
                !DateTime.TryParseExact(csv.GetField("tpep_dropoff_datetime"), formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var estDropoffDateTime))
            {
                throw new FormatException("Invalid date format");
            }

            var utcPickupDateTime = TimeZoneInfo.ConvertTimeToUtc(estPickupDateTime, estZone);
            var utcDropoffDateTime = TimeZoneInfo.ConvertTimeToUtc(estDropoffDateTime, estZone);

            var trip = new TaxiTrip
            {
                PickupDateTime = utcPickupDateTime,
                DropoffDateTime = utcDropoffDateTime,
                PassengerCount = csv.GetField<int>("passenger_count"),
                TripDistance = csv.GetField<double>("trip_distance"),
                StoreAndFwdFlag = cleaner.ConvertStoreAndFwdFlag(csv.GetField<string>("store_and_fwd_flag")),
                PULocationID = csv.GetField<int>("PULocationID"),
                DOLocationID = csv.GetField<int>("DOLocationID"),
                FareAmount = csv.GetField<decimal>("fare_amount"),
                TipAmount = csv.GetField<decimal>("tip_amount")
            };

            // if (!cleaner.ValidateData(trip))
            //     throw new InvalidOperationException("Invalid trip data");

            cleaner.TrimFields(trip);
            return (true, trip);
        }
        catch (Exception ex)
        {
            return (false, null);
        }
    }



    private TaxiTrip CreateTripWithDefaults(CsvReader csv, DataCleaner cleaner)
    {
        return new TaxiTrip
        {
            PickupDateTime = csv.GetField<DateTime?>("tpep_pickup_datetime") ?? default,
            DropoffDateTime = csv.GetField<DateTime?>("tpep_dropoff_datetime") ?? default,
            PassengerCount = csv.GetField<int?>("passenger_count") ?? 0,
            TripDistance = csv.GetField<double?>("trip_distance") ?? 0.0,
            StoreAndFwdFlag = cleaner.ConvertStoreAndFwdFlag(csv.GetField<string>("store_and_fwd_flag") ?? "No"),
            PULocationID = csv.GetField<int?>("PULocationID") ?? 0,
            DOLocationID = csv.GetField<int?>("DOLocationID") ?? 0,
            FareAmount = csv.GetField<decimal?>("fare_amount") ?? 0,
            TipAmount = csv.GetField<decimal?>("tip_amount") ?? 0
        };
    }

    private void WriteErrors(List<(int RowNumber, string Record)> errorRows, string errorOutputPath)
    {
        using (var writer = new StreamWriter(errorOutputPath))
        {
            writer.WriteLine("RowNumber,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,store_and_fwd_flag,PULocationID,DOLocationID,fare_amount,tip_amount");
            foreach (var (rowNumber, record) in errorRows)
            {
                writer.Write($"{rowNumber},{record}");
            }
        }
    }

    private void WriteDuplicates(List<TaxiTrip> duplicates, string duplicatesOutputPath)
    {
        using (var writer = new StreamWriter(duplicatesOutputPath))
        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
        {
            csv.WriteRecords(duplicates);
        }
    }
}

