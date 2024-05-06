namespace ETLProjectCSVToSQL.Queries;

public static class AnalysisQueries
{
    public static string GetPULocationWithHighestTip()
    {
        return @"SELECT TOP 1 PULocationID, AVG(tip_amount) AS AverageTip
                     FROM TaxiTrips
                     GROUP BY PULocationID
                     ORDER BY AverageTip DESC;";
    }

    public static string GetTop100LongestFaresByDistance()
    {
        return @"SELECT TOP 100 *
                     FROM TaxiTrips
                     ORDER BY trip_distance DESC;";
    }

    public static string GetTop100LongestFaresByTime()
    {
        return @"SELECT TOP 100 *, DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS TripDuration
                     FROM TaxiTrips
                     ORDER BY TripDuration DESC;";
    }

    public static string SearchByPULocation(int PULocationID)
    {
        return $@"SELECT * FROM TaxiTrips
                      WHERE PULocationID = @PULocationID;";
    }
}

