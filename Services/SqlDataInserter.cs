using System.Data;
using System.Data.SqlClient;
using ETLProjectCSVToSQL.Models;

namespace ETLProjectCSVToSQL.Services;

public class SqlDataInserter
{
    private readonly string _connectionString;

    public SqlDataInserter(string connectionString)
    {
        _connectionString = connectionString;
    }

    public void BulkInsertTaxiTrips(List<TaxiTrip> taxiTrips)
    {
        using (var connection = new SqlConnection(_connectionString))
        {
            connection.Open();
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "TaxiTrips";

                bulkCopy.ColumnMappings.Add("tpep_pickup_datetime", "tpep_pickup_datetime");
                bulkCopy.ColumnMappings.Add("tpep_dropoff_datetime", "tpep_dropoff_datetime");
                bulkCopy.ColumnMappings.Add("passenger_count", "passenger_count");
                bulkCopy.ColumnMappings.Add("trip_distance", "trip_distance");
                bulkCopy.ColumnMappings.Add("store_and_fwd_flag", "store_and_fwd_flag");
                bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
                bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
                bulkCopy.ColumnMappings.Add("fare_amount", "fare_amount");
                bulkCopy.ColumnMappings.Add("tip_amount", "tip_amount");

                using (var dataTable = new DataTable())
                {
                    dataTable.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
                    dataTable.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
                    dataTable.Columns.Add("passenger_count", typeof(int));
                    dataTable.Columns.Add("trip_distance", typeof(double));
                    dataTable.Columns.Add("store_and_fwd_flag", typeof(string));
                    dataTable.Columns.Add("PULocationID", typeof(int));
                    dataTable.Columns.Add("DOLocationID", typeof(int));
                    dataTable.Columns.Add("fare_amount", typeof(decimal));
                    dataTable.Columns.Add("tip_amount", typeof(decimal));

                    foreach (var trip in taxiTrips)
                    {
                        var row = dataTable.NewRow();
                        row["tpep_pickup_datetime"] = trip.PickupDateTime;
                        row["tpep_dropoff_datetime"] = trip.DropoffDateTime;
                        row["passenger_count"] = trip.PassengerCount;
                        row["trip_distance"] = trip.TripDistance;
                        row["store_and_fwd_flag"] = trip.StoreAndFwdFlag;
                        row["PULocationID"] = trip.PULocationID;
                        row["DOLocationID"] = trip.DOLocationID;
                        row["fare_amount"] = trip.FareAmount;
                        row["tip_amount"] = trip.TipAmount;

                        dataTable.Rows.Add(row);
                    }

                    bulkCopy.WriteToServer(dataTable);
                }
            }
        }
    }
}
