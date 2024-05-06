namespace ETLProjectCSVToSQL.Models;

public class TaxiTrip
{
    public DateTime PickupDateTime { get; set; }
    public DateTime DropoffDateTime { get; set; }
    public int PassengerCount { get; set; }
    public double TripDistance { get; set; }
    public string StoreAndFwdFlag { get; set; }
    public int PULocationID { get; set; }
    public int DOLocationID { get; set; }
    public decimal FareAmount { get; set; }
    public decimal TipAmount { get; set; }
}

