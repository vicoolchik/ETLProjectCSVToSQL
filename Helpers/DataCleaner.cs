
using ETLProjectCSVToSQL.Models;

namespace ETLProjectCSVToSQL.Helpers;

public class DataCleaner
{
    public void TrimFields(TaxiTrip trip)
    {
        trip.StoreAndFwdFlag = trip.StoreAndFwdFlag.Trim();
    }

    public string ConvertStoreAndFwdFlag(string flag)
    {
        return flag.Trim() == "Y" ? "Yes" : "No";
    }

    public bool ValidateData(TaxiTrip trip)
    {
        if (trip.PickupDateTime > DateTime.UtcNow || trip.DropoffDateTime > DateTime.UtcNow)
            return false;

        if (trip.PassengerCount < 1 || trip.TripDistance < 0)
            return false;

        return true;
    }

}

