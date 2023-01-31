package FlinkCEP01;



import java.io.Serializable;

public class Location implements Serializable {
    double latitude, longitude;

    public Location() {

    }

    public Location(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public void parseLocation(String s){
        String[] argsLoc = s.split(":");

        this.latitude=Double.parseDouble(argsLoc[0]);
        this.longitude = Double.parseDouble(argsLoc[1]);

    }
}
