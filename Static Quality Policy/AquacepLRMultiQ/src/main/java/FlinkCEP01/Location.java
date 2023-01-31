package FlinkCEP01;



import java.io.Serializable;

public class Location implements Serializable {
    double latitude, longitude;

    public Location() {

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

    public Location parseLocation(String s){
        String[] argsLoc = s.split(":");
        Location l=new Location();
        l.setLatitude(Double.parseDouble(argsLoc[0]));
        l.setLongitude(Double.parseDouble(argsLoc[1]));
        return l;
    }
}
