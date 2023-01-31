package FlinkCEP01;


import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class SE implements Serializable {

    String eventType;
    String queryID;
    Location DALocation= new Location();
    Location currentLocation = new Location();
    Location previousLocation = new Location();
    String operator; //operators can be (b:bigger) (s:smaller) (e:equal) (ne: not equal)
    String value;
    int positionInPattern;
    ProducerEventTypeMatch currentPET;
    DataStream<DataEvent> SEInput;
    DataStream<DataEvent> SEOutput;

    public Location getDALocation() {
        return DALocation;
    }

    public void setDALocation(Location DALocation) {
        this.DALocation = DALocation;
    }

    public Location getPreviousLocation() {
        return previousLocation;
    }

    public void setPreviousLocation(Location previousLocation) {
        this.previousLocation = previousLocation;
    }

    public String getQueryID() {
        return queryID;
    }

    public void setQueryID(String queryID) {
        this.queryID = queryID;
    }

    public DataStream<DataEvent> getSEOutput() {
        return SEOutput;
    }

    public void setSEOutput(DataStream<DataEvent> SEOutput) {
        this.SEOutput = SEOutput;
    }


    public DataStream<DataEvent> getSEInput() {
        return SEInput;
    }

    public void setSEInput(DataStream<DataEvent> SEInput) {
        this.SEInput = SEInput;
    }

    public ProducerEventTypeMatch getCurrentPET() {
        return currentPET;
    }

    public void setCurrentPET(ProducerEventTypeMatch currentPET) {
        this.currentPET = currentPET;
    }

    public int getPositionInPattern() {
        return positionInPattern;
    }

    public void setPositionInPattern(int positionInPattern) {
        this.positionInPattern = positionInPattern;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }


    public Location getCurrentLocation() {
        return currentLocation;
    }

    public void setCurrentLocation(Location location) {
        currentLocation = location;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public SE(){

    }
    public boolean checkThreshold(String eventValue){



        Location eventLocation = new Location();
        eventLocation.parseLocation(eventValue);
        double euclideanDistance = Math.sqrt(
                Math.pow((eventLocation.getLatitude()-this.getDALocation().getLatitude()),2)+
                        Math.pow((eventLocation.getLongitude()-this.getDALocation().getLongitude()),2));
        System.out.println("x:y = "+eventLocation.getLatitude()+":"+eventLocation.getLongitude()+" distance : "+euclideanDistance);



        if (euclideanDistance < Double.parseDouble(this.value)) {
            return true;
        }
        /*

        if (se.getOperator().equals("<") && doubleEventValue < doubleSEValue) {
            return true;
        }

        if (se.getOperator().equals("=") && doubleEventValue == doubleSEValue) {
            return true;
        }

        if (se.getOperator().equals("!=") && doubleEventValue != doubleSEValue) {
            return true;
        }
        */

        return false;
    }

}
