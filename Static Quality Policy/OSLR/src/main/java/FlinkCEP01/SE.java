package FlinkCEP01;


import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class SE implements Serializable {

    String eventType;
    String queryID;
    Location loc= new Location();
    String operator; //operators can be (b:bigger) (s:smaller) (e:equal) (ne: not equal)
    String value;
    int positionInPattern;
    ProducerEventTypeMatch currentPET;
    DataStream<DataEvent> SEInput;
    DataStream<DataEvent> SEOutput;


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

    public Location getLoc() {
        return loc;
    }

    public void setLoc(Location loc) {
        this.loc = loc;
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
    public boolean checkThreshold(String operator, String eventValue, String SEValue){

        double doubleEventValue = Double.parseDouble(eventValue);
        double doubleSEValue = Double.parseDouble(SEValue);

        if (operator.equals(">") && doubleEventValue > doubleSEValue) {
            return true;
        }

        if (operator.equals("<") && doubleEventValue < doubleSEValue) {
            return true;
        }

        if (operator.equals("=") && doubleEventValue == doubleSEValue) {
            return true;
        }

        if (operator.equals("!=") && doubleEventValue != doubleSEValue) {
            return true;
        }

        return false;
    }

}
