package FlinkCEP01;

import java.io.Serializable;

public class DataPair implements Serializable {
    double value;
    int latency;
    Boolean loss; // true for lost event and false for not lost event

    public DataPair(){}


    public int getLatency() {
        return latency;
    }

    public void setLatency(int latency) {
        this.latency = latency;
    }

    public Boolean getLoss() {
        return loss;
    }

    public void setLoss(Boolean loss) {
        this.loss = loss;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
