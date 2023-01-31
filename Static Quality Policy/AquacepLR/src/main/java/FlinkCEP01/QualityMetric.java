package FlinkCEP01;

import java.io.Serializable;

public class QualityMetric implements Serializable {
    private String metricName;//like Accuracy
    private double metricThreshold;
    private String operator;// like > or < or =

    public QualityMetric(){

    }

    public String getMetricName() {
        return metricName;
    }

    public double getMetricThreshold() {
        return metricThreshold;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public void setMetricThreshold(double metricThreshold) {
        this.metricThreshold = metricThreshold;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }
}
