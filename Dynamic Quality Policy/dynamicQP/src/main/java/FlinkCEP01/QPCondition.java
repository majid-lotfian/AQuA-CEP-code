package FlinkCEP01;

public class QPCondition {
    String criterion;
    int minThreshold;
    int maxThreshold;
    public QPCondition(){}

    public QPCondition(String criterion, String operator, int minThreshold, int maxThreshold) {
        this.criterion = criterion;
        this.minThreshold = minThreshold;
        this.maxThreshold = maxThreshold;
    }


    public String getCriterion() {
        return criterion;
    }

    public void setCriterion(String criterion) {
        criterion = criterion;
    }

    public int getMinThreshold() {
        return minThreshold;
    }

    public void setMinThreshold(int minThreshold) {
        this.minThreshold = minThreshold;
    }

    public int getMaxThreshold() {
        return maxThreshold;
    }

    public void setMaxThreshold(int maxThreshold) {
        this.maxThreshold = maxThreshold;
    }
}
