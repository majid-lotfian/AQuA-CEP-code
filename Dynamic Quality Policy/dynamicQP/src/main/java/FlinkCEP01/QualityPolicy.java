package FlinkCEP01;

import java.io.Serializable;

public class QualityPolicy implements Serializable {
    QualityMetric qualityMetric;
    QPCondition qpCondition;

    String type;//like temperature

    public QPCondition getQpCondition() {
        return qpCondition;
    }

    public void setQpCondition(QPCondition qpCondition) {
        this.qpCondition = qpCondition;
    }

    public QualityPolicy() {
    }

    public QualityMetric getQualityMetric() {
        return qualityMetric;
    }

    public void setQualityMetric(QualityMetric qualityMetric) {
        this.qualityMetric = qualityMetric;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
