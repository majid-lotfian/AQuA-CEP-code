package FlinkCEP01;

import java.io.Serializable;

public class QualityPolicy implements Serializable {
    QualityMetric qualityMetric;
    String type;//like temperature

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
