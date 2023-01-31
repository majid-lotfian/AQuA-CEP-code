package FlinkCEP01;

import org.apache.kafka.common.protocol.types.Field;

import java.io.Serializable;

public enum ProducerType implements Serializable {

    Temp("Temperature"),
    SmokeDetector("SmokeDetector");

    private final String type;
    private ProducerType(String value){
        this.type = value;
    }
    public String toString(){
        return this.type;
    }
}
