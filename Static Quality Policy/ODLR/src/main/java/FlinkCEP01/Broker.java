package FlinkCEP01;

import java.io.Serializable;

public class Broker implements Serializable {
    String config;
    public Broker(String config){
        this.config = config;
    }

    public String getConfig() {
        return config;
    }
}
