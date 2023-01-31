package FlinkCEP01;

import java.io.Serializable;
import java.util.ArrayList;


public class Query implements Serializable {
    private String ID;
    private String consumerID;
    private ArrayList<QualityPolicy> qualityPolicy;
    private ArrayList<SE> etList;
    long startTime;
    private int issueTime;
    private int duration = 3600;
    private boolean active= false;
    private int patternDuration=0;

    //double wAccuracy;
    double wReuseFactor;
    double wEnergyConsumption;
    public double getwEnergyConsumption() {
        return wEnergyConsumption;
    }

    public void setwEnergyConsumption(double wEnergyConsumption) {
        this.wEnergyConsumption = wEnergyConsumption;
    }
    public double getwReuseFactor() {
        return wReuseFactor;
    }

    public void setwReuseFactor(double wEnergyConsumption) {
        this.wReuseFactor = wEnergyConsumption;
    }

    /*
    double wLossRate;

    double wLatency;

    public double getwAccuracy() {
        return wAccuracy;
   /

    public void setwAccuracy(double wAccuracy) {
        this.wAccuracy = wAccuracy;
    }



    public double getwLossRate() {
        return wLossRate;
    }

    public void setwLossRate(double wLossRate) {
        this.wLossRate = wLossRate;
    }

    public double getwLatency() {
        return wLatency;
    }

    public void setwLatency(double wLatency) {
        this.wLatency = wLatency;
    }

     */

    public int getPatternDuration() {
        return patternDuration;
    }

    public void setPatternDuration(int patternDuration) {
        this.patternDuration = patternDuration;
    }

    public int getIssueTime() {
        return issueTime;
    }

    public void setIssueTime(int issueTime) {
        this.issueTime = issueTime;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public String getID() {
        return ID;
    }

    public String getConsumerID() {
        return consumerID;
    }

    public ArrayList<QualityPolicy> getQualityPolicy() {
        return qualityPolicy;
    }

    public ArrayList<SE> getEtList() {
        return etList;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public void setConsumerID(String issuingUserID) {
        this.consumerID = issuingUserID;
    }

    public void setQualityPolicy(ArrayList<QualityPolicy> qualityPolicy) {
        this.qualityPolicy = qualityPolicy;
    }

    public void setEtList(ArrayList<SE> etList) {
        this.etList = etList;
    }

    public static String setQueryTopic(String s) {
        return s + "Topic";
    }

    public Consumer getConsumer(ArrayList<Consumer> consumerList){
        Consumer newConsumer = new Consumer();
        for (Consumer c: consumerList) {
            if (this.getConsumerID().equals(c.getID())){
                newConsumer = c;
                break;
            }
        }

        return newConsumer;
    }

    public void setQualityMetricsWeights(){
       // if (this.getQualityPolicy().isEmpty()){
            //this.wAccuracy = 0.25;
            this.wEnergyConsumption = 1.0;
            //this.wReuseFactor=0.5;
            //this.wLossRate = 0.25;
            //this.wLatency=0.25;
        /*
        }
        if (this.getQualityPolicy().size()==1){
            if (this.getQualityPolicy().get(0).getQualityMetric().getMetricName().equals("Accuracy")){
                //this.wAccuracy = 0.40;
                this.wEnergyConsumption = 0.20;
                //this.wLossRate = 0.20;
                //this.wLatency=0.20;
            }
            if (this.getQualityPolicy().get(0).getQualityMetric().getMetricName().equals("EnergyConsumption")){
                //this.wAccuracy = 0.20;
                this.wEnergyConsumption = 0.40;
                //this.wLossRate = 0.20;
                //this.wLatency=0.20;
            }
            if (this.getQualityPolicy().get(0).getQualityMetric().getMetricName().equals("LossRate")){
                //this.wAccuracy = 0.20;
                this.wEnergyConsumption = 0.20;
                //this.wLossRate = 0.40;
                //this.wLatency=0.20;
            }
            if (this.getQualityPolicy().get(0).getQualityMetric().getMetricName().equals("Latency")){
                //this.wAccuracy = 0.20;
                this.wEnergyConsumption = 0.20;
                //this.wLossRate = 0.20;
                //this.wLatency=0.40;
            }
        }
        //it is also possible to add more if_statements for more than one quality policy


         */
    }
}
