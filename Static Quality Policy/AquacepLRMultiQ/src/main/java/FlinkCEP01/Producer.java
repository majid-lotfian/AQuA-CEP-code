package FlinkCEP01;



import java.io.Reader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;


import org.apache.kafka.clients.producer.KafkaProducer;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


public class Producer implements Serializable {

    String ID, Type;
    long currentThreadID;

    public long getCurrentThreadID() {
        return currentThreadID;
    }

    public void setCurrentThreadID(long currentThreadID) {
        this.currentThreadID = currentThreadID;
    }

    public Properties prop_p;

    Location loc = new Location();
    boolean stop = false;


    int inUse = 0; //zero for not in use and one for in use
    double coverage;
    int SensingInterval;
    public static KafkaProducer kafkaProducer;
    ArrayList<QualityMetric> qmList = new ArrayList<>();

    double range;
    public static double maxError =3.0;
    public static double maxLatency = 2.0;

    public int offset = 0;

    public boolean isStop() {
        return stop;
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }
    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public Producer(){
        prop_p = new Properties();
        prop_p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Controller.broker1.getConfig());
        prop_p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop_p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducer = new KafkaProducer<>(prop_p);

    }


    public double getAccuracy(){
        for (QualityMetric qm:qmList) {
            if (qm.getMetricName().equals("Accuracy")){
                return qm.getMetricThreshold();
            }
        }
        return 0.0;

    }
    public void setAccuracy(double d){
        for (QualityMetric qm:qmList){
            if(qm.getMetricName().equals("Accuracy")){
                qm.setMetricThreshold(d);
            }
        }
    }
    public double getEnergyConsumption(){
        for (QualityMetric qm:qmList) {
            if (qm.getMetricName().equals("EnergyConsumption")){
                return qm.getMetricThreshold();
            }
        }
        return 0.0;

    }
    public void setEnergyConsumption(double d){
        for (QualityMetric qm:qmList){
            if(qm.getMetricName().equals("EnergyConsumption")){
                qm.setMetricThreshold(d);
            }
        }
    }
    public int getLatency(){
        for (QualityMetric qm:qmList) {
            if (qm.getMetricName().equals("Latency")){
                return (int)qm.getMetricThreshold();
            }
        }
        return 0;

    }
    public void setLatency(int d){
        for (QualityMetric qm:qmList){
            if(qm.getMetricName().equals("Latency")){
                qm.setMetricThreshold(d);
            }
        }
    }
    public double getLossRate(){
        for (QualityMetric qm:qmList) {
            if (qm.getMetricName().equals("LossRate")){
                return qm.getMetricThreshold();
            }
        }
        return 0.0;
    }
    public void setLossRate(double d){
        for (QualityMetric qm:qmList){
            if(qm.getMetricName().equals("LossRate")){
                qm.setMetricThreshold(d);
            }
        }
    }


    public static DataPair CalculateData(ProducerEventTypeMatch pet) {
        double pAccuracy=pet.getP().getAccuracy();
        double pEnergyConsumption = pet.getP().getEnergyConsumption();
        double pEventLossRate = pet.getP().getLossRate();
        int pLatency = pet.getP().getLatency();

        //create a random variation in producer's accuracy and loss rate
        Random random = new Random();

        int randomAccuracy = random.nextInt(100 );
        int randomLoss = random.nextInt(100);


        DataPair dp = new DataPair();
        String newGT = GenerateGroundTruthData(pet.getQ().getQueryOffset());


        System.out.println("query "+pet.getQ().getID()+ " offset : "+pet.getQ().getQueryOffset());
        if (Controller.DynamicLossRatePosition[pet.getQ().getQueryOffset()] == 1){
            double newLossRate = pet.getP().getLossRate()+ Controller.LossRatePenalty;
            pet.getP().setLossRate(newLossRate);
            //Controller.currentProducerLossRate = newLossRate;
            //System.out.println("loss rate in producer : "+pet.getP().getLossRate());
            //System.out.println("loss rate in se : "+pet.getQ().getEtList().get(0).getCurrentPET().getP().getLossRate());

        }
        if (Controller.DynamicLatencyPosition[pet.getQ().getQueryOffset()] == 1){
            pet.getP().setLatency(pet.getP().getLatency()+ Controller.LatencyPenalty);

        }

        //System.out.println("random loss : "+randomLoss+" producer loss rate : " +pEventLossRate);
        if (randomLoss >= pEventLossRate){
            if (randomAccuracy >=pAccuracy){
                double randomError = random.nextDouble()* maxError *2;
                double randValue =(randomError) + (Double.parseDouble(newGT)- maxError);
                dp.setValue(randValue);
            }
            if (randomAccuracy< pAccuracy){
                dp.setValue(Double.parseDouble(newGT));
                //System.out.println("new gt : " + newGT);
            }
            dp.setLoss(false);

            dp.setLatency(pet.getP().getLatency());
        }
        if (randomLoss < pEventLossRate){
            dp.setValue(0.0);
            dp.setLoss(true);
            dp.setLatency(0);
        }




        return dp;
    }

    public static KafkaProducer getKafkaProducer() {
        return kafkaProducer;
    }

    public static void setKafkaProducer(KafkaProducer kafkaProducer) {
        Producer.kafkaProducer = kafkaProducer;
    }

    public void producerStart(ProducerEventTypeMatch pet){
        //Controller.stopSendingData=false;
        long startProcessingTime = System.currentTimeMillis();

        try {

            //System.out.println("query is active : "+pet.getQ().isActive());
            while (pet.getQ().isActive() && !stop&& pet.getQ().getQueryOffset() < pet.getQ().getDuration()){

                long currentTime = System.currentTimeMillis();
                /*
                System.out.println("system time : "+ currentTime + " query start time : "+ pet.getQ().getStartTime() +
                        " minus : "+(currentTime - pet.getQ().getStartTime()));
                System.out.println("Query Duration : "+pet.getQ().getDuration());

                 */

                if ((currentTime - pet.getQ().getStartTime()) > ((pet.getQ().getDuration()+5)*1000)){
                    pet.getQ().setActive(false);
                    Producer.producerStop(pet);
                    //Thread.currentThread().stop();
                    break;

                }

                if ((currentTime -pet.getQ().getStartTime()) % pet.getP().getSensingInterval() == 0){

                    //System.out.println("ct : "+currentTimeSecond+" qst : "+pet.getQ().getStartTime());
                    ArrayList<AttributeValue> data = new ArrayList<>();
                    DataPair newDP = Producer.CalculateData(pet);
                    if (!newDP.getLoss()) {
                        data.add(new AttributeValue("ProducerID", pet.getP().getID()));
                        data.add(new AttributeValue("Type", pet.getP().getType()));
                        data.add(new AttributeValue("Value", "" + newDP.getValue()));
                        //data.add(new AttributeValue("GroundTruth", ""+ newDP.getGroundTruth()));
                        data.add(new AttributeValue("TimeStamp", "" + currentTime));
                        data.add(new AttributeValue("SendingLatency", "" +newDP.getLatency()));
                        data.add(new AttributeValue("loss", "" + newDP.getLoss()));
                        data.add(new AttributeValue("lossRate", "" + pet.getP().getLossRate()));

                        DataEvent dataEvent = new DataEvent(data);
                        System.out.println("event : " + dataEvent);
                        ProducerRecord<String, String> record = new ProducerRecord<String, String>
                                (pet.getQ().getID()+pet.getQ().getEtList().get(0).getEventType()+"Topic", "key", dataEvent.toString());
                        kafkaProducer.send(record);

                    }
                    if (newDP.getLoss()){
                        System.out.println("Lost event ................................!");
                        pet.getQ().setProducerLostEvents(pet.getQ().getProducerLostEvents()+1);
                        //Controller.producerLostEvents++;
                    }
                    //offset++;
                    //System.out.println("offset in producer : "+offset);
                    pet.getQ().setProducerOffset(pet.getQ().getProducerOffset()+1);
                    pet.getQ().setQueryOffset(pet.getQ().getQueryOffset()+1);

                    //Controller.producerOffset++;
                    //Controller.queryOffset++;

                    if(pet.getQ().getQueryOffset() == pet.getQ().getDuration()){
                        System.out.println("Query processed successfully");
                        pet.getQ().terminateQuery();
                    }
                }
                try {
                    Thread.currentThread().sleep(pet.getP().getSensingInterval()*1000);
                }
                catch (InterruptedException ignored) {
                //throw new RuntimeException(e);
                }
            }
        }catch (Exception e){

            System.out.println("Exception in thread " + pet.getQ().getID() + pet.getP().getID());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void producerStop(ProducerEventTypeMatch pet){

        pet.getP().getKafkaProducer().flush();
        pet.getP().getKafkaProducer().close();

    }

    public static void extractProducer(Reader fileReader) {
    }

    public ArrayList<QualityMetric> getQmList() {
        return qmList;
    }

    public void setQmList(ArrayList<QualityMetric> qmList) {
        this.qmList = qmList;
    }

    public static String setProducerTopic(ProducerEventTypeMatch pet){
        String topic = pet.getP().getID() + pet.getQ().getID() + "SE" + pet.getEt().getPositionInPattern();
        return topic;
    }

    public static String GenerateGroundTruthData(int i){



        String j = Controller.GroundTruthTemperature.get(i);

        return j;

    }


    public double getRange() {
        return range;
    }

    public void setRange(double range) {
        this.range = range;
    }

    public double getCoverage() {
        return coverage;
    }

    public void setCoverage(double coverage) {
        this.coverage = coverage;
    }

    public Location getLoc() {
        return loc;
    }

    public void setLoc(Location loc) {
        this.loc = loc;
    }

    public String getID() {
        return ID;
    }

    public String getType() {
        return Type;
    }

    public int getSensingInterval() {
        return SensingInterval;
    }


    public void setID(String ID) {
        this.ID = ID;
    }

    public void setType(String type) {
        this.Type = type;
    }


    public int getInUse() {
        return inUse;
    }

    public void setInUse(int inUse) {
        this.inUse = inUse;
    }


    public void setSensingInterval(int sensingInterval) {
        this.SensingInterval = sensingInterval;
    }

    public boolean inCoverage(Location loc){
        //boolean incoverage = false;
        //System.out.println();
        double euclideanDistance = Math.sqrt(
                Math.pow((this.loc.getLatitude()-loc.getLatitude()),2)+
                        Math.pow((this.loc.getLongitude()-loc.getLongitude()),2));
        //System.out.println("p "+this.ID+" coverage : "+this.coverage+" and distance is : "+euclideanDistance);
        if (this.coverage> euclideanDistance){
            return true;
        }
        return false;
    }

    public static void InitializeErrors(int duration) {
        //distribute loss rate dynamics
        Controller.DynamicLossRatePosition = new int[duration];
        for (int i=0; i<duration;i++ ){
            Controller.DynamicLossRatePosition[i]=0;
        }
        ArrayList<Integer> LRShuffleList = new ArrayList<>();
        for (int k=0; k<duration; k++){
            LRShuffleList.add(k);
        }
        Collections.shuffle(LRShuffleList);
        for (int j = 0; j<Controller.NumberofDynamicLossRate; j++){
            Controller.DynamicLossRatePosition[LRShuffleList.get(j)]=1;
            System.out.println("Dynamic loss is placed in : "+ LRShuffleList.get(j));
        }

        //distribute latency dynamics
        Controller.DynamicLatencyPosition = new int[duration];
        for (int i=0; i<duration;i++ ){
            Controller.DynamicLatencyPosition[i]=0;
        }
        ArrayList<Integer> LShuffleList = new ArrayList<>();
        for (int k=0; k<duration; k++){
            LShuffleList.add(k);
        }
        Collections.shuffle(LShuffleList);
        for (int j = 0; j<Controller.NumberofDynamicLatency; j++){
            Controller.DynamicLatencyPosition[LShuffleList.get(j)]=1;
            System.out.println("Dynamic latency is placed in : "+ LRShuffleList.get(j));
        }
    }


    public boolean meetQualityPolicy(QualityPolicy qualityPolicy){

        if (this.getType().equals(qualityPolicy.getType())){
            for (QualityMetric qm: this.qmList) {
                if (qm.getMetricName().equals(qualityPolicy.getQualityMetric().getMetricName())){
                    if (qualityPolicy.getQualityMetric().getOperator().equals(">")){
                        if (qm.getMetricThreshold() > qualityPolicy.getQualityMetric().getMetricThreshold()){
                            return true;
                        }
                    }
                    if (qualityPolicy.getQualityMetric().getOperator().equals("<")){
                        if (qm.getMetricThreshold() < qualityPolicy.getQualityMetric().getMetricThreshold()){
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

}
