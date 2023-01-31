package FlinkCEP01;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;




public class ProducerStart implements Runnable{
    ProducerEventTypeMatch pet;
    KafkaProducer kafkaProducer;

    public ProducerStart(ProducerEventTypeMatch pet) {
        this.pet = pet;
        this.kafkaProducer = pet.getP().kafkaProducer;
    }

    @Override
    public void run() {

        int startProcessingTime = (int)System.currentTimeMillis()/1000;




        //System.out.println(pet.getQ().getID() + " " + pet.getP().getID());

        try {

            while (pet.getQ().isActive()){

                Long currentTimeMilli = System.currentTimeMillis();
                int currentTimeSecond = (int)System.currentTimeMillis()/1000;

                if (currentTimeSecond - startProcessingTime > pet.getQ().getDuration()){
                    pet.getQ().setActive(false);
                    Producer.producerStop(pet);
                    //Thread.currentThread().stop();
                    break;
                }
                if ((currentTimeSecond -startProcessingTime) % pet.getP().getSensingInterval() == 0){
                    ArrayList<AttributeValue> data = new ArrayList<>();
                    DataPair newDP = Producer.CalculateData(pet);
                    data.add(new AttributeValue("ProducerID", pet.getP().getID()));
                    data.add(new AttributeValue("Type", pet.getP().getType()));
                    data.add(new AttributeValue("Value", ""+ newDP.getValue()));
                    data.add(new AttributeValue("GroundTruth", ""+ newDP.getValue()));
                    data.add(new AttributeValue("TimeStamp", ""+ currentTimeMilli));
                    data.add(new AttributeValue("SendingLatency", ""+ (pet.getP().getLatency() + newDP.getLatency())));
                    DataEvent dataEvent = new DataEvent(data);
                    System.out.println(dataEvent);
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(Query.setQueryTopic(pet.getQ().getID()), "key", dataEvent.toString());
                    kafkaProducer.send(record);

                }
                try {
                    Thread.currentThread().sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }catch (Exception e){

            System.out.println("Exception in thread " + pet.getQ().getID() + pet.getP().getID());
            throw new RuntimeException(e);
        }
    }
}
