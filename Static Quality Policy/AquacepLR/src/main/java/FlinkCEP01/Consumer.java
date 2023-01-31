package FlinkCEP01;



import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Consumer implements Serializable {
    public String ID;
    public KafkaConsumer kafkaConsumer;
    public Properties prop_c;



    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public Consumer() {
        prop_c = new Properties();
        prop_c.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Controller.broker1.getConfig());
        prop_c.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop_c.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop_c.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Group");
        prop_c.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        kafkaConsumer = new KafkaConsumer(prop_c);
    }

    public Consumer(String id) {
        this.ID = id;
        prop_c = new Properties();
        prop_c.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Controller.broker1.getConfig());
        prop_c.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop_c.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop_c.setProperty(ConsumerConfig.GROUP_ID_CONFIG, id);
        prop_c.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        kafkaConsumer = new KafkaConsumer(prop_c);
    }

    public KafkaConsumer getKafkaConsumer() {
        return this.kafkaConsumer;
    }

    public void setKafkaConsumer(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public void ConsumerStart(Query q){

        String topic = setConsumerTopic(q);


        //KafkaConsumer c11 = q.getConsumer(Controller.consumerList).getKafkaConsumer();
        this.kafkaConsumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records = this.kafkaConsumer.poll(Duration.ofSeconds(1));

        for (ConsumerRecord record : records) {
            //System.out.println("in consumer : "+record.value());
        }
    }

    public String setConsumerTopic(Query q) {
        String topic= q.getConsumerID()+ q.getID();

        return topic;
    }

    public static void ConsumerStop(ProducerEventTypeMatch pet){
        Consumer c1 = pet.getQ().getConsumer(Controller.consumerList);
        c1.kafkaConsumer.close();

    }
}

