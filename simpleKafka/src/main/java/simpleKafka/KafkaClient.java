package simpleKafka;

public class KafkaClient {
    public static void main(String[] args) {
        JProducer pro = new JProducer(ConfigureAPI.KafkaProperties.TOPIC);
        pro.start();

        JConsumer con = new JConsumer(ConfigureAPI.KafkaProperties.TOPIC);
        con.start();
    }
}
