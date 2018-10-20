package simpleKafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class JConsumer extends  Thread{
    private String topic;
    private final int SLEEP = 1000 * 3;
    private ConsumerConnector consumer;

    public JConsumer(String topic) {
        consumer = Consumer.createJavaConsumerConnector(this.consumerConfig());
        this.topic = topic;
    }

    private ConsumerConfig consumerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers",  ConfigureAPI.KafkaProperties.BROKER_LIST);
        props.put("zookeeper.connect", ConfigureAPI.KafkaProperties.ZK);
        props.put("group.id", ConfigureAPI.KafkaProperties.GROUP_ID);
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("enable.auto.commit", "false");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new ConsumerConfig(props);
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
        List<KafkaStream<String, String>> streams = consumerMap.get(topic);
        for (final KafkaStream<String, String> stream : streams) {
            ConsumerIterator<String, String> it = stream.iterator();
            while (it.hasNext()) {
                MessageAndMetadata<String, String> messageAndMetadata= it.next();
                System.out.println("Receive->[" + new String(messageAndMetadata.message()) + "],topic->["+messageAndMetadata.topic()
                        +"],offset->["+messageAndMetadata.offset()+"],partition->["+messageAndMetadata.partition()
                        +"],timestamp->["+messageAndMetadata.timestamp()+"]");
                consumer.commitOffsets();
                try {
                    sleep(SLEEP);
                } catch (Exception ex) {
                    consumer.commitOffsets();
                    ex.printStackTrace();
                }
            }
        }
        /*Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> messageAndMetadata= it.next();
                System.out.println("Receive->[" + new String(messageAndMetadata.message()) + "],topic->["+messageAndMetadata.topic()
                +"],offset->["+messageAndMetadata.offset()+"],partition->["+messageAndMetadata.partition()
                +"],timestamp->["+messageAndMetadata.timestamp()+"]");
                //consumer.commitOffsets();
                consumer.commitOffsets();
                try {
                    sleep(SLEEP);
                } catch (Exception ex) {
                    consumer.commitOffsets();
                    ex.printStackTrace();
                }
            }
        }*/
    }
}
