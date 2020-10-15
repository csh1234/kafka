package comcsh.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class CustomerConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //配置信息
        props.put("bootstrap.servers", "localhost:9092");
        //消费者组id
        props.put("group.id", "test");
        //是否自动提交 offset
        props.put("enable.auto.commit", "true");
        //自动提交延时
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //指定topic
        consumer.subscribe(Arrays.asList("second", "first", "third"));
        while (true) {
            //获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record.topic() + "=======" + record.partition() + "=======" + record.value());
            }
        }

//        consumer.close();
    }
}
