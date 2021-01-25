package com.csh.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomerProducer {

    /**
     * 联系idea
     * @param args
     */
    public static void main(String[] args) {
        //配置信息
        Properties props = new Properties();
        //集群
        props.put("bootstrap.servers", "localhost:9092");
        //应答级别-all：不会丢数据
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        props.put("retries", 0);
        //批量大小
        props.put("batch.size", 16384);
        //提交延时
        props.put("linger.ms", 1);
        //缓存
        props.put("buffer.memory", 33554432);
        //传输的加密方式,k  v的序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建生产者
        Producer<String, String> producer = new KafkaProducer<>(props);

        //循环发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + "---------" + metadata.offset());
                    } else {
                        System.out.println("发送失败");
                    }
                }
            });
        }
        producer.close();
    }
}
