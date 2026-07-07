package realtime.common.util.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import realtime.bean.OrderStatusLog;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.function.Function;

public class KafkaSinkUtil {
    public interface SerializableFunction<T,R> extends Function<T,R>, Serializable{

    }
//    public static <T> KafkaSink<T> createSink(String topic,
//                                              Class<T> clazz,
//                                              SerializableFunction<T, Long> keyExtractor,
//                                              String transactionalIdPrefix){
//
//        SerializationSchema<T> valueSerializer = new SerializationSchema<T>() {
//
//            private transient ObjectMapper mapper;
//            @Override
//            public void open(InitializationContext context) throws Exception {
//                mapper=new ObjectMapper();
//            }
//            @Override
//            public byte[] serialize(T element) {
//                try {
//                    return mapper.writeValueAsBytes(element);
//                } catch (JsonProcessingException e) {
//                    throw new RuntimeException("Failed to serialize " + clazz.getSimpleName() + " ," + e);
//                }
//            }
//        };
//        // key序列化器
//        SerializationSchema<T> keySerializer = null;
//        if (keyExtractor !=null){
//            keySerializer = element ->{
//                Object key = keyExtractor.apply(element);
//                return key==null ? null : String.valueOf(key).getBytes(StandardCharsets.UTF_8);
//            };
//        }
//
//        KafkaRecordSerializationSchema<T> recordSerialization = KafkaRecordSerializationSchema.<T>builder()
//                .setTopic(topic)
//                .setValueSerializationSchema(valueSerializer)
//                .setKeySerializationSchema(keySerializer)
//                .build();
//
//
//        // 4. 配置生产者属性（如事务超时）
//        Properties props = new Properties();
//        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000");
//        // 构建kafkaSink
//        return KafkaSink.<T>builder().setBootstrapServers("192.168.94.200:29092")
//                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
//                        .setTopic(topic)
//                        .setValueSerializationSchema(valueSerializer)
//                        .setKeySerializationSchema(keySerializer).build())
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix(transactionalIdPrefix)
//                .setKafkaProducerConfig(props)
//                .build();
//    }
public static <T> KafkaSink<T> createSink(
        String topic,
        Class<T> clazz,
        SerializableFunction<T, Long> keyExtractor,
        String transactionalIdPrefix) {

    ObjectMapper mapper = new ObjectMapper();

    // TODO value序列化器
    SerializationSchema<T> valueSerializer = element -> {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    };

    SerializationSchema<T> keySerializer = element -> {
        if (keyExtractor == null) {
            return null;
        }
        Object key = keyExtractor.apply(element);
        return key == null ? null :
                String.valueOf(key).getBytes(StandardCharsets.UTF_8);
    };

    return KafkaSink.<T>builder()
            .setBootstrapServers("192.168.94.200:29092")
            .setRecordSerializer(
                    KafkaRecordSerializationSchema.<T>builder()
                            .setTopic(topic)
                            .setValueSerializationSchema(valueSerializer)
                            .setKeySerializationSchema(keySerializer)
                            .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setTransactionalIdPrefix(transactionalIdPrefix)
            .build();
}
    // 方法重载，针对无key时
    public static <T> KafkaSink<T> createSink(String topic,
                                              Class<T> clazz
                                              ,String transactionalIdPrefix){
        return createSink(topic,clazz,null,transactionalIdPrefix);

    }

    /*public static KafkaSink<OrderStatusLog> getOrderStatusSink(String topic) {
        // TODO 连接容器内kafka
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"600000");

        return KafkaSink.<OrderStatusLog>builder().
                setBootstrapServers("192.168.94.200:29092")
                // TODO 数据流反序列化以json格式存入kafka对应topic（即，将kafka作为dwd数据存储）
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(serializationSchema)
                                .setKeySerializationSchema(new SerializationSchema<OrderStatusLog>() {
                                    @Override
                                    public byte[] serialize(OrderStatusLog orderStatusLog) {
                                        return String.valueOf(orderStatusLog.getOrderId()).getBytes(StandardCharsets.UTF_8);
                                    }
                                }).build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) //设置传输EOS
                .setTransactionalIdPrefix("dwd_order_status_")//设置事务前缀，必须唯一
                .setKafkaProducerConfig(properties)//传入指定的生产者事务超时时间配置
                .build();
    }*/
}
