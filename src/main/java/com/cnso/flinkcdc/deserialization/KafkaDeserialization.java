package com.cnso.flinkcdc.deserialization;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaDeserialization implements KafkaDeserializationSchema<String> {
    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String topic = record.topic();
        long offset = record.offset();
        JSONObject valueJson = JSONObject.parseObject(record.value());
        valueJson.put("topic", topic);
        valueJson.put("offset", offset);

        return valueJson.toJSONString();
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
