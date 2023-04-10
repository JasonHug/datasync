package com.cnso.flinkcdc.deserialization;

import com.alibaba.fastjson.JSONObject;
import com.cnso.flinkcdc.common.constant.OperatorEnum;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.util.List;

import static com.cnso.flinkcdc.common.constant.MySqlBinlogConstant.AFTER;
import static com.cnso.flinkcdc.common.constant.MySqlBinlogConstant.BEFORE;
import static com.cnso.flinkcdc.common.constant.MySqlBinlogConstant.DATA;
import static com.cnso.flinkcdc.common.constant.MySqlBinlogConstant.DB;
import static com.cnso.flinkcdc.common.constant.MySqlBinlogConstant.OP;
import static com.cnso.flinkcdc.common.constant.MySqlBinlogConstant.SOURCE;
import static com.cnso.flinkcdc.common.constant.MySqlBinlogConstant.TABLE;
import static com.cnso.flinkcdc.common.constant.MySqlBinlogConstant.TS_MS;

/**
 * 对mysql-binlog进行反序列化
 *
 * 格式如下：
 * {"op":"CREATE","data":{"course_id":349,"course_grade":40.22999954223633,"course_name":"Database11","course_info":"MySQL11"},"db":"z_test","table":"tb_courses","opTime":1603123200000}
 */
public class CustomerDeserialization implements DebeziumDeserializationSchema<JSONObject> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<JSONObject> collector) throws Exception {
        JSONObject result = new JSONObject();

        Struct value = (Struct) sourceRecord.value();

        // 获取库表信息
        Struct source= value.getStruct(SOURCE);
        String db = source.getString(DB);
        String table = source.getString(TABLE);
        result.put(DB, db);
        result.put(TABLE, table);

        // 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        Field opField = value.schema().field("op");
        result.put(OP, OperatorEnum.forName(Envelope.Operation.forCode(value.getString(opField.name())).name()).code());

        // 获取数据
        switch (operation) {
            case READ:
            case CREATE:
            case UPDATE:
                // 获取after数据
                Struct after = value.getStruct(AFTER);
                JSONObject afterJson = new JSONObject();
                if (after != null) {
                    // 获取列信息
                    Schema schema = after.schema();
                    List<Field> fieldList = schema.fields();
                    fieldList.forEach(field -> afterJson.put(field.name(), after.get(field)));
                }
                result.put(DATA, afterJson);

                break;
            case DELETE:
            case TRUNCATE:
                // 获取before数据
                Struct before = value.getStruct(BEFORE);
                JSONObject beforeJson = new JSONObject();
                if (before != null) {
                    // 获取列信息
                    Schema schema = before.schema();
                    List<Field> fieldList = schema.fields();
                    fieldList.forEach(field -> beforeJson.put(field.name(), before.get(field)));
                }
                result.put(DATA, beforeJson);

                break;
            default:
                throw new IllegalStateException("Operation that does not exist!!!");
        }

        // 获取操作时间
        Long opTime = Long.valueOf(String.valueOf(value.get(TS_MS)));
        result.put(TS_MS, opTime);

        // 输出数据
        collector.collect(result);
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return BasicTypeInfo.of(JSONObject.class);
    }
}
