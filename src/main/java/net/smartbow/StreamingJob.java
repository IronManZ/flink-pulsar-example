package net.smartbow;

import com.smartbow.sensormessages.SensorUpMessageOuterClass;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

public class StreamingJob {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * https://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */

        String serviceUrl = "pulsar://192.168.111.177:6650";
        String adminUrl = "http://192.168.111.112:8081";
        String topic = "persistent://lmk/lmk_ns/flink-test-vibration";

        PulsarSourceBuilder<SensorUpMessageOuterClass.SensorUpMessage> builder = PulsarSource.builder()
                .setServiceUrl(serviceUrl)
                .setAdminUrl(adminUrl)
                .setStartCursor(StartCursor.earliest())
                .setTopics(topic)
//                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(Schema.PROTOBUF_NATIVE(SensorUpMessageOuterClass.SensorUpMessage.class)))
                .setSubscriptionName("my-subscription")
                .setSubscriptionType(SubscriptionType.Exclusive);

        DataStreamSource<SensorUpMessageOuterClass.SensorUpMessage> dataStreamSource = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "Pulsar Source");

        dataStreamSource.print();
        env.execute();
    }
}
