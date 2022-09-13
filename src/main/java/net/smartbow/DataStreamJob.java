/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.smartbow;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.smartbow.sensormessages.SensorUpMessageOuterClass;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

import java.nio.ByteBuffer;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
	public static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars);
	}

	public static void main(String[] args) throws Exception {

		Schema<KeyValue<String, ByteBuffer>> kvSchema = Schema.KeyValue(
				Schema.STRING,
				Schema.BYTEBUFFER,
				KeyValueEncodingType.SEPARATED
		);


		System.out.println("app start ...");
		//创建环境，设置参数
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String serviceUrl = "pulsar://192.168.111.177:6650";
		String adminUrl = "http://192.168.111.112:8081";
		String topic = "persistent://lmk/lmk_ns/flink-test-vibration";

//		PulsarSourceBuilder<SensorUpMessageOuterClass.SensorUpMessage> builder = PulsarSource.builder()
		PulsarSourceBuilder<byte[]> builder = PulsarSource.builder()
//		PulsarSourceBuilder<GenericRecord> builder = PulsarSource.builder()
				.setServiceUrl(serviceUrl)
				.setAdminUrl(adminUrl)
				.setStartCursor(StartCursor.earliest())
				.setTopics(topic)
//                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
//				.setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(Schema.PROTOBUF(SensorUpMessageOuterClass.SensorUpMessage.class), SensorUpMessageOuterClass.SensorUpMessage.class))
//				.setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(kvSchema, String.class, ByteBuffer.class))
				.setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(Schema.BYTES, byte[].class))
				.setSubscriptionName("my-subscription")
				.setSubscriptionType(SubscriptionType.Exclusive);

		DataStreamSource<byte[]> dataStreamSource = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "Pulsar Source");
//		DataStreamSource<GenericRecord> dataStreamSource = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "Pulsar Source");

//		dataStreamSource.map(l -> l.getKey()).print();
//		dataStreamSource.map(l -> bytesToHex(l.getValue().array())).print();
		dataStreamSource.map(DataStreamJob::bytesToHex).print();
		dataStreamSource.print();
		env.execute();
	}
}
