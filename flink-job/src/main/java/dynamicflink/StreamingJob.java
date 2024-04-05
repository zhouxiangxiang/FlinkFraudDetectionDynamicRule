package dynamicflink;

import dynamicflink.blocks.BlockProcessor;
import dynamicflink.blocks.droptozero.DropToZeroBlockProcessor;
import dynamicflink.blocks.processor.simple.SimpleAnomalyBlockProcessor;
import dynamicflink.blocks.processor.singleevent.SingleEventBlockProcessor;
import dynamicflink.blocks.processor.threshold.ThresholdBlockProcessor;
import dynamicflink.blocks.processor.uniquethreshold.UniqueThresholdBlockProcessor;
import dynamicflink.events.outputevents.OutputEventSerializationSchema;
import dynamicflink.control.ControlInputDeserializationSchema;
import dynamicflink.control.ControlOutputSerializationSchema;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Dynamic Flink Streaming Job
 */
@Slf4j
public class StreamingJob {
    public static void main(String[] args) throws Exception {
        val parameters = ParameterTool.fromArgs(args);

        // kafka
        val kafkaServer = parameters.get("kafkaServer", "localhost:9092");
        val kafkaGroup = parameters.get("kafkaGroup", "group");
        val inputTopic = parameters.get("inputTopic", "input-stream");
        val outputTopic = parameters.get("outputTopic", "output-stream");
        val controlTopic = parameters.get("controlTopic", "control-stream");
        val controlOutputTopic = parameters.get("controlOutputTopic", "control-output-stream");

        log.info("Starting {} Job v{}, connecting to Kafka at {}",
                StreamingJob.class.getPackage().getName(),
                StreamingJob.class.getPackage().getImplementationVersion(),
                kafkaServer);
        // Job env
        val env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        val kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", kafkaServer);
        kafkaConfig.setProperty("group.id", kafkaGroup);

        // stream
        val controlStream = env.addSource(
                new FlinkKafkaConsumer<>(controlTopic, new ControlInputDeserializationSchema(), kafkaConfig));

        val inputStream = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic, new SimpleStringSchema(), kafkaConfig));

        val outputStream = new FlinkKafkaProducer<>(
                kafkaServer,
                outputTopic,
                new OutputEventSerializationSchema()
        );

        val controlOutput = new FlinkKafkaProducer<>(
                kafkaServer,
                controlOutputTopic,
                new ControlOutputSerializationSchema()
        );

        // processor
        List<BlockProcessor> blocks = Arrays.asList(
                new SingleEventBlockProcessor(),
                new ThresholdBlockProcessor(),
                new UniqueThresholdBlockProcessor(),
                new SimpleAnomalyBlockProcessor(),
                new DropToZeroBlockProcessor()
        );

        // build stream graph
        DynamicFlink.build(inputStream, controlStream, outputStream, controlOutput, blocks);
        env.execute(StreamingJob.class.getPackage().getName());
    }
}
