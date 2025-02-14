package dynamicflink;

import dynamicflink.blocks.BlockProcessor;
import dynamicflink.blocks.MatchedEvent;
import dynamicflink.windows.SlidingWindowAssigner;
import lombok.var;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import dynamicflink.blocks.MatchedBlock;
import dynamicflink.control.ControlOutput;
import dynamicflink.events.outputevents.OutputEvent;
import dynamicflink.control.ControlInput;
import dynamicflink.control.ControlInputWatermarkAssigner;
import dynamicflink.control.ControlOutputTag;
import dynamicflink.events.EventTimestampExtractor;
import dynamicflink.rules.Rule;
import lombok.val;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import dynamicflink.rules.RuleType;
import dynamicflink.rules.RuleTypeStreamSplitter;
import dynamicflink.rules.RuleWindowProcessFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Class containing a single method which connects the separate parts of the Dynamic Flink job.
 */
public class DynamicFlink {

    /**
     * Builds the Dynamic Flink job graph.
     *
     * @param eventStream     Provides events/messages to process
     * @param controlStream   Provides rules
     * @param outputEventSink Collects results of the job
     * @param controlOutput   Collects replies to control events
     * @param blocks          The block processors to be used in the job
     */
    public static void build(
            DataStream<String> eventStream,
            DataStream<ControlInput> controlStream,
            SinkFunction<OutputEvent> outputEventSink,
            SinkFunction<ControlOutput> controlOutput,
            List<BlockProcessor> blocks
    ) {
        // Assign timestamps based on the event time specified in the event
        val timestampedEventStream = eventStream.assignTimestampsAndWatermarks(
                new EventTimestampExtractor(Time.seconds(10)));

        // Set up the descriptor for storing the rules in managed state
        val controlInputStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                Types.STRING,
                Types.POJO(Rule.class));

        // Broadcast the control stream, so that rules are available at every node
        val controlBroadcastStream = controlStream
                .assignTimestampsAndWatermarks(new ControlInputWatermarkAssigner())
                .broadcast(controlInputStateDescriptor);

        // Send the events into the input function.  The matching event stream is then split based on the type of the
        // block which matched the event.
        SingleOutputStreamOperator<MatchedEvent> processFunctionOutput = timestampedEventStream
                .connect(controlBroadcastStream)
                .process(new InputBroadcastProcessFunction())
                .name("input-broadcast-processor")
                .uid("input-broadcast-processor");

        // Send control output to the appropriate stream
        processFunctionOutput
                .getSideOutput(ControlOutputTag.controlOutput)
                .addSink(controlOutput)
                .uid("control-sideoutput-sink")
                .name("control-sideoutput-sink");

        // complex processing
        val complexRuleOutput = new ArrayList<DataStream<MatchedBlock>>();
        // Send events to the block processors
        for (val block : blocks) {
            SingleOutputStreamOperator<MatchedBlock> processorOutput = block
                    .processEvents(processFunctionOutput)
                    .process(new RuleTypeStreamSplitter())
                    .name(String.format("%s-stream-splitter", block.getClass().getSimpleName()))
                    .uid(String.format("%s-stream-splitter", block.getClass().getSimpleName()));

            // Collect all the Simple rule matches (those with only a single block) and emit the alerts
            processorOutput
                    .getSideOutput(new OutputTag<MatchedBlock>(RuleType.SIMPLE.toString()) {})
                    .map(OutputEvent::createFromMatchedBlock)
                    .uid(String.format("%s-sideoutput-mapper", block.getClass().getSimpleName()))
                    .name(String.format("%s-sideoutput-mapper", block.getClass().getSimpleName()))
                    .addSink(outputEventSink)
                    .uid(String.format("%s-simple-rule-sink", block.getClass().getSimpleName()))
                    .name(String.format("%s-simple-rule-sink", block.getClass().getSimpleName()));

            // Collect all the complex rule matches
            complexRuleOutput.add(processorOutput.getSideOutput(new OutputTag<MatchedBlock>(RuleType.COMPLEX.toString()) {}));
        }

        // Union all the complex output
        var complexMatchStream = complexRuleOutput.get(0);
        for (int i = 1; i < complexRuleOutput.size(); i++) {
            complexMatchStream = complexMatchStream.union(complexRuleOutput.get(i));
        }

        complexMatchStream
                .keyBy("customer", "matchedRuleId", "groupBy")
                .window(new SlidingWindowAssigner())
                .process(new RuleWindowProcessFunction())
                .uid("rule-window-processor")
                .name("rule-window-processor")
                .addSink(outputEventSink)
                .uid("complex-rule-sink")
                .name("complex-rule-sink");
    }
}
