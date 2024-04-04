package dynamicflink.blocks.uniquethreshold;

import dynamicflink.blocks.*;
import dynamicflink.events.InputEvent;
import dynamicflink.functions.SafeFlatMapFunction;
import dynamicflink.windows.AggregatedEventProcessWindowFunction;
import dynamicflink.windows.MatchedEventAggregateFunction;
import dynamicflink.windows.SlidingWindowAssigner;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

@Slf4j
public class UniqueThresholdBlockProcessor implements BlockProcessor {
    private static final OutputTag<MatchedEvent> outputTag = new OutputTag<MatchedEvent>(BlockType.UNIQUE_THRESHOLD.toString()) {};

    @Override
    public DataStream<MatchedBlock> processEvents(SingleOutputStreamOperator<MatchedEvent> inputStream) {
        // Events matching Unique Value blocks are passed into a keyed window in order to be counted
        return inputStream.getSideOutput(outputTag)
                // Check that a value is present in the Unique Field, discard events where the field is not set
                .flatMap(new SafeFlatMapFunction<>((element, out) -> {
                    val uniqueField = element.getBlockParameters().get(BlockParameterKey.UniqueField);
                    val currentValue = new InputEvent(element.getEventContent()).getField(uniqueField);

                    if (currentValue != null && !currentValue.isEmpty()) {
                        out.collect(element);
                    }
                }, MatchedEvent.class))
                .name("unique-threshold-block-filter")
                .uid("unique-threshold-block-filter")
                .keyBy("customer", "matchedRuleId", "groupBy")
                .window(new SlidingWindowAssigner())
                .trigger(new UniqueThresholdWindowTrigger())
                .aggregate(new MatchedEventAggregateFunction(), new AggregatedEventProcessWindowFunction())
                .name("unique-threshold-block-aggregator")
                .uid("unique-threshold-block-aggregator")
                .flatMap(new SafeFlatMapFunction<>((block, out) -> {
                    block.setMatchMessage("Unique threshold exceeded.");
                    out.collect(block);
                }, MatchedBlock.class))
                .name("unique-threshold-block-mapper")
                .uid("unique-threshold-block-mapper");
    }
}
