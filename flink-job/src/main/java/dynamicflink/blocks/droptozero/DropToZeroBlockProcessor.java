package dynamicflink.blocks.droptozero;

import dynamicflink.blocks.BlockProcessor;
import dynamicflink.blocks.BlockType;
import dynamicflink.blocks.MatchedBlock;
import dynamicflink.blocks.MatchedEvent;
import dynamicflink.windows.CountOutputWindowAssigner;
import dynamicflink.windows.TumblingWindowAssigner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import dynamicflink.blocks.functions.CountAggregateFunction;
import dynamicflink.blocks.functions.CountProcessWindowFunction;

public class DropToZeroBlockProcessor implements BlockProcessor {
    static final int ComparisonWindowCount = 10;

    private static final OutputTag<MatchedEvent> outputTag = new OutputTag<MatchedEvent>(BlockType.DROP_TO_ZERO.toString()) {};

    @Override
    public DataStream<MatchedBlock> processEvents(SingleOutputStreamOperator<MatchedEvent> inputStream) {
        return inputStream.getSideOutput(outputTag)
                // First, create a window to count the events
                .keyBy("customer", "matchedRuleId", "groupBy")
                .window(new TumblingWindowAssigner())
                .aggregate(new CountAggregateFunction(), new CountProcessWindowFunction())
                .name("drop-to-zero-block-aggregator")
                .uid("drop-to-zero-block-aggregator")
                // Then send the counts into a further window to detect a count of zero following continuous activity
                .keyBy("1.customer", "1.matchedRuleId", "1.groupBy")
                .window(new CountOutputWindowAssigner(ComparisonWindowCount))
                .process(new DropToZeroWindowProcessFunction())
                .name("drop-to-zero-block-processor")
                .uid("drop-to-zero-block-processor");
    }
}
