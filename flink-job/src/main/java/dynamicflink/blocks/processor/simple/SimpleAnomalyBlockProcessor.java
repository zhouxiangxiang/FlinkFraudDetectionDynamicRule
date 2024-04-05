package dynamicflink.blocks.processor.simple;

import dynamicflink.blocks.BlockProcessor;
import dynamicflink.blocks.BlockType;
import dynamicflink.blocks.MatchedBlock;
import dynamicflink.blocks.MatchedEvent;
import dynamicflink.blocks.functions.CountAggregateFunction;
import dynamicflink.blocks.functions.CountProcessWindowFunction;
import dynamicflink.windows.CountOutputWindowAssigner;
import dynamicflink.windows.TumblingWindowAssigner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

public class SimpleAnomalyBlockProcessor implements BlockProcessor {
    static final int ComparisonWindowCount = 10;

    private static final OutputTag<MatchedEvent> outputTag = new OutputTag<MatchedEvent>(BlockType.SIMPLE_ANOMALY.toString()) {
    };

    @Override
    public DataStream<MatchedBlock> processEvents(SingleOutputStreamOperator<MatchedEvent> inputStream) {
        return inputStream.getSideOutput(outputTag)
                // First, create a window to count the events
                .keyBy("customer", "matchedRuleId", "groupBy")
                .window(new TumblingWindowAssigner())
                .aggregate(new CountAggregateFunction(), new CountProcessWindowFunction())
                .uid("simple-anomaly-block-aggregator")
                .name("simple-anomaly-block-aggregator")
                // Then send the counts into a further window to detect major changes in the amount
                .keyBy("1.customer", "1.matchedRuleId", "1.groupBy")
                .window(new CountOutputWindowAssigner(ComparisonWindowCount))
                .process(new SimpleAnomalyWindowProcessFunction())
                .name("simple-anomaly-block-processor")
                .uid("simple-anomaly-block-processor");
    }
}
