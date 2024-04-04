package dynamicflink.blocks.singleevent;

import dynamicflink.blocks.BlockProcessor;
import dynamicflink.blocks.BlockType;
import dynamicflink.blocks.MatchedBlock;
import dynamicflink.blocks.MatchedEvent;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import dynamicflink.functions.SafeFlatMapFunction;

/**
 * Block which matches single events, then immediately produces an alert if all conditions are met.
 */
@Slf4j
public class SingleEventBlockProcessor implements BlockProcessor {
    private static final OutputTag<MatchedEvent> outputTag = new OutputTag<MatchedEvent>(BlockType.SINGLE_EVENT.toString()) {};

    @Override
    public DataStream<MatchedBlock> processEvents(SingleOutputStreamOperator<MatchedEvent> inputStream) {
        return inputStream.getSideOutput(outputTag)
                .flatMap(new SafeFlatMapFunction<>((matchedEvent, out) -> {
                    val matchedBlock = MatchedBlock.createFromMatchedEvent(
                            matchedEvent,
                            "An event matching the specified conditions was observed.");
                    out.collect(matchedBlock);
                }, MatchedBlock.class))
                .uid("single-event-block-mapper")
                .name("single-event-block-mapper");
    }
}
