package dynamicflink.windows;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

@Slf4j
public class TumblingWindowAssigner extends WindowAssigner<WindowDetailProvider, TimeWindow> {

    @Override
    public Collection<TimeWindow> assignWindows(WindowDetailProvider detailProvider, long timestamp, WindowAssignerContext context) {
        if (timestamp > Long.MIN_VALUE) {
            val windowSize = detailProvider.getWindowSize() * 1000;

            val start = TimeWindow.getWindowStartWithOffset(timestamp, 0, windowSize);
            return Collections.singletonList(new TimeWindow(start, start + windowSize));
        } else {
            log.error("Record has Long.MIN_VALUE timestamp (= no timestamp marker).");
            return Collections.emptyList();
        }
    }

    @Override
    public Trigger<WindowDetailProvider, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new GenericEventTimeTrigger<>();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}