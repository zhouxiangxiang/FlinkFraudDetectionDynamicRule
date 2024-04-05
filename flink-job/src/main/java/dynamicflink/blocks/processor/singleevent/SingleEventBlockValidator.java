package dynamicflink.blocks.processor.singleevent;

import dynamicflink.blocks.Block;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

public class SingleEventBlockValidator {
    public static List<String> validate(Block block) {
        val validationErrors = new ArrayList<String>();

        if (block.getWindowSize() > 0) {
            validationErrors.add("WindowSize must not be set for Single Event Blocks.");
        }
        if (block.getWindowSlide() > 0) {
            validationErrors.add("WindowSlide must not be set for Single Event Blocks.");
        }

        val aggregationFields = block.getAggregationGroupingFields();
        if (aggregationFields != null && !aggregationFields.isEmpty()) {
            validationErrors.add("AggregationGroupingFields must not be set for Single Event Blocks.");
        }

        return validationErrors;
    }
}
