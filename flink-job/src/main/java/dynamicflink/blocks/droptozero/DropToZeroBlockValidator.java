package dynamicflink.blocks.droptozero;

import dynamicflink.blocks.Block;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

public class DropToZeroBlockValidator {
    public static List<String> validate(Block block) {
        val validationErrors = new ArrayList<String>();

        if (block.getWindowSize() <= 0) {
            validationErrors.add("WindowSize was not set for Block.");
        }
        if (block.getWindowSlide() > 0) {
            validationErrors.add("WindowSlide is not supported for DropToZero blocks.");
        }

        return validationErrors;
    }
}
