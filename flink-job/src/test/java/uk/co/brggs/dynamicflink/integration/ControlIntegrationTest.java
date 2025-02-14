package uk.co.brggs.dynamicflink.integration;

import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import uk.co.brggs.dynamicflink.TestEventGenerator;
import dynamicflink.blocks.Block;
import dynamicflink.blocks.BlockType;
import dynamicflink.blocks.conditions.*;
import dynamicflink.control.ControlInput;
import dynamicflink.control.ControlInputType;
import dynamicflink.control.ControlOutputStatus;
import uk.co.brggs.dynamicflink.integration.shared.IntegrationTestBase;
import uk.co.brggs.dynamicflink.integration.shared.IntegrationTestCluster;
import dynamicflink.rules.Rule;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class ControlIntegrationTest extends IntegrationTestBase {

    @Test
    void queryStatus_shouldReturnActiveRules() throws Exception {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("hostname", "importantLaptop"))
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);
        val controlInput = Arrays.asList(
                new ControlInput(ControlInputType.ADD_RULE, "rule_1", 1, ruleData),
                new ControlInput(ControlInputType.ADD_RULE, "rule_2", 1, ruleData),
                new ControlInput(ControlInputType.QUERY_STATUS));

        testCluster.run(controlInput, Collections.emptyList());

        val statusResponse = IntegrationTestCluster.ControlSink.values.get(2);

        assertThat(statusResponse.getStatus()).isEqualTo(ControlOutputStatus.STATUS_UPDATE);
    }
}