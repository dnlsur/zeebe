/*
 * Zeebe Workflow Engine
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.engine.processor.workflow.message;

import static io.zeebe.protocol.Protocol.START_PARTITION_ID;
import static io.zeebe.test.util.MsgPackUtil.asMsgPack;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class MessageCorrelationMultiplePartitionsTest {

  private static final String CORRELATION_KEY_PARTITION_0 = "item-2";
  private static final String CORRELATION_KEY_PARTITION_1 = "item-1";
  private static final String CORRELATION_KEY_PARTITION_2 = "item-0";

  private static final String PROCESS_ID = "process";

  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess(PROCESS_ID)
          .startEvent()
          .intermediateCatchEvent("receive-message")
          .message(m -> m.name("message").zeebeCorrelationKey("key"))
          .endEvent("end")
          .done();

  @Rule public EngineRule engine = new EngineRule(3);

  @Before
  public void init() {
    //    assertThat(getPartitionId(CORRELATION_KEY_PARTITION_0)).isEqualTo(START_PARTITION_ID);
    //    assertThat(getPartitionId(CORRELATION_KEY_PARTITION_1)).isEqualTo(START_PARTITION_ID + 1);
    //    assertThat(getPartitionId(CORRELATION_KEY_PARTITION_2)).isEqualTo(START_PARTITION_ID + 2);

    engine.deploy(WORKFLOW);
  }

  @Test
  public void shouldOpenMessageSubscriptionsOnDifferentPartitions() {
    // when
    IntStream.range(0, 10)
        .forEach(
            i -> {
              engine
                  .createWorkflowInstance(
                      r ->
                          r.setBpmnProcessId(PROCESS_ID)
                              .setVariables(asMsgPack("key", CORRELATION_KEY_PARTITION_0)))
                  .getKey();
              engine
                  .createWorkflowInstance(
                      r ->
                          r.setBpmnProcessId(PROCESS_ID)
                              .setVariables(asMsgPack("key", CORRELATION_KEY_PARTITION_1)))
                  .getKey();
              engine
                  .createWorkflowInstance(
                      r ->
                          r.setBpmnProcessId(PROCESS_ID)
                              .setVariables(asMsgPack("key", CORRELATION_KEY_PARTITION_2)))
                  .getKey();
            });

    // then
    assertThat(
            RecordingExporter.messageSubscriptionRecords(MessageSubscriptionIntent.OPENED)
                .limit(30))
        .extracting(r -> tuple(r.getMetadata().getPartitionId(), r.getValue().getCorrelationKey()))
        .containsOnly(
            tuple(START_PARTITION_ID, CORRELATION_KEY_PARTITION_0),
            tuple(START_PARTITION_ID + 1, CORRELATION_KEY_PARTITION_1),
            tuple(START_PARTITION_ID + 2, CORRELATION_KEY_PARTITION_2));
  }
  //
  //  @Test
  //  public void shouldCorrelateMessageOnDifferentPartitions() {
  //    // given
  //    engine
  //        .partitionClient(START_PARTITION_ID)
  //        .publishMessage("message", CORRELATION_KEY_PARTITION_0, asMsgPack("p", "p0"));
  //    apiRule
  //        .partitionClient(START_PARTITION_ID + 1)
  //        .publishMessage("message", CORRELATION_KEY_PARTITION_1, asMsgPack("p", "p1"));
  //    apiRule
  //        .partitionClient(START_PARTITION_ID + 2)
  //        .publishMessage("message", CORRELATION_KEY_PARTITION_2, asMsgPack("p", "p2"));
  //
  //    // when
  //    final long wfiKey1 =
  //        testClient
  //            .createWorkflowInstance(
  //                r3 ->
  //                    r3.setBpmnProcessId(PROCESS_ID)
  //                        .setVariables(asMsgPack("key", CORRELATION_KEY_PARTITION_0)))
  //            .getInstanceKey();
  //    final long wfiKey2 =
  //        testClient
  //            .createWorkflowInstance(
  //                r2 ->
  //                    r2.setBpmnProcessId(PROCESS_ID)
  //                        .setVariables(asMsgPack("key", CORRELATION_KEY_PARTITION_1)))
  //            .getInstanceKey();
  //    final long wfiKey3 =
  //        testClient
  //            .createWorkflowInstance(
  //                r1 ->
  //                    r1.setBpmnProcessId(PROCESS_ID)
  //                        .setVariables(asMsgPack("key", CORRELATION_KEY_PARTITION_2)))
  //            .getInstanceKey();
  //
  //    // then
  //    final List<String> correlatedValues =
  //        Arrays.asList(
  //            WorkflowInstances.getCurrentVariables(wfiKey1).get("p"),
  //            WorkflowInstances.getCurrentVariables(wfiKey2).get("p"),
  //            WorkflowInstances.getCurrentVariables(wfiKey3).get("p"));
  //
  //    assertThat(correlatedValues).contains("\"p0\"", "\"p1\"", "\"p2\"");
  //  }
  //
  //  private int getPartitionId(final String correlationKey) {
  //    final List<Integer> partitionIds = apiRule.getPartitionIds();
  //    return SubscriptionUtil.getSubscriptionPartitionId(
  //        BufferUtil.wrapString(correlationKey), partitionIds.size());
  //  }
}
