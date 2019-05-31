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
package io.zeebe.engine.util;

import static io.zeebe.engine.processor.TypedEventRegistry.EVENT_REGISTRY;
import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.zeebe.engine.processor.ReadonlyProcessingContext;
import io.zeebe.engine.processor.StreamProcessorLifecycleAware;
import io.zeebe.engine.processor.workflow.EngineProcessors;
import io.zeebe.engine.processor.workflow.deployment.distribute.DeploymentDistributor;
import io.zeebe.engine.processor.workflow.deployment.distribute.PendingDeploymentDistribution;
import io.zeebe.engine.processor.workflow.message.command.PartitionCommandSender;
import io.zeebe.engine.processor.workflow.message.command.SubscriptionCommandMessageHandler;
import io.zeebe.engine.processor.workflow.message.command.SubscriptionCommandSender;
import io.zeebe.engine.state.DefaultZeebeDbFactory;
import io.zeebe.exporter.api.record.Record;
import io.zeebe.exporter.api.record.value.DeploymentRecordValue;
import io.zeebe.exporter.api.record.value.JobRecordValue;
import io.zeebe.exporter.api.record.value.MessageRecordValue;
import io.zeebe.exporter.api.record.value.WorkflowInstanceCreationRecordValue;
import io.zeebe.exporter.api.record.value.deployment.ResourceType;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.impl.record.value.message.MessageRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceCreationRecord;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.protocol.intent.WorkflowInstanceCreationIntent;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import io.zeebe.util.ReflectUtil;
import io.zeebe.util.buffer.BufferWriter;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class EngineRule extends ExternalResource {

  private static final int PARTITION_ID = Protocol.DEPLOYMENT_PARTITION;
  private static final RecordingExporter RECORDING_EXPORTER = new RecordingExporter();
  public static final Duration DEFAULT_MSG_TTL = Duration.ofHours(1);

  protected final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();
  private final StreamProcessorRule environmentRule;

  private final int partitionCount;

  public EngineRule() {
    this(1);
  }

  public EngineRule(int partitionCount) {
    this.partitionCount = partitionCount;
    environmentRule =
        new StreamProcessorRule(
            PARTITION_ID, partitionCount, DefaultZeebeDbFactory.DEFAULT_DB_FACTORY);
  }

  @Override
  public Statement apply(Statement base, Description description) {
    Statement statement = recordingExporterTestWatcher.apply(base, description);
    statement = super.apply(statement, description);
    return environmentRule.apply(statement, description);
  }

  @Override
  protected void before() {

    final DeploymentRecord deploymentRecord = new DeploymentRecord();
    final UnsafeBuffer deploymentBuffer = new UnsafeBuffer(new byte[deploymentRecord.getLength()]);
    deploymentRecord.write(deploymentBuffer, 0);

    final PendingDeploymentDistribution deploymentDistribution =
        mock(PendingDeploymentDistribution.class);
    when(deploymentDistribution.getDeployment()).thenReturn(deploymentBuffer);

    forEachPartition(
        partitonId -> {
          final int currentPartitionId = partitonId;
          environmentRule.startTypedStreamProcessor(
              partitonId,
              (processingContext) ->
                  EngineProcessors.createEngineProcessors(
                          processingContext,
                          partitionCount,
                          new SubscriptionCommandSender(
                              currentPartitionId, new PartitionCommandSenderImpl()),
                          new DeploymentDistributionImpl())
                      .withListener(new ProcessingExporterTransistor()));
        });
  }

  public void forEachPartition(Consumer<Integer> partitionIdConsumer) {
    int partitionId = PARTITION_ID;
    for (int i = 0; i < partitionCount; i++) {
      partitionIdConsumer.accept(partitionId++);
    }
  }

  public Record<DeploymentRecordValue> deploy(final BpmnModelInstance modelInstance) {
    final DeploymentRecord deploymentRecord = new DeploymentRecord();
    deploymentRecord
        .resources()
        .add()
        .setResourceName(wrapString("process.bpmn"))
        .setResource(wrapString(Bpmn.convertToString(modelInstance)))
        .setResourceType(ResourceType.BPMN_XML);

    environmentRule.writeCommand(DeploymentIntent.CREATE, deploymentRecord);

    return RecordingExporter.deploymentRecords(DeploymentIntent.DISTRIBUTED)
        .withPartitionId(PARTITION_ID)
        .getFirst();
  }

  public Record<WorkflowInstanceCreationRecordValue> createWorkflowInstance(
      Function<WorkflowInstanceCreationRecord, WorkflowInstanceCreationRecord> transformer) {
    final long position =
        environmentRule.writeCommand(
            WorkflowInstanceCreationIntent.CREATE,
            transformer.apply(new WorkflowInstanceCreationRecord()));

    return RecordingExporter.workflowInstanceCreationRecords()
        .withIntent(WorkflowInstanceCreationIntent.CREATED)
        .withSourceRecordPosition(position)
        .getFirst();
  }

  public Record<JobRecordValue> completeJobOfType(String type) {
    final Record<JobRecordValue> createdEvent =
        RecordingExporter.jobRecords().withIntent(JobIntent.CREATED).withType(type).getFirst();

    final JobRecord jobRecord = new JobRecord();
    environmentRule.writeCommand(createdEvent.getKey(), JobIntent.COMPLETE, jobRecord);

    return RecordingExporter.jobRecords().withType(type).withIntent(JobIntent.COMPLETED).getFirst();
  }

  public Record<MessageRecordValue> publishMessage(
      int partitionId, String messageName, String correlationKey, DirectBuffer variables) {
    final MessageRecord messageRecord =
        new MessageRecord()
            .setName(messageName)
            .setCorrelationKey(correlationKey)
            .setVariables(variables)
            .setTimeToLive(DEFAULT_MSG_TTL.toMillis());

    environmentRule.writeCommandOnPartition(partitionId, MessageIntent.PUBLISH, messageRecord);

    return RecordingExporter.messageRecords(MessageIntent.PUBLISH)
        .withPartitionId(partitionId)
        .withCorrelationKey(correlationKey)
        .getFirst();
  }

  public List<Integer> getPartitionIds() {
    return IntStream.range(PARTITION_ID, PARTITION_ID + partitionCount)
        .boxed()
        .collect(Collectors.toList());
  }

  private class DeploymentDistributionImpl implements DeploymentDistributor {

    private final Map<Long, PendingDeploymentDistribution> pendingDeployments = new HashMap<>();

    @Override
    public ActorFuture<Void> pushDeployment(long key, long position, DirectBuffer buffer) {
      final PendingDeploymentDistribution pendingDeployment =
          new PendingDeploymentDistribution(buffer, position);
      pendingDeployments.put(key, pendingDeployment);

      forEachPartition(
          partitionId -> {
            if (partitionId == PARTITION_ID) {
              return;
            }

            final DeploymentRecord deploymentRecord = new DeploymentRecord();
            deploymentRecord.wrap(buffer);

            environmentRule.writeCommandOnPartition(
                partitionId, DeploymentIntent.CREATE, deploymentRecord);

            RecordingExporter.deploymentRecords(DeploymentIntent.CREATED)
                .withPartitionId(partitionId)
                .getFirst();
          });

      return CompletableActorFuture.completed(null);
    }

    @Override
    public PendingDeploymentDistribution removePendingDeployment(long key) {
      return pendingDeployments.remove(key);
    }
  }

  private class PartitionCommandSenderImpl implements PartitionCommandSender {

    private final SubscriptionCommandMessageHandler handler =
        new SubscriptionCommandMessageHandler(Runnable::run, environmentRule::getLogStream);

    @Override
    public boolean sendCommand(int receiverPartitionId, BufferWriter command) {

      final byte[] bytes = new byte[command.getLength()];
      final UnsafeBuffer commandBuffer = new UnsafeBuffer(bytes);
      command.write(commandBuffer, 0);

      handler.apply(bytes);
      return true;
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////// PROCESSOR EXPORTER CROSSOVER ///////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static class ProcessingExporterTransistor implements StreamProcessorLifecycleAware {

    private BufferedLogStreamReader logStreamReader;

    @Override
    public void onOpen(ReadonlyProcessingContext context) {
      final ActorControl actor = context.getActor();

      final ActorCondition onCommitCondition =
          actor.onCondition("on-commit", this::onNewEventCommitted);
      final LogStream logStream = context.getLogStream();
      logStream.registerOnCommitPositionUpdatedCondition(onCommitCondition);

      logStreamReader = new BufferedLogStreamReader(logStream);
    }

    private void onNewEventCommitted() {
      while (logStreamReader.hasNext()) {
        final LoggedEvent rawEvent = logStreamReader.next();

        final CopiedTypedEvent typedRecord = createCopiedEvent(rawEvent);

        Loggers.LOGSTREAMS_LOGGER.warn("Export: {}", typedRecord);
        RECORDING_EXPORTER.export(typedRecord);
      }
    }

    private CopiedTypedEvent createCopiedEvent(LoggedEvent rawEvent) {
      // we have to access the underlying buffer and copy the metadata and value bytes
      // otherwise next event will overwrite the event before, since UnpackedObject
      // and RecordMetadata has properties (buffers, StringProperty etc.) which only wraps the given
      // buffer instead of copying it

      final DirectBuffer contentBuffer = rawEvent.getValueBuffer();

      final byte[] metadataBytes = new byte[rawEvent.getMetadataLength()];
      contentBuffer.getBytes(rawEvent.getMetadataOffset(), metadataBytes);
      final DirectBuffer metadataBuffer = new UnsafeBuffer(metadataBytes);

      final RecordMetadata metadata = new RecordMetadata();
      metadata.wrap(metadataBuffer, 0, metadataBuffer.capacity());

      final byte[] valueBytes = new byte[rawEvent.getValueLength()];
      contentBuffer.getBytes(rawEvent.getValueOffset(), valueBytes);
      final DirectBuffer valueBuffer = new UnsafeBuffer(valueBytes);

      final UnifiedRecordValue recordValue =
          ReflectUtil.newInstance(EVENT_REGISTRY.get(metadata.getValueType()));
      recordValue.wrap(valueBuffer);

      return new CopiedTypedEvent(
          recordValue,
          metadata,
          rawEvent.getKey(),
          rawEvent.getPosition(),
          rawEvent.getSourceEventPosition());
    }
  }
}
