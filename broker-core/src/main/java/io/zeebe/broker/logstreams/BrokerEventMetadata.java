package io.zeebe.broker.logstreams;

import static io.zeebe.protocol.clientapi.EventType.NULL_VAL;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import io.zeebe.broker.Constants;
import io.zeebe.protocol.clientapi.BrokerEventMetadataDecoder;
import io.zeebe.protocol.clientapi.BrokerEventMetadataEncoder;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.clientapi.MessageHeaderDecoder;
import io.zeebe.protocol.clientapi.MessageHeaderEncoder;
import io.zeebe.util.buffer.BufferReader;
import io.zeebe.util.buffer.BufferWriter;

public class BrokerEventMetadata implements BufferWriter, BufferReader
{
    public static final int ENCODED_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH +
            BrokerEventMetadataEncoder.BLOCK_LENGTH;

    protected final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    protected final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    protected BrokerEventMetadataEncoder encoder = new BrokerEventMetadataEncoder();
    protected BrokerEventMetadataDecoder decoder = new BrokerEventMetadataDecoder();

    protected int reqChannelId;
    protected long reqConnectionId;
    protected long reqRequestId;
    protected int raftTermId;
    protected long subscriberKey;
    protected int protocolVersion = Constants.PROTOCOL_VERSION; // always the current version by default
    protected EventType eventType = NULL_VAL;
    protected long incidentKey;

    @Override
    public void wrap(DirectBuffer buffer, int offset, int length)
    {
        reset();

        headerDecoder.wrap(buffer, offset);

        offset += headerDecoder.encodedLength();

        decoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

        reqChannelId = decoder.reqChannelId();
        reqConnectionId = decoder.reqConnectionId();
        reqRequestId = decoder.reqRequestId();
        raftTermId = decoder.raftTermId();
        subscriberKey = decoder.subscriptionId();
        protocolVersion = decoder.protocolVersion();
        eventType = decoder.eventType();
        incidentKey = decoder.incidentKey();
    }

    @Override
    public int getLength()
    {
        return ENCODED_LENGTH;
    }

    @Override
    public void write(MutableDirectBuffer buffer, int offset)
    {
        headerEncoder.wrap(buffer, offset);

        headerEncoder.blockLength(encoder.sbeBlockLength())
            .templateId(encoder.sbeTemplateId())
            .schemaId(encoder.sbeSchemaId())
            .version(encoder.sbeSchemaVersion());

        offset += headerEncoder.encodedLength();

        encoder.wrap(buffer, offset);

        encoder.reqChannelId(reqChannelId)
            .reqConnectionId(reqConnectionId)
            .reqRequestId(reqRequestId)
            .raftTermId(raftTermId)
            .subscriptionId(subscriberKey)
            .protocolVersion(protocolVersion)
            .eventType(eventType)
            .incidentKey(incidentKey);
    }

    public int getReqChannelId()
    {
        return reqChannelId;
    }

    public BrokerEventMetadata reqChannelId(int reqChannelId)
    {
        this.reqChannelId = reqChannelId;
        return this;
    }

    public long getReqConnectionId()
    {
        return reqConnectionId;
    }

    public BrokerEventMetadata reqConnectionId(long reqConnectionId)
    {
        this.reqConnectionId = reqConnectionId;
        return this;
    }

    public long getReqRequestId()
    {
        return reqRequestId;
    }

    public BrokerEventMetadata reqRequestId(long reqRequestId)
    {
        this.reqRequestId = reqRequestId;
        return this;
    }

    public int getRaftTermId()
    {
        return raftTermId;
    }

    public BrokerEventMetadata raftTermId(int raftTermId)
    {
        this.raftTermId = raftTermId;
        return this;
    }

    public long getSubscriberKey()
    {
        return subscriberKey;
    }

    public BrokerEventMetadata subscriberKey(long subscriberKey)
    {
        this.subscriberKey = subscriberKey;
        return this;
    }

    public BrokerEventMetadata protocolVersion(int protocolVersion)
    {
        this.protocolVersion = protocolVersion;
        return this;
    }

    public int getProtocolVersion()
    {
        return protocolVersion;
    }

    public EventType getEventType()
    {
        return eventType;
    }

    public BrokerEventMetadata eventType(EventType eventType)
    {
        this.eventType = eventType;
        return this;
    }

    public long getIncidentKey()
    {
        return incidentKey;
    }

    public BrokerEventMetadata incidentKey(long incidentKey)
    {
        this.incidentKey = incidentKey;
        return this;
    }

    public boolean hasIncidentKey()
    {
        return incidentKey != BrokerEventMetadataDecoder.incidentKeyNullValue();
    }

    public BrokerEventMetadata reset()
    {
        reqChannelId = BrokerEventMetadataEncoder.reqChannelIdNullValue();
        reqConnectionId = BrokerEventMetadataEncoder.reqConnectionIdNullValue();
        reqRequestId = BrokerEventMetadataDecoder.reqRequestIdNullValue();
        raftTermId = BrokerEventMetadataDecoder.raftTermIdNullValue();
        subscriberKey = BrokerEventMetadataDecoder.subscriptionIdNullValue();
        protocolVersion = Constants.PROTOCOL_VERSION;
        eventType = NULL_VAL;
        incidentKey = BrokerEventMetadataDecoder.incidentKeyNullValue();
        return this;
    }

    public boolean hasRequestMetadata()
    {
        return reqChannelId != BrokerEventMetadataEncoder.reqChannelIdNullValue() &&
                reqConnectionId != BrokerEventMetadataEncoder.reqConnectionIdNullValue() &&
                reqRequestId != BrokerEventMetadataDecoder.reqRequestIdNullValue();
    }
}