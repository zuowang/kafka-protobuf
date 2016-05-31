package kafka.serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;

import kafka.message.ExchangeMessage.Order;

public class OrderDeserializer extends Adapter implements Deserializer<Order> {
    private static final Logger LOG = LoggerFactory.getLogger(OrderDeserializer.class);

    @Override
    public Order deserialize(final String topic, byte[] data) {
        try {
            return Order.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            LOG.error("Received unparseable message", e);
            throw new RuntimeException("Received unparseable message " + e.getMessage(), e);
        }
    }

}