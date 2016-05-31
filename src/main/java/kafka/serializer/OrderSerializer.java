package kafka.serializer;

import kafka.message.ExchangeMessage.Order;
import org.apache.kafka.common.serialization.Serializer;

public class OrderSerializer extends Adapter implements Serializer<Order> {
    @Override
    public byte[] serialize(final String topic, final Order data) {
        return data.toByteArray();
    }
}