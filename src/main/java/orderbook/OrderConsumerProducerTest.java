package orderbook;

public class OrderConsumerProducerTest {
    public static void main(String[] args) {
        OrderProducer producerThread = new OrderProducer("zz");
        producerThread.start();

        OrderConsummer consumerThread = new OrderConsummer("zz");
        consumerThread.start();

    }
}