using logs_kafka_producer.Models;

namespace logs_kafka_producer.Contracts;

public interface IKafkaProducerService
{
   Task ProduceLogAsync(LogInput logInput);
}
