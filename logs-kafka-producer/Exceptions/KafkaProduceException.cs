namespace logs_kafka_producer.Exceptions;

public class KafkaProduceException : Exception 
{
   public KafkaProduceException() { }
   public KafkaProduceException(string message) : base(message) { }
   public KafkaProduceException(string message, Exception innerException) : base(message, innerException) { }
}
