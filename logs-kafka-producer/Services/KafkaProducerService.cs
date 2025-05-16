using Confluent.Kafka;
using logs_kafka_producer.Configuration;
using logs_kafka_producer.Contracts;
using logs_kafka_producer.Models;
using logs_kafka_producer.Exceptions;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace logs_kafka_producer.Services;

public class KafkaProducerService : IKafkaProducerService, IDisposable
{
   private readonly IProducer<Null, string> _producer;
   private readonly string _topic;
   private readonly ILogger<KafkaProducerService> _logger;
   private readonly KafkaSettings _kafkaSettings;

   private static readonly JsonSerializerOptions _jsonSerializerOptions = new()
   {
      PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
   };

   public KafkaProducerService(IOptions<KafkaSettings> kafkaSettingsOptions, ILogger<KafkaProducerService> logger)
   {
      _logger = logger ?? throw new ArgumentNullException(nameof(logger));
      _kafkaSettings = kafkaSettingsOptions.Value;

      _topic = _kafkaSettings.TopicName;

      var producerConfig = new ProducerConfig
      {
         BootstrapServers = _kafkaSettings.ServerIpAddress,
         ClientId = _kafkaSettings.ClientId,
         Acks = Acks.All,
         MessageSendMaxRetries = 6,
         BatchSize = 16384,
         LingerMs = 10,
         CompressionType = CompressionType.Snappy,
         EnableIdempotence = true,
      };

      if (_kafkaSettings.SecurityProtocol != SecurityProtocol.Plaintext)
      {
         producerConfig.SecurityProtocol = _kafkaSettings.SecurityProtocol;

         if (_kafkaSettings.SecurityProtocol == SecurityProtocol.SaslPlaintext ||
                _kafkaSettings.SecurityProtocol == SecurityProtocol.SaslSsl)
         {
            
            if (string.IsNullOrWhiteSpace(_kafkaSettings.SaslUsername) ||
                string.IsNullOrWhiteSpace(_kafkaSettings.SaslPassword))
            {
               var errorMessage = $"SASL Username and Password are required for SecurityProtocol.";
               _logger.LogCritical(errorMessage);
               
               throw new InvalidOperationException(errorMessage + " Please check the configuration.");
            }

            producerConfig.SaslMechanism = _kafkaSettings.SaslMechanism;
            producerConfig.SaslUsername = _kafkaSettings.SaslUsername;
            producerConfig.SaslPassword = _kafkaSettings.SaslPassword;
         }
      }

      try
      {
         var builder = new ProducerBuilder<Null, string>(producerConfig)
             .SetErrorHandler((_, e) => _logger.LogError("Kafka Producer Error: {Reason} (Code: {Code}, IsFatal: {IsFatal})",
                                                         e.Reason, e.Code, e.IsFatal))
             .SetLogHandler((_, log) => _logger.LogInformation("Kafka Producer internal log [{Level}]: {Name} - {Message}",
                                                             log.Level, log.Name, log.Message));
         _producer = builder.Build();
         _logger.LogInformation("Producer Kafka initialized. Server: {Server}, Topic: {Topic}", _kafkaSettings.ServerIpAddress, _topic);
      }
      catch (Exception ex)
      {
         _logger.LogCritical(ex, "Unable to initialize Kafka producer.");
         throw new Exception("Kafka producer initialization failed. Check kafka configuration and availability.", ex);
      }
   }

   public async Task ProduceLogAsync(LogInput logInput)
   {
      if (logInput == null)
         throw new ArgumentNullException(nameof(logInput));
     
      string messageValue;
      try
      {
         messageValue = JsonSerializer.Serialize(logInput, _jsonSerializerOptions);
      }
      catch (JsonException ex)
      {
         _logger.LogError(ex, "LogInput serialization to JSON fails. LogInput ID: {LogId}", logInput.Id);
         throw new JsonException("Error serializing log message.", ex);
      }

      try
      {
         _logger.LogDebug("Generate message to Kafka topic : {Topic}. Message ID: {LogId}", _topic, logInput.Id);

         var result = await _producer.ProduceAsync(_topic, new Message<Null, string> { Value = messageValue });

         if (result.Status == PersistenceStatus.NotPersisted)
         {
            _logger.LogError("Kafka message persistence fails. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}, LogInput ID: {LogId}. Raison: {ErrorReason}", result.Topic, result.Partition, result.Offset, logInput.Id, result.Message?.Value);

            throw new KafkaProduceException($"Message Kafka (ID: {logInput.Id}) could not be persisted. Statut: {result.Status}");
         }
         else if (result.Status == PersistenceStatus.PossiblyPersisted)
         {
            _logger.LogWarning("The message Kafka (ID: {LogId}) has been possibly persisted. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                               logInput.Id, result.Topic, result.Partition, result.Offset);
         }
         else
         {
            _logger.LogInformation("Message (ID: {LogId}) successfully generated to Kafka. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                                   logInput.Id, result.Topic, result.Partition, result.Offset);
         }
      }
      catch (ProduceException<Null, string> e) 
      {
         _logger.LogError(e, "Kafka production failure to topic {Topic} for LogInput ID: {LogId}. Erreur Kafka: {KafkaErrorReason} (Code: {KafkaErrorCode})",
                          _topic, logInput.Id, e.Error.Reason, e.Error.Code);

         throw new KafkaProduceException($"Production failure on the Kafka topic '{_topic}' for LogInput ID: {logInput.Id}.", e);
      }
      catch (Exception ex)
      {
         _logger.LogError(ex, "Unexpected error when generating LogInput ID: {LogId} via Kafka to topic {Topic}..", logInput.Id, _topic);
         throw new Exception($"Unexpected error during message production (ID: {logInput.Id}).", ex);
      }
   }

   public void Dispose()
   {
      _logger.LogInformation("Cleaning and closing of the Kafka producer (Flush in progress...).");
      try
      {
         var messages = _producer?.Flush(TimeSpan.FromSeconds(10));
         if (messages > 0)
         {
            _logger.LogWarning("{Count} messages could not be sent before the Kafka producer closed.", messages);
         }
      }
      catch (ObjectDisposedException)
      {
         _logger.LogDebug("Producer Kafka was already prepared for Flush's attempt.");
      }
      catch (Exception ex) 
      {
         _logger.LogError(ex, "Flush error for producer Kafka.");
      }
      finally
      {
         _producer?.Dispose();
         _logger.LogInformation("Kafka producer closed.");
      }
      GC.SuppressFinalize(this);
   }
}
