using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;

namespace logs_kafka_producer.Configuration;

public record KafkaSettings
{
   public const string SectionName = "KafkaSettings";

   [Required(AllowEmptyStrings = false, ErrorMessage = "Kafka server address (BootstrapServer) is required.")]
   public string ServerIpAddress { get; init; } = string.Empty;
   
   [Required(AllowEmptyStrings = false, ErrorMessage = "Kafka topic name is required.")]
   public string TopicName { get; init; } = string.Empty;
   
   [Required(ErrorMessage = "Client ID is required.")]
   [MinLength(1, ErrorMessage = "Client ID cannot be empty.")]
   public string ClientId { get; set; } = Environment.MachineName;

   public string? SaslUsername { get; init; }

   public string? SaslPassword { get; init; }

   ///<summary>
   /// Protocole de sécurité : 
   /// - Plaintext (pas de chiffrement ni auth),
   /// - Ssl (SSL/TLS seulement),
   /// - SaslPlaintext (auth SASL sans chiffrement),
   /// - SaslSsl (auth SASL + chiffrement SSL/TLS).
   /// </summary>
   public SecurityProtocol SecurityProtocol { get; init; } = SecurityProtocol.SaslSsl;

   /// <summary>
   /// Mécanisme SASL :
   /// - Plain : SASL/PLAIN,
   /// - ScramSha256 : SCRAM-SHA-256,
   /// - ScramSha512 : SCRAM-SHA-512,
   /// - OauthBearer : OAuthBearer.
   /// </summary>
   public SaslMechanism SaslMechanism { get; init; } = SaslMechanism.ScramSha256;
}
