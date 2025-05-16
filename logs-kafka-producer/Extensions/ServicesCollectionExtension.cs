using logs_kafka_producer.Contracts;
using logs_kafka_producer.Services;

namespace logs_kafka_producer.Extensions;

public static class ServicesCollectionExtension 
{
   public static IServiceCollection AddServices(this IServiceCollection services, IConfiguration configuration)
   {
      services.AddSingleton<IKafkaProducerService, KafkaProducerService>();

      return services;
   }
}
