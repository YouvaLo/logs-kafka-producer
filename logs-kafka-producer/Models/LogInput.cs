using System.ComponentModel.DataAnnotations;
using LogType = logs_kafka_producer.Enums; //alias

namespace logs_kafka_producer.Models;

public record LogInput
{
   public Guid Id { get; init; }
   
   public DateTime Timestamp { get; init; }

   [Required(ErrorMessage = "Log level is required.")]
   [EnumDataType(typeof(LogType.LogLevel), ErrorMessage = "The value for Type is not a valid LogLevel.")]
   public LogType.LogLevel Type { get; init; }

   [Required(AllowEmptyStrings = false, ErrorMessage = "A log message is required.")]
   [StringLength(4000, ErrorMessage = "The message cannot exceed 4000 characters.")]
   public string Message { get; init; }

   [Required(AllowEmptyStrings = false, ErrorMessage = "The source application is required.")]
   [StringLength(30, MinimumLength = 3, ErrorMessage = "The source application name must be between 3 and 30 characters long.")]
   public string SourceApplication { get; init; }

   public LogInput()
   {
      Id = Guid.NewGuid();
      Timestamp = DateTime.UtcNow;
      Message = string.Empty;
      SourceApplication = string.Empty;
      Type = default;
   }
}
