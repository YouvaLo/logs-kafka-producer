using logs_kafka_producer.Contracts;
using logs_kafka_producer.Exceptions;
using logs_kafka_producer.Models;
using Microsoft.AspNetCore.Mvc;

namespace logs_kafka_producer.Controllers;

[Route("api")]
[ApiController]
public class KafkaProducerController : ControllerBase
{
   private readonly IKafkaProducerService _kafkaProducerService;
   private readonly ILogger<KafkaProducerController> _logger;

   public KafkaProducerController(
        IKafkaProducerService kafkaProducerService,
        ILogger<KafkaProducerController> logger)
   {
      _kafkaProducerService = kafkaProducerService ?? throw new ArgumentNullException(nameof(kafkaProducerService));
      _logger = logger ?? throw new ArgumentNullException(nameof(logger));
   }

   [HttpPost("log-producer")]
   [ProducesResponseType(StatusCodes.Status202Accepted)]
   [ProducesResponseType(StatusCodes.Status400BadRequest)]
   [ProducesResponseType(StatusCodes.Status500InternalServerError)]
   public async Task<IActionResult> ProduceLog([FromBody] LogInput logInput)
   {
      _logger.LogInformation("Request received. SourceApplication: {SourceApp}, Type: {LogType}",
                               logInput.SourceApplication, logInput.Type);

      try
      {
         await _kafkaProducerService.ProduceLogAsync(logInput);

         _logger.LogInformation("Log (ID: {LogId}) sended for Kafka", logInput.Id);
         return Ok(new { message = "Log received and waited for processing.", logId = logInput.Id });
      }
      catch (ArgumentNullException ex)
      {
         _logger.LogWarning(ex, "Null argument when attempting to generate the log.");
         return BadRequest(new ProblemDetails { Title = "Invalid input", Detail = ex.Message });
      }
      catch (KafkaProduceException ex) 
      {
         _logger.LogError(ex, "Unsuccessful log production to Kafka. LogInput ID: {LogId}", logInput.Id);
         var problemDetails = new ProblemDetails
         {
            Status = StatusCodes.Status500InternalServerError,
            Title = "Failed to produce log to Kafka",
            Detail = $"An error occurred while sending the log (ID: {logInput.Id}) to Kafka. Please try again later or contact support.",
            Instance = HttpContext.Request.Path
         };
         
         return StatusCode(StatusCodes.Status500InternalServerError, problemDetails);
      }
      catch (Exception ex)
      {
         _logger.LogCritical(ex, "Unexpected error during log generation. LogInput ID: {LogId}", logInput.Id);
         var problemDetails = new ProblemDetails
         {
            Status = StatusCodes.Status500InternalServerError,
            Title = "An unexpected error occurred",
            Detail = "An unexpected error occurred while processing your request. Please try again.",
            Instance = HttpContext.Request.Path
         };
         return StatusCode(StatusCodes.Status500InternalServerError, problemDetails);
      }
   }
}
