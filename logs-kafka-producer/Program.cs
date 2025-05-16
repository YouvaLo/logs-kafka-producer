using logs_kafka_producer.Configuration;
using logs_kafka_producer.Extensions;
using Microsoft.OpenApi.Models;
using Serilog;
using System.Reflection;
using AspNetCoreRateLimit;

Log.Information("Application Start......");

var builder = WebApplication.CreateBuilder(args);


builder.Services.AddOptions<KafkaSettings>()
   .Bind(builder.Configuration.GetSection(KafkaSettings.SectionName))
   .ValidateDataAnnotations()
   .ValidateOnStart();

builder.WebHost.ConfigureKestrel(serverOptions =>
{
   serverOptions.Limits.MaxRequestBodySize = 1 * 1024 * 1024; //1Mo
   serverOptions.Limits.MaxRequestHeadersTotalSize = 8 * 1024; //8Ko
});

var logFilePath = Path.Combine(AppContext.BaseDirectory, "Logs", "log-.txt");
Log.Logger = new LoggerConfiguration()
   .ReadFrom.Configuration(builder.Configuration)
   .WriteTo.File(logFilePath, rollingInterval: RollingInterval.Day,
         outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
   .Enrich.FromLogContext()
   .CreateLogger();

builder.Host.UseSerilog();

builder.Services.AddServices(builder.Configuration);

builder.Services.AddCors(options =>
{
   options.AddPolicy("LocalhostPolicy", policy =>
   {
      policy.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader();
   });
});

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
   var versionInfo = Assembly.GetExecutingAssembly().GetName().Version;
   var version = versionInfo is not null ? $"{versionInfo.Major}.{versionInfo.Minor}.{versionInfo.Build}" : "1.0.0";

   c.SwaggerDoc("v1", new OpenApiInfo
   {
      Title = "API Logs Distributed",
      Version = "api-" + version,
      Description = "API permettant de gérer des logs via Kafka",
      Contact = new OpenApiContact { Name = "Support API", Email = "" }
   });
});

// Ajouter un cache en mémoire pour stocker les informations de limitation
builder.Services.AddMemoryCache();
// Configurer les options de limitation 
builder.Services.Configure<IpRateLimitOptions>(builder.Configuration.GetSection("IpRateLimiting"));
// Ajouter le service de limitation en mémoire
builder.Services.AddInMemoryRateLimiting();
// Enregistrer la configuration en tant que service singleton
// Une seule instance utilisé pour toute la durée de vie de l'application 
builder.Services.AddSingleton<IRateLimitConfiguration, RateLimitConfiguration>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
   app.UseSwagger();
   app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "LogProducer API v1"));
}

app.UseHttpsRedirection();

if (app.Environment.IsDevelopment()) app.UseCors("LocalhostPolicy");

app.UseIpRateLimiting();

app.Use(async (context, next) =>
{
   // Empêche le navigateur de deviner le type de contenu, ce qui limite les attaques de type MIME-sniffing
   context.Response.Headers["X-Content-Type-Options"] = "nosniff";
   // Bloque l'affichage de la page dans une iframe, ce qui empêche les attaques de clickjacking
   context.Response.Headers["X-Frame-Options"] = "DENY";
   // Autorise le chargement des ressources uniquement depuis notre site, réduisant ainsi le risque d'injections de contenu malveillant (ex : attaques XSS)
   context.Response.Headers["Content-Security-Policy"] = "default-src 'self' script-src 'self' cdn.jsdelivr.net; style-src 'self' fonts.googleapis.com;";
   // N'envoie pas d'informations sur l'origine de la requête, afin de limiter la fuite d'informations
   context.Response.Headers.Append("Referrer-Policy", "no-referrer");
   // Empêche le chargement de fichiers de domaines externes, ce qui protège contre certaines attaques de cross-domain.
   context.Response.Headers.Append("X-Permitted-Cross-Domain-Policies", "none");

   // Supprimer les headers révélant des informations serveur
   context.Response.Headers.Remove("Server");
   context.Response.Headers.Remove("X-Powered-By");

   // Passer au middleware suivant
   await next();
});

app.UseRouting();
app.MapControllers();

app.Run();
