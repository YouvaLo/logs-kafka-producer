FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["*.sln", "."]
COPY ["logs-kafka-producer/logs-kafka-producer.csproj", "logs-kafka-producer/"]
RUN dotnet restore "./logs-kafka-producer/logs-kafka-producer.csproj"
COPY . .
WORKDIR "/src/logs-kafka-producer"
RUN dotnet build "./logs-kafka-producer.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./logs-kafka-producer.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "logs-kafka-producer.dll"]