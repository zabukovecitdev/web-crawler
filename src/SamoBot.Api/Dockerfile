FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Directory.Build.props", "Directory.Packages.props", "./"]
COPY ["src/SamoBot.Api/SamoBot.Api.csproj", "src/SamoBot.Api/"]
COPY ["src/SamoBot.Adapters/SamoBot.Adapters.csproj", "src/SamoBot.Adapters/"]
COPY ["src/SamoBot.Core/SamoBot.Core.csproj", "src/SamoBot.Core/"]
RUN dotnet restore "src/SamoBot.Api/SamoBot.Api.csproj"
COPY . .
WORKDIR "/src/src/SamoBot.Api"
RUN dotnet build "./SamoBot.Api.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./SamoBot.Api.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SamoBot.Api.dll"]
