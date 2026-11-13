FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Directory.Build.props", "Directory.Packages.props", "./"]
COPY ["src/SamoBot.Worker/SamoBot.Worker.csproj", "src/SamoBot.Worker/"]
COPY ["src/SamoBot.Adapters/SamoBot.Adapters.csproj", "src/SamoBot.Adapters/"]
COPY ["src/SamoBot.Core/SamoBot.Core.csproj", "src/SamoBot.Core/"]
RUN dotnet restore "src/SamoBot.Worker/SamoBot.Worker.csproj"
COPY . .
WORKDIR "/src/src/SamoBot.Worker"
RUN dotnet build "./SamoBot.Worker.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./SamoBot.Worker.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SamoBot.Worker.dll"]
