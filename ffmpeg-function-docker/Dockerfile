FROM mcr.microsoft.com/azure-functions/python:4-python3.10

RUN apt-get update && apt-get install -y ffmpeg

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

COPY . /home/site/wwwroot
