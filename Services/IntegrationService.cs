using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer.Services
{
    public class IntegrationService : BackgroundService
    {
        private readonly IConsumer<string, string> consumer;
        private readonly IApplicationLifetime applicationLifetime;
        private readonly MessageProcessor messageProcessor;
        private bool isFirstRun = true;

        public IntegrationService(
            IConsumer<string, string> consumer,
            IApplicationLifetime applicationLifetime,
            MessageProcessor messageProcessor)
        {
            this.consumer = consumer;
            this.applicationLifetime = applicationLifetime;
            this.messageProcessor = messageProcessor;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var allCancellationTS = CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken,
                applicationLifetime.ApplicationStopping
            );

            while (!allCancellationTS.IsCancellationRequested)
            {
                try
                {
                    if (isFirstRun)
                    {
                        DoFirstRunWork(stoppingToken);
                        await Task.Delay(1000, allCancellationTS.Token);
                        isFirstRun = false;
                    }
                    else
                        await DoWork(allCancellationTS);
                }
                catch (Exception exception)
                {
                    Console.WriteLine("Error while consuming message: {message}, {innerMessage}, {stackTrace}",
                        exception.Message,
                        exception.InnerException?.Message,
                        exception.StackTrace
                    );
                    applicationLifetime.StopApplication();
                }
            }
        }

        private async Task DoWork(CancellationTokenSource stoppingTS)
        {
            Console.WriteLine("Waiting for next message...");

            var message = consumer.Consume(stoppingTS.Token);

            Console.WriteLine("Consumed message at: {offset}", message.TopicPartitionOffset);

            await messageProcessor.Process(message.Value, stoppingTS.Token);
            consumer.Commit();
        }

        private void DoFirstRunWork(CancellationToken stoppingToken)
        {
            //first run work
        }

    }
}