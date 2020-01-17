using Confluent.Kafka;
using KafkaConsumer.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace KafkaConsumer
{
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            var env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production";
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .AddCommandLine(args)
                .Build();

            Console.WriteLine("Kafka consumer is starting...");

            var builder = new HostBuilder()
                .UseEnvironment(env)
                .ConfigureHostConfiguration(configHost => configHost.AddConfiguration(configuration))
                .ConfigureServices((hostContext, services) =>
                {
                    //main
                    services.AddHostedService<IntegrationService>();
                    services.AddSingleton<MessageProcessor>();

                    //kafka
                    services.AddSingleton<IConsumer<string, string>>(_ =>
                      CreateConsumer(
                        hostContext.Configuration["Kafka:Server"],
                        hostContext.Configuration["Kafka:Group"],
                        hostContext.Configuration["Kafka:Topic"]
                      )
                    );
                });

            await builder.RunConsoleAsync();
            Console.WriteLine("\nKafka consumer is stopping.");
            return 0;
        }

        private static IConsumer<string, string> CreateConsumer(string kafkaServer, string kafkaGroup, string kafkaTopic)
        {
            Console.WriteLine($"Connecting to Kafka server: {kafkaServer}");
            var conf = new ConsumerConfig
            {
                BootstrapServers = kafkaServer,
                GroupId = kafkaGroup,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                MaxPollIntervalMs = 24 * 60 * 60 * 1000
            };
            var consumer = new ConsumerBuilder<string, string>(conf)
                .SetErrorHandler((consumerService, error) =>
                    Console.WriteLine($"Error while connecting to kafka: {error.Reason}")
                ).Build();
            consumer.Subscribe(kafkaTopic);
            return consumer;
        }

    }
}
