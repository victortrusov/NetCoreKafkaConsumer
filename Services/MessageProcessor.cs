using KafkaConsumer.Models;
using Newtonsoft.Json;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer.Services
{
    public class MessageProcessor
    {
        public MessageProcessor()
        {
        }

        public async Task Process(string message, CancellationToken stoppingToken)
        {
            var json = message.Substring(message.IndexOf("{"));
            var kafkaMessage = JsonConvert.DeserializeObject<Message>(json);
        }
    }
}