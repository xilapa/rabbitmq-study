using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer._8___DeadLetterExchange;

public class DeadLetterExchange
{
    public static void Consume()
    {
        // getting a channel
        var connectionFactory = new ConnectionFactory {HostName = "localhost"};
        using var connection = connectionFactory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.BasicQos(0, 1, false);
        
        // declaring the exchanges
        channel.ExchangeDeclare("normal-flow", ExchangeType.Direct);
        channel.ExchangeDeclare("dlx-flow", ExchangeType.Direct);
        
        // declaring the queue with the dlx
        var queueArgs = new Dictionary<string, object>
        {
            {"x-dead-letter-exchange", "dlx-flow"},
            {"x-dead-letter-routing-key", "error-key"}
        };
        channel.QueueDeclare("normal-flow-queue", false, false, false, queueArgs);
        channel.QueueDeclare("dlx-flow-queue", false, false, false);

        // binding the exchanges and queues
        channel.QueueBind("dlx-flow-queue","dlx-flow","error-key");
        channel.QueueBind("normal-flow-queue", "normal-flow", "normal-key" );
        
        // nacking msg from normal flow
        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (_, ea) =>
            channel.BasicNack(ea.DeliveryTag, false, false);

        channel.BasicConsume("normal-flow-queue",autoAck: false, consumer);

        
        // consuming messages from dlx
        var dlxChannel = connection.CreateModel();
        dlxChannel.BasicQos(0, 1, false);
        var dlxConsumer = new EventingBasicConsumer(dlxChannel);
        dlxConsumer.Received += (_, ea) =>
        {
            var msg = JsonSerializer.Deserialize<string>(ea.Body.Span);
            var headers = ea.BasicProperties.Headers;
            Console.WriteLine(msg);
            dlxChannel.BasicAck(ea.DeliveryTag, false);
        };

        dlxChannel.BasicConsume("dlx-flow-queue", autoAck: false, dlxConsumer);

        Console.ReadKey();
    }
}