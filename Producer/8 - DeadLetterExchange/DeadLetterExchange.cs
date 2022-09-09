using System.Text.Json;
using RabbitMQ.Client;

namespace Producer._8___DeadLetterExchange;

public class DeadLetterExchange
{
    public static void Publish()
    {
        // getting the channel
        var connectionFactory = new ConnectionFactory {HostName = "localhost"};
        using var connection = connectionFactory.CreateConnection();
        using var channel = connection.CreateModel();
        
        // declaring the exchange
        channel.ExchangeDeclare("normal-flow", ExchangeType.Direct);

        // generating a message
        var body = JsonSerializer.SerializeToUtf8Bytes("test message");
        var msgProps = channel.CreateBasicProperties();
        
        channel.BasicPublish("normal-flow", "normal-key", msgProps, body);
    }
}