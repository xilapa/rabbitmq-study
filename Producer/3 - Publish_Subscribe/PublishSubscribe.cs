using System.Text.Json;
using RabbitMQ.Client;

namespace Producer._3___Publish_Subscribe;

public class PublishSubscribe
{
    public static void Produce(string message)
    {
        // Connect to the broker
        var connectionFactory = new ConnectionFactory {HostName = "localhost", UserName = "guest", Password = "guest"};
        using var connection = connectionFactory.CreateConnection();

        // get a channel on the connection
        using var channel = connection.CreateModel();
        
        // declare an exchange
        channel.ExchangeDeclare("logs", ExchangeType.Fanout, true, false);

        // creating the message
        var msgBody = JsonSerializer.SerializeToUtf8Bytes(message);
        var msgProps = channel.CreateBasicProperties();
        msgProps.ContentType = "application/json";
        msgProps.DeliveryMode = 1;

        // publishing the message
        // the routing key is ignored in fanout exchanges
        foreach (var _ in Enumerable.Range(0, 5))
            channel.BasicPublish("logs", routingKey:"", msgProps, msgBody);

        Console.WriteLine($"Message sent: {message}");
    }
}