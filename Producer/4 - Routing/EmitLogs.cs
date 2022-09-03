using System.Text.Json;
using RabbitMQ.Client;

namespace Producer._4___Routing;

public class EmitLogs
{
    public static void Produce(string severity, string message)
    {
        // Connect to the broker
        var connectionFactory = new ConnectionFactory {HostName = "localhost", UserName = "guest", Password = "guest"};
        using var connection = connectionFactory.CreateConnection();

        // get a channel on the connection
        using var channel = connection.CreateModel();
        
        // declare an exchange
        channel.ExchangeDeclare("direct_logs", ExchangeType.Direct);

        // creating the message
        var msgBody = JsonSerializer.SerializeToUtf8Bytes(message);
        var msgProps = channel.CreateBasicProperties();
        msgProps.ContentType = "application/json";
        msgProps.DeliveryMode = 1;

        // publishing the message using severity as routing key
        channel.BasicPublish("direct_logs", routingKey: severity, msgProps, msgBody);

        Console.WriteLine($"Message sent: {message}");
    }
}