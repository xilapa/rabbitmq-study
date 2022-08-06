using System.Text.Json;
using RabbitMQ.Client;

namespace Producer._1___HelloWorld;

public class HelloWorld
{
    public static void Produce(string message)
    {
        // Connect to the broker
        var connectionFactory = new ConnectionFactory {HostName = "localhost", UserName = "guest", Password = "guest"};
        using var connection = connectionFactory.CreateConnection();

        // get a channel on the connection
        using var channel = connection.CreateModel();

        // declare a queue
        channel.QueueDeclare("hello", true, false, false);

        // creating the message
        var msgBody = JsonSerializer.SerializeToUtf8Bytes(message);
        var msgProps = channel.CreateBasicProperties();
        msgProps.ContentType = "application/json";
        msgProps.DeliveryMode = 1;

        // publishing the message
        channel.BasicPublish("", routingKey:"hello", msgProps, msgBody);
        Console.WriteLine($"Message sent: {message}");
    }
}