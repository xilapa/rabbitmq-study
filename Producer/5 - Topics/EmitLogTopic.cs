using System.Text.Json;
using RabbitMQ.Client;

namespace Producer._5___Topics;

public class EmitLogTopic
{
    public static void Produce(string[] args)
    {
        // Connect to the broker
        var factory = new ConnectionFactory {HostName = "localhost"};
        using var connection = factory.CreateConnection();
        
        // get a channel from the connection
        using var channel = connection.CreateModel();
        
        // declare an exchange
        channel.ExchangeDeclare("topic_logs", ExchangeType.Topic);

        // get the routing key
        var routingKey = args.Length > 0 ? args[0] : "anonymous.info";

        // create the message
        var message = args.Length > 1 ? string.Join(" ", args.Skip(1)) : "hello!";
        var messageBody = JsonSerializer.SerializeToUtf8Bytes(message);
        var msgProps = channel.CreateBasicProperties();
        msgProps.ContentType = "application/json";
        msgProps.DeliveryMode = 1;
        
        // publish the message
        channel.BasicPublish("topic_logs", routingKey, msgProps, messageBody);
        Console.WriteLine($"{DateTime.Now} Message sent: {routingKey} - {message}");
    }
}