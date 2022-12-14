using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer._1___HelloWorld;

public class HelloWorld
{
    public static void Consume()
    {
        // Connect to the broker
        var connectionFactory = new ConnectionFactory {HostName = "localhost", UserName = "guest", Password = "guest"};
        using var connection = connectionFactory.CreateConnection();

        // get a channel on the connection
        using var channel = connection.CreateModel();

        // declare a queue
        channel.QueueDeclare("hello", true, false, false);

        // set the limit of unnacked messages to receive
        channel.BasicQos(0,1,false);

        // function to consume the message
        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (cons, ea) =>
        {
            var msg = JsonSerializer.Deserialize<string>(ea.Body.Span);
            Console.WriteLine($"{DateTime.Now} - Message received: {msg}");
            (cons as EventingBasicConsumer)!.Model.BasicAck(ea.DeliveryTag, false);
        };

        // subscribe to the queue
        channel.BasicConsume("hello", false, consumer);

        Console.WriteLine("Press [enter] to exit.");
        Console.ReadLine();
    }
}