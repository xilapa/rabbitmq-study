using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer._4___Routing;

public class ConsumeLogs
{
    public static void Consume(string[] severities)
    {
        // Connect to the broker
        var connectionFactory = new ConnectionFactory
        {
            HostName = "localhost", UserName = "guest", Password = "guest",
            DispatchConsumersAsync = true
        };
        using var connection = connectionFactory.CreateConnection();

        // get a channel on the connection
        using var channel = connection.CreateModel();
        
        // declare an exchange
        channel.ExchangeDeclare("direct_logs", ExchangeType.Direct);

        // declare a queue
        var queue = channel.QueueDeclare(durable: false, exclusive: true, autoDelete: true);
        
        foreach (var severity in severities)
            channel.QueueBind(queue.QueueName, "direct_logs", severity);

        // function to consume the message
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            var msg = JsonSerializer.Deserialize<string>(ea.Body.Span);
            Console.WriteLine($"{DateTime.Now} - Message received: {msg}");
            var dots = msg!.Split('.').Length - 1;
            await Task.Delay(dots * 1000);
            Console.WriteLine($"{DateTime.Now} - Message process done: {msg}");
        };

        // subscribe to the queue
        channel.BasicConsume(queue.QueueName, true, consumer);

        Console.WriteLine("Press [enter] to exit.");
        Console.ReadLine();
    }
}