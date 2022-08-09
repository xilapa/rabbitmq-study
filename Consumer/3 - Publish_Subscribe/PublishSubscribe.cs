using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer._3___Publish_Subscribe;

public class PublishSubscribe
{
    public static void Consume()
    {
        // Connect to the broker
        var connectionFactory = new ConnectionFactory {HostName = "localhost", UserName = "guest", Password = "guest"};
        connectionFactory.DispatchConsumersAsync = true;
        using var connection = connectionFactory.CreateConnection();

        // get a channel on the connection
        using var channel = connection.CreateModel();
        
        // declare an exchange
        channel.ExchangeDeclare("logs", ExchangeType.Fanout, true, false);

        // declare a queue
        var queue = channel.QueueDeclare(durable: false, exclusive: true, autoDelete: true);

        // bind the exchange and queue
        // the routing key is ignored in fanout exchanges
        channel.QueueBind(queue.QueueName, "logs", "");

        // function to consume the message
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (cons, ea) =>
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