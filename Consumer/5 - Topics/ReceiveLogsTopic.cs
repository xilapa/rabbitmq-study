using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer._5___Topics;

public class ReceiveLogsTopic
{
    public static void Consume(string[] args)
    {
        if (args.Length == 0)
            args = new[] {"kernel.*"};
        
        // Connect to the broker
        var connectionFactory = new ConnectionFactory {HostName = "localhost"};
        using var connection = connectionFactory.CreateConnection();
        
        // get a channel from the connection
        using var channel = connection.CreateModel();
        
        // limit the maximum received messages
        channel.BasicQos(0, 1, false);

        // declare an exchange
        channel.ExchangeDeclare("topic_logs", ExchangeType.Topic);
        
        // declare a queue
        var queueName = channel.QueueDeclare().QueueName;
        
        // bind the queue and exchange
        foreach (var routingKey in args)
            channel.QueueBind(queueName, "topic_logs", routingKey);
        
        // function to consume
        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            var msg = JsonSerializer.Deserialize<string>(ea.Body.Span);
            Console.WriteLine(
                $"{DateTime.Now} - Message received: {msg}");

            // Simulate a heavy task
            var dots = msg!.Split('.').Length;
            await Task.Delay(dots * 1000);
            Console.WriteLine($"{DateTime.Now} - Message process done: {msg}");


            channel.BasicAck(ea.DeliveryTag, false);
        };

        channel.BasicConsume(queueName, autoAck: false, consumer);

        Console.WriteLine("Press [enter] to exit.");
        Console.ReadLine();
    }
}