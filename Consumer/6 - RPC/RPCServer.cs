using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer._6___RPC;

public class RPCServer
{
    public static void Consume()
    {
        // create a connection
        var connectionFactory = new ConnectionFactory {HostName = "localhost"};
        using var connection = connectionFactory.CreateConnection();
        
        // getting a channel
        using var channel = connection.CreateModel();
        
        // declare a queue
        channel.QueueDeclare(queue: "rpc_queue", durable: false, exclusive: false, autoDelete: false);
        
        // limiting the maximum messages to process
        channel.BasicQos(0,1,false);
        
        // declare a consumer
        var consumer = new EventingBasicConsumer(channel);
        
        // consume the messages and reply back
        consumer.Received += (_, ea) =>
        {
            var receivedMsg = JsonSerializer.Deserialize<int>(ea.Body.Span);
            var receivedMsgProps = ea.BasicProperties;

            // set the CorrelationId on reply message
            var replyProps = channel.CreateBasicProperties();
            replyProps.CorrelationId = receivedMsgProps.CorrelationId;
            
            // Calculate the fibonacci number
            Console.WriteLine($"{DateTime.Now} - Calculating the fibonacci number on position {receivedMsg}");
            var fibonacciNumber = GetFibonacci(receivedMsg);
            
            // Reply
            var replyMsg = JsonSerializer.SerializeToUtf8Bytes(fibonacciNumber);
            channel.BasicPublish(exchange: "", routingKey: receivedMsgProps.ReplyTo, basicProperties: replyProps,
                body: replyMsg);
            
            // Ack the received message
            channel.BasicAck(ea.DeliveryTag, multiple: false);
            
            // It's not a good practice to publish and consume from the same channel,
            // because channels aren't thread-safe more than one thread can be accessing it
        };
        
        // listen to messages
        channel.BasicConsume(queue: "rpc_queue", autoAck: false, consumer);
        Console.WriteLine($"{DateTime.Now} - Listening for RPC requests");
        Console.ReadLine();
        



    }
    
    /// Assumes only valid positive integer input.
    /// Don't expect this one to work for big numbers, and it's
    /// probably the slowest recursive implementation possible.
    private static int GetFibonacci(int n)
    {
        if (n is 0 or 1) return n;
        return GetFibonacci(n - 1) + GetFibonacci(n - 2);
    }
}