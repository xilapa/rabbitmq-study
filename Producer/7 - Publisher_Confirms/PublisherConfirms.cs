using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using RabbitMQ.Client;

namespace Producer._7___Publisher_Confirms;

public class PublisherConfirms
{
    public static void ProduceAndWaitSync()
    {
        // creating a connection
        var connectionFactory = new ConnectionFactory {HostName = "localhost"};
        using var connection = connectionFactory.CreateConnection();
        
        // getting a channel
        using var channel = connection.CreateModel();
        
        // enabling publish confirms
        channel.ConfirmSelect();
        
        // creating a queue
        channel.QueueDeclare("publisher_confirms", autoDelete: false, durable: true, exclusive: false);
        
        // publishing a test message using the default exchange
        var body = JsonSerializer.SerializeToUtf8Bytes("Olá!");
        var props = channel.CreateBasicProperties();
        channel.BasicPublish("", "publisher_confirms", props, body);
        
        // bad: block until message is confirmed or throw an exception
        channel.WaitForConfirmsOrDie(new TimeSpan(0,0,0,7));
    }
    
    public static async Task ProduceAndWaitAsync()
    {
        // creating a connection
        var connectionFactory = new ConnectionFactory {HostName = "localhost"};
        using var connection = connectionFactory.CreateConnection();
        
        // getting a channel
        using var channel = connection.CreateModel();

        // enabling publish confirms
        channel.ConfirmSelect();
        
        // creating a queue
        channel.QueueDeclare("publisher_confirms", autoDelete: false, durable: true, exclusive: false);
        
        // saving published and nacked messages to 
        var publishedMessages = new ConcurrentDictionary<ulong, byte[]>();
        var nackedMessages = Channel.CreateUnbounded<byte[]>();

        // each number is unique per channel, take caution when using more than one publisher per channel
        var messageId = channel.NextPublishSeqNo;
        var body = JsonSerializer.SerializeToUtf8Bytes("Olá!");
        publishedMessages.TryAdd(messageId, body);
        var props = channel.CreateBasicProperties();
        channel.BasicPublish("", "publisher_confirms", props, body);

        void RemoveMessageById(ulong sequence, bool multiple)
        {
            if (!multiple)
            {
                publishedMessages.TryRemove(sequence, out _);
                return;
            }

            foreach (var key in publishedMessages.Keys)
                if (key <= sequence)
                    publishedMessages.TryRemove(key, out _);
        }

        async Task RedirectToNackedMessages(ulong sequence, bool multiple)
        {
            if (!multiple)
            {
                publishedMessages.TryRemove(sequence, out var body);
                await nackedMessages.Writer.WriteAsync(body);
                return;
            }

            foreach (var key in publishedMessages.Keys)
                if (key <= sequence)
                {
                    publishedMessages.TryRemove(key, out var body);
                    await nackedMessages.Writer.WriteAsync(body);
                }
        }

        // listen to acks
        channel.BasicAcks += (sender, ea) =>
            RemoveMessageById(ea.DeliveryTag, ea.Multiple);

        // listen to nacks
        channel.BasicNacks += (sender, ea) =>
            RedirectToNackedMessages(ea.DeliveryTag, ea.Multiple)
                .ContinueWith(faultedTask => Console.WriteLine(faultedTask.Exception!.Message),
                    TaskContinuationOptions.OnlyOnFaulted);

        // do something with nacked messages
        // we could retry and if the error persists send to another queue,
        // log the error or save it on a database
        await foreach (var nackedMsgBody in nackedMessages.Reader.ReadAllAsync())
        {
            var msg = JsonSerializer.Deserialize<string>(nackedMsgBody);
            Console.WriteLine($"This message cannot be published: {msg}");
        }
    }
}