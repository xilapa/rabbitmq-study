using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var connectionFactory = new ConnectionFactory 
    {HostName = "localhost", DispatchConsumersAsync = true};
using var connection = connectionFactory.CreateConnection();
using var channel = connection.CreateModel();

channel.QueueDeclare("multiple_consumers_single_channel");
channel.BasicQos(prefetchSize: 0, prefetchCount: 2, global: false);

var consumer = new AsyncEventingBasicConsumer(channel);
var channelLock = new object();

consumer.Received += (_, ea) => HandleMessage(ea, channelLock);

channel.BasicConsume("multiple_consumers_single_channel", autoAck: false, consumer);
Console.WriteLine("Listening for new messages");
Console.ReadKey();

Task HandleMessage(BasicDeliverEventArgs ea, object channelLockObject)
{
    var msgBody = ea.Body.Span.ToArray();
    Task.Run(async () =>
    {
        var body = JsonSerializer.Deserialize<string>(msgBody);
        var splittedMsg = body.Split(" ");
        var msg = splittedMsg[0];
        var timesToRepeat = int.Parse(splittedMsg[1]);

        for (int i = 0; i < timesToRepeat; i++)
        {
            Console.WriteLine($"{i} - {DateTime.Now} - {msg}");
            await Task.Delay(1000);
        }

        lock (channelLockObject)
        {
            channel.BasicAck(ea.DeliveryTag, false);
        }
    }).ContinueWith(faultedTask =>
    {
        var msgs = faultedTask.Exception!
            .InnerExceptions.Select(e => e.Message);
        Console.WriteLine(string.Join(", ", msgs));
        lock (channelLock)
        {
            channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
        }
    }, TaskContinuationOptions.OnlyOnFaulted);
    
    return Task.CompletedTask;
}