using System.Text.Json;
using RabbitMQ.Client;

var connectionFactory = new ConnectionFactory {HostName = "localhost"};
using var connection = connectionFactory.CreateConnection();
using var channel = connection.CreateModel();
var defaultProps = channel.CreateBasicProperties();
while (true)
{
    var message = Console.ReadLine();
    if ("exit".Equals(message))
        break;
    
    var body = JsonSerializer.SerializeToUtf8Bytes(message);
    channel.BasicPublish(exchange: "", routingKey: "multiple_consumers_single_channel", defaultProps, body);
}