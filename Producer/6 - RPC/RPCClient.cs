using System.Text.Json;
using System.Threading.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Producer._6___RPC;

public class RPCClient : IDisposable
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _replyQueueName;
    private readonly Channel<int> _responseQueue;
    private string _correlationId;

    public RPCClient()
    {
        // create a connection
        var connectionFactory = new ConnectionFactory {HostName = "localhost", DispatchConsumersAsync = true};
        _connection = connectionFactory.CreateConnection();

        // get a channel
        _channel = _connection.CreateModel();
        
        // create the reply queue
        _replyQueueName = _channel.QueueDeclare().QueueName;
        
        // create the consumer to listen the reply
        _responseQueue = Channel.CreateUnbounded<int>();
        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.Received += async (_, ea) =>
        {
            // check if the correlation id is the same that was sent
            if (ea.BasicProperties.CorrelationId == _correlationId)
            {
                var receivedMsg = JsonSerializer.Deserialize<int>(ea.Body.Span);
                await _responseQueue.Writer.WriteAsync(receivedMsg);
            }
        };

        // start listening for reply
        _channel.BasicConsume(queue: _replyQueueName, consumer: consumer, autoAck: true);
        // It's not a good practice to publish and consume from the same channel,
        // because channels aren't thread-safe more than one thread can be accessing it
    }

    public ValueTask<int> Call(int message)
    {
        var messageBytes = JsonSerializer.SerializeToUtf8Bytes(message);
        var messageProps = _channel.CreateBasicProperties();
        _correlationId = messageProps.CorrelationId = Guid.NewGuid().ToString();
        messageProps.ReplyTo = _replyQueueName;

        // publishing to the queue
        _channel.BasicPublish(exchange: "", routingKey: "rpc_queue", basicProperties: messageProps, body: messageBytes);
        
        // getting the reply asynchronously
        return _responseQueue.Reader.ReadAsync();
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        _connection.Close();
        _connection.Dispose();
        _channel.Dispose();
    }
}