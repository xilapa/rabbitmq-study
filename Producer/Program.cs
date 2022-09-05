using Producer._1___HelloWorld;
using Producer._2___WorkQueues;
using Producer._3___Publish_Subscribe;
using Producer._4___Routing;
using Producer._5___Topics;
using Producer._6___RPC;

using var rpcClient = new RPCClient();
Console.WriteLine("Requesting the 30th fibonacci number");
var fibNumber = await rpcClient.Call(30);
Console.WriteLine($"Received {fibNumber}");

