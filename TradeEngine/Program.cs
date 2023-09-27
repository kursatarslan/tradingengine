using System;
using System.Text;
using Newtonsoft.Json;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using StackExchange.Redis;
using System.Linq;
using Newtonsoft.Json.Converters;

namespace TradingEngine
{
    class Order
    {
        public Guid OrderId { get; set; }
        public string UserId { get; set; }
        public string Symbol { get; set; }
        public OrderType Type { get; set; }
        public decimal Quantity { get; set; }
        public decimal Price { get; set; }
    }

    public enum OrderType
    {
        [JsonProperty("Buy")] Buy = 0,

        [JsonProperty("Sell")] Sell = 1
    }

    public class User
    {
        public string Id { get; set; }
        public decimal HDOBalance { get; set; }
        public decimal HCNBalance { get; set; }
    }

    class Program
    {
        static readonly string connectionString =
            "Host=localhost;Username=postgres;Password=postgres;Database=postgres";

        static readonly ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        static readonly IDatabase db = redis.GetDatabase();

        static void Main()
        {
            Console.WriteLine("Trading Engine started. Waiting for orders...");

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = factory.CreateConnection();
            NpgsqlConnection.GlobalTypeMapper.MapEnum<OrderType>("ordertype");

            using var channel = connection.CreateModel();
            channel.QueueDeclare(queue: "engine1", durable: false, exclusive: false, autoDelete: false,
                arguments: null);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var orderJson = Encoding.UTF8.GetString(body);
                    Console.WriteLine($"Received order: {orderJson}");
                    if (string.IsNullOrEmpty(orderJson))
                    {
                        Console.WriteLine("Order JSON is null or empty.");
                        return;
                    }

                    var order = JsonConvert.DeserializeObject<Order>(orderJson);
                    if (order == null)
                    {
                        Console.WriteLine("Failed to deserialize order.");
                        return;
                    }

                    ProcessOrder(order);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Received order Exception: {e.Message}");
                }
            };

            channel.BasicConsume(queue: "engine1", autoAck: true, consumer: consumer);
            Console.WriteLine("Listening to engine1 queue...");

            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
        }

        static void ProcessOrder(Order order)
        {
            if (order.Type == OrderType.Buy)
            {
                PrintSortedSet("SELL");
                Console.WriteLine("ProcessOrder buying.....");
                var bestSellOrders = db.SortedSetRangeByScoreWithScores("SELL", start: double.NegativeInfinity,
                    stop: (double)order.Price, order: StackExchange.Redis.Order.Ascending).FirstOrDefault();

                RedisValue bestOrderJson = bestSellOrders.Element;

                Console.WriteLine(
                    $"ProcessOrder bestSellOrders.Element.IsNullOrEmpty {bestSellOrders.Element.IsNullOrEmpty} ");

                if (!bestSellOrders.Element.IsNullOrEmpty /*&& Convert.ToDecimal(bestSellOrder.Score) <= order.Price*/)
                {
                    Console.WriteLine(
                        $"Found Matching Sell Order: {bestSellOrders.Element}, Score: {bestSellOrders.Score}");


                    // You might want to ExecuteOrder here if a matching sell order is found.
                    ExecuteOrder(order, bestOrderJson.ToString());
                }
                else
                {
                    // Otherwise, add the buy order to the SortedSet with a negative score
                    db.SortedSetAdd("BUY", JsonConvert.SerializeObject(order), Convert.ToDouble(-order.Price));
                    Console.WriteLine(
                        $"No matching Sell order found, Order added to BUY set: {JsonConvert.SerializeObject(order)}");
                }
            }
            else if (order.Type == OrderType.Sell)
            {
                PrintSortedSet("BUY");

                Console.WriteLine("ProcessOrder selling.....");
                var bestBuyOrders = db.SortedSetRangeByScoreWithScores("BUY", start: (double)order.Price,
                    stop: double.PositiveInfinity, order: StackExchange.Redis.Order.Descending).FirstOrDefault();
                RedisValue bestOrderJson = bestBuyOrders.Element;

                // Check if a matching buy order exists, and process it if it does.
                Console.WriteLine(
                    $"ProcessOrder bestBuyOrders.Element.IsNullOrEmpty{bestBuyOrders.Element.IsNullOrEmpty} ");
                if (!bestBuyOrders.Element.IsNullOrEmpty /*&& Convert.ToDecimal(-bestBuyOrder.Score) >= order.Price*/)
                {
                    Console.WriteLine(
                        $"Found Matching Buy Order: {bestBuyOrders.Element}, Score: {bestBuyOrders.Score}");

                    ExecuteOrder(order, bestOrderJson.ToString());
                }
                else
                {
                    // Otherwise, add the sell order to the SortedSet with the positive score
                    db.SortedSetAdd("SELL", JsonConvert.SerializeObject(order), Convert.ToDouble(order.Price));
                    Console.WriteLine(
                        $"No matching Buy order found, Order added to SELL set: {JsonConvert.SerializeObject(order)}");
                }
            }

            static void ExecuteOrder(Order newOrder, string matchedOrderJson)
            {
                Console.WriteLine($"Executing Order: {newOrder.OrderId}, Matching Order: {matchedOrderJson}");

                var matchedOrder = JsonConvert.DeserializeObject<Order>(matchedOrderJson);

                using var connection = new NpgsqlConnection(connectionString);
                connection.Open();
                Console.WriteLine(
                    $"--- User newOrder.UserId {newOrder.UserId} matchedOrder.UserId {matchedOrder.UserId}");

                using NpgsqlTransaction transaction = connection.BeginTransaction();
                try
                {
                    // Check if the User exists
                    using var checkUserCmd =
                        new NpgsqlCommand("SELECT COUNT(*) FROM users WHERE Id = @UserId", connection);

                    checkUserCmd.Parameters.AddWithValue("UserId", newOrder.UserId);
                    var newOrderUserExists = (long)checkUserCmd.ExecuteScalar() > 0;

                    checkUserCmd.Parameters["UserId"].Value = matchedOrder.UserId;
                    var matchedOrderUserExists = (long)checkUserCmd.ExecuteScalar() > 0;

                    if (!newOrderUserExists || !matchedOrderUserExists)
                    {
                        Console.WriteLine(
                            $"User not found in database. newOrderUserExists {newOrderUserExists}  id {newOrder.UserId}matchedOrderUserExists {matchedOrderUserExists} id {matchedOrder.UserId} Skipping transaction.");
                        transaction.Rollback();
                        return; // exit early if user doesn't exist
                    }

                    // Check for duplicate order ID
                    using var checkOrderCmd = new NpgsqlCommand("SELECT COUNT(*) FROM orders WHERE OrderId = @OrderId",
                        connection);
                    checkOrderCmd.Parameters.AddWithValue("OrderId", newOrder.OrderId);
                    var orderExists = (long)checkOrderCmd.ExecuteScalar() > 0;

                    if (orderExists)
                    {
                        Console.WriteLine($"Order with ID {newOrder.OrderId} already exists. Skipping transaction.");
                        transaction.Rollback();
                        return; // exit early if order already exists
                    }

                    // Insert the orders
                     using var cmdInsertOrder = new NpgsqlCommand(
                    "INSERT INTO orders(OrderId, UserId, Symbol, Type, Quantity, Price) VALUES (@OrderId, @UserId, @Symbol, @Type, @Quantity, @Price);",
                    connection);
                    cmdInsertOrder.Parameters.AddWithValue("OrderId", newOrder.OrderId);
                    cmdInsertOrder.Parameters.AddWithValue("UserId", newOrder.UserId);
                    cmdInsertOrder.Parameters.AddWithValue("Symbol", newOrder.Symbol);
                    cmdInsertOrder.Parameters.AddWithValue("Type", (int)newOrder.Type); // Cast to int
                    cmdInsertOrder.Parameters.AddWithValue("Quantity", newOrder.Quantity);
                    cmdInsertOrder.Parameters.AddWithValue("Price", newOrder.Price);
                    Console.WriteLine($"Insertion new order OrderId {newOrder.OrderId} id {newOrder.UserId} Symbol {newOrder.Symbol} Price {newOrder.Price}.");
     
                    var newOrderId = cmdInsertOrder.ExecuteNonQuery();
                    Console.WriteLine($" Done");

                    if (newOrderId == 0)
                    {
                        Console.WriteLine("Failed to insert new order.");
                        transaction.Rollback();
                        return;
                    }

                    // Execute trade logic to modify balances
                    decimal transactionAmount = newOrder.Quantity * matchedOrder.Price;
                    if (newOrder.Type == OrderType.Buy)
                    {
                        // Adjust buyer and seller's balances
                        AdjustUserBalance(connection, newOrder.UserId, "HCNBalance",
                            newOrder.Quantity); // add HCN for buyer
                        AdjustUserBalance(connection, newOrder.UserId, "HDOBalance",
                            -transactionAmount); // remove HDO from buyer

                        AdjustUserBalance(connection, matchedOrder.UserId, "HCNBalance",
                            -matchedOrder.Quantity); // remove HCN from seller
                        AdjustUserBalance(connection, matchedOrder.UserId, "HDOBalance",
                            transactionAmount); // add HDO for seller
                    }
                    else
                    {
                        AdjustUserBalance(connection, newOrder.UserId, "HCNBalance",
                            -newOrder.Quantity); // remove HCN from seller
                        AdjustUserBalance(connection, newOrder.UserId, "HDOBalance",
                            transactionAmount); // add HDO for seller

                        AdjustUserBalance(connection, matchedOrder.UserId, "HCNBalance",
                            matchedOrder.Quantity); // add HCN for buyer
                        AdjustUserBalance(connection, matchedOrder.UserId, "HDOBalance",
                            -transactionAmount); // remove HDO from buyer
                    }

                    decimal marketPrice = FetchMarketPriceFromDatabase(connection);
                    InsertMarketPrice(connection, marketPrice);
                    // Create the transaction record

                    Console.WriteLine("insert new order.");
                    using var cmdTransaction = new NpgsqlCommand(
                        "INSERT INTO transactions(BuyerId, SellerId, Symbol, Quantity, Price, is_deleted) VALUES (@BuyerId, @SellerId, @Symbol, @Quantity, @Price, @IsDeleted) RETURNING TransactionId;",
                        connection);
                    cmdTransaction.Transaction = transaction;
                    cmdTransaction.Parameters.AddWithValue("BuyerId",
                        newOrder.Type == OrderType.Buy ? newOrder.UserId : matchedOrder.UserId);
                    cmdTransaction.Parameters.AddWithValue("SellerId",
                        newOrder.Type == OrderType.Buy ? matchedOrder.UserId : newOrder.UserId);
                    cmdTransaction.Parameters.AddWithValue("Symbol", newOrder.Symbol);
                    cmdTransaction.Parameters.AddWithValue("Quantity", newOrder.Quantity);
                    cmdTransaction.Parameters.AddWithValue("Price", newOrder.Price);
                    cmdTransaction.Parameters.AddWithValue("IsDeleted", false);

                    var transactionId = (int?)cmdTransaction.ExecuteScalar();

                    if (!transactionId.HasValue)
                    {
                        Console.WriteLine("Failed to insert transaction record.");
                        transaction.Rollback();
                        return;
                    }

                    transaction.Commit();
                    Console.WriteLine($"Transaction executed successfully with ID {transactionId}");

                    // Remove matched order from Redis
                    db.SortedSetRemove(newOrder.Type == OrderType.Buy ? "SELL" : "BUY", matchedOrderJson);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing order: {ex.Message} StackTrace {ex.StackTrace}");
                    transaction.Rollback();
                }
            }
            static void AdjustUserBalance(NpgsqlConnection connection, string userId, string column, decimal amount)
            {
                var cmdText = column switch
                {
                    "HCNBalance" => "UPDATE users SET HCNBalance = HCNBalance + @amount WHERE Id = @UserId",
                    "HDOBalance" => "UPDATE users SET HDOBalance = HDOBalance + @amount WHERE Id = @UserId",
                    _ => throw new ArgumentException("Invalid column name", nameof(column)),
                };

                using var cmd = new NpgsqlCommand(cmdText, connection);
                cmd.Parameters.AddWithValue("UserId", userId);
                cmd.Parameters.AddWithValue("amount", amount);
                var affectedRows = cmd.ExecuteNonQuery();

                if (affectedRows != 1)
                {
                    throw new Exception($"Failed to adjust balance for User {userId} in column {column}");
                }
            }

            static decimal FetchMarketPriceFromDatabase(NpgsqlConnection connection)
            {
                try
                {
                    using var cmd = new NpgsqlCommand(
                        "SELECT current_price FROM market_price ORDER BY market_id DESC LIMIT 1;",
                        connection);
                    var result = cmd.ExecuteScalar();

                    if (result != null && result != DBNull.Value)
                    {
                        return (decimal)result;
                    }
                    else
                    {
                        Console.WriteLine("No market price found in the database.");
                        // Return a default value or handle the error as needed
                        return 0.0m;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error fetching market price from the database: {ex.Message}");
                    // Return a default value or handle the error as needed
                    return 0.0m;
                }
            }

            static void PrintSortedSet(string setName)
            {
                var allOrders = db.SortedSetRangeByScoreWithScores(setName);
                Console.WriteLine($"--- {setName} Set ---");
                foreach (var order in allOrders)
                {
                    Console.WriteLine($"Order: {order.Element}, Score: {order.Score}");
                }
            }

            static void InsertMarketPrice(NpgsqlConnection connection, decimal marketPrice)
            {
                try
                {
                    using var cmd = new NpgsqlCommand(
                        "INSERT INTO market_price (current_price, last_price, change_time) VALUES (@CurrentPrice, @LastPrice, NOW());",
                        connection);
                    cmd.Parameters.AddWithValue("CurrentPrice", marketPrice);
                    cmd.Parameters.AddWithValue("LastPrice",
                        marketPrice); // Set last_price to the same value as current_price
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error inserting market price into the database: {ex.Message}");
                }
            }
        }
    }
}