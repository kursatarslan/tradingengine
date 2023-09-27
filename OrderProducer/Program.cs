using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Text;
using Npgsql;

public class User
{
    public string Id { get; set; }
    public decimal HDOBalance { get; set; }
    public decimal HCNBalance { get; set; }
}

public class Order
{
    public string UserId { get; set; }
    public Guid OrderId { get; set; }
    public string Symbol { get; set; }
    public decimal Quantity { get; set; }
    public decimal Price { get; set; }
    public OrderType Type { get; set; }
}

public enum OrderType
{
    Buy,
    Sell
}

public static class RandomExtensions
{
    public static decimal NextDecimal(this Random rand, decimal min, decimal max)
    {
        return (decimal)rand.NextDouble() * (max - min) + min;
    }
}


public class OrderProducer
{
    private static readonly Random random = new Random();
    private const decimal SomeSmallValue = 0.01m;

    public static void SendOrderToQueue(Order order)
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.QueueDeclare(queue: "engine1",
                             durable: false,
                             exclusive: false,
                             autoDelete: false,
                             arguments: null);

            string message = JsonConvert.SerializeObject(order);
            var body = Encoding.UTF8.GetBytes(message);

            channel.BasicPublish(exchange: "",
                             routingKey: "engine1",
                             basicProperties: null,
                             body: body);
            Console.WriteLine($"Order sent: {message}");
        }
    }

    public static Order GenerateAIOrder(User user, decimal currentMarketPrice, Queue<decimal> shortTermPrices, Queue<decimal> longTermPrices)
    {
        decimal shortTermAverage = shortTermPrices.Average();
        decimal longTermAverage = longTermPrices.Average();
        decimal confidence = Math.Abs(shortTermAverage - longTermAverage) / currentMarketPrice; 

        // Decision logic
        OrderType predictedSignal;
        decimal adviceFactor = 1;

        Console.WriteLine($"Short Term Average: {shortTermAverage}");
        Console.WriteLine($"Long Term Average: {longTermAverage}");
        Console.WriteLine($"Market Price: {currentMarketPrice}");
        Console.WriteLine($"Calculated Confidence: {confidence}");
        if(Math.Abs(shortTermAverage - longTermAverage) < SomeSmallValue)
        {
            // Either default to a specific OrderType or maybe randomize the OrderType.
            predictedSignal = random.Next(0, 2) == 0 ? OrderType.Buy : OrderType.Sell;
        }
        else
        {
            if (shortTermAverage > longTermAverage)
            {
                predictedSignal = OrderType.Buy;
                adviceFactor = 1 + (confidence * random.NextDecimal(0.01m, 0.05m));
            }
            else
            {
                predictedSignal = OrderType.Sell;
                adviceFactor = 1 - (confidence * random.NextDecimal(0.01m, 0.05m));
            }
        }

        // Ensure user has sufficient funds for the decision
        decimal quantity = random.Next(1, 10); 
        decimal price = currentMarketPrice * adviceFactor;

        if (predictedSignal == OrderType.Buy && user.HDOBalance < price * quantity)
        {
            Console.WriteLine($"Insufficient HDOBalance for User: {user.Id}. Cannot buy.");
            return null;
        }
        else if (predictedSignal == OrderType.Sell && user.HCNBalance < quantity)
        {
            Console.WriteLine($"Insufficient HCNBalance for User: {user.Id}. Cannot sell.");
            return null;
        }

        Console.WriteLine($"AI's Decision for User: {user.Id} - Signal: {predictedSignal}, Confidence: {confidence * 100:0.##}%, Price: {price}");

        return new Order
        {
            OrderId = Guid.NewGuid(),
            UserId = user.Id,
            Symbol = "HCN",
            Quantity = quantity,
            Price = price,
            Type = predictedSignal
        };
    }


    public static async Task Main()
    {
        Console.WriteLine("Order generation started. Press any key to stop.");
        string connectionString = "Host=localhost;Username=postgres;Password=postgres;Database=postgres";

        using var dbConnection = new NpgsqlConnection(connectionString);
        dbConnection.Open();

        try
        {
            decimal marketPrice = FetchMarketPriceFromDatabase(dbConnection);

            int shortTermPeriods = 10;
            int longTermPeriods = 30;
            Queue<decimal> shortTermMA = new Queue<decimal>(shortTermPeriods);
            Queue<decimal> longTermMA = new Queue<decimal>(longTermPeriods);

            while (!Console.KeyAvailable)
            {
                shortTermMA.Enqueue(marketPrice);
                longTermMA.Enqueue(marketPrice);

                if (shortTermMA.Count > shortTermPeriods)
                {
                    shortTermMA.Dequeue();
                }

                if (longTermMA.Count > longTermPeriods)
                {
                    longTermMA.Dequeue();
                }

                decimal shortTermAverage = shortTermMA.Average();
                decimal longTermAverage = longTermMA.Average();

                OrderType signal = OrderType.Buy; // Default to "Buy" signal
                if (shortTermAverage < longTermAverage)
                {
                    signal = OrderType.Sell; // Generate "Sell" signal
                }

                var users = FetchUsersFromDatabase(dbConnection);

                foreach (var user in users)
                {
                    Console.WriteLine("user   " + user.Id);
                    Order randomOrder = GenerateAIOrder(user, marketPrice, shortTermMA, longTermMA);

                    if (randomOrder != null)
                    {
                        SendOrderToQueue(randomOrder);
                        await Task.Delay(random.Next(100, 2000));
                    }
                }

                marketPrice = FetchMarketPriceFromDatabase(dbConnection);
            }

            Console.WriteLine("Order generation stopped.");
        }
        finally
        {
            dbConnection.Close();
        }
    }

    private static List<User> FetchUsersFromDatabase(NpgsqlConnection connection)
    {
        var users = new List<User>();
        using var command = new NpgsqlCommand("SELECT Id, HDOBalance, HCNBalance FROM users", connection);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            users.Add(new User
            {
                Id = reader.GetString(0),
                HDOBalance = reader.GetDecimal(1),
                HCNBalance = reader.GetDecimal(2)
            });
        }
        return users;
    }

        static decimal FetchMarketPriceFromDatabase(NpgsqlConnection connection)
        {
            try
            {
                using var cmd = new NpgsqlCommand(
                    "SELECT current_price FROM market_price ORDER BY change_time DESC LIMIT 1;",
                    connection);
                var result = cmd.ExecuteScalar();

                // Log the raw database response.
                Console.WriteLine($"Database response: {result}");

                var marketPrice = result != null ? (decimal)result : 0;

                // Check if market price is valid.
                if (marketPrice <= 0)
                    throw new Exception("Fetched invalid market price from database.");

                decimal fluctuation = (decimal)(Math.Sin(DateTime.UtcNow.Minute / 2.0) + 1) / 2.0m;
                marketPrice += fluctuation * 1.0m;
                Console.WriteLine($"Raw market price: {marketPrice}");
                return marketPrice;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching market price from the database: {ex.Message}");
                return 15.0m; // Default market price. This should be used only for troubleshooting, not in production.
            }
        }
}
