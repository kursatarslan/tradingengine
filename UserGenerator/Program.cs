using System;
using System.Text;
using Newtonsoft.Json;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using StackExchange.Redis;
using System.Linq;
using Newtonsoft.Json.Converters;

string connectionString = "Host=localhost;Username=postgres;Password=postgres;Database=postgres";
using var connection = new NpgsqlConnection(connectionString);
connection.Open();

using var cmd = new NpgsqlCommand();
cmd.Connection = connection;

for (int i = 1; i <= 100; i++)
{
    var userId = $"User{i}";
    var hdoBalance = 1000; // You can set a default balance or randomize
    var hcnBalance = 1000; // You can set a default balance or randomize

    cmd.CommandText = "INSERT INTO users (Id, HDOBalance, HCNBalance) VALUES (@Id, @HDOBalance, @HCNBalance)";
    cmd.Parameters.AddWithValue("Id", userId);
    cmd.Parameters.AddWithValue("HDOBalance", hdoBalance);
    cmd.Parameters.AddWithValue("HCNBalance", hcnBalance);

    cmd.ExecuteNonQuery();
    cmd.Parameters.Clear();
}

connection.Close();