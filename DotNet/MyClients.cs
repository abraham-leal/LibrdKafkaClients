using System;
using Confluent.Kafka;

namespace LibrdKafkaProducer
{
    public class MyProducer
    {

        public static void Consumer()
        {
            var props = new ConsumerConfig
            {
                BootstrapServers = "<CCLOUD-ADDRESS>",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "<CCLOUD_API_KEY>",
                SaslPassword = "<CCLOUD_API_SECRET>",
                ClientId = "DotNetProducer",
                GroupId = "DotNetConsumer",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                SslCaLocation = "cacert.pem",
                StatisticsIntervalMs = 1000
            };

            using (var consumer = new ConsumerBuilder<string, string>(props)
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .Build())
            {
                consumer.Subscribe("CCloudTopic");
                try
                {
                    while (true)
                    {
                        var records = consumer.Consume();
                        Console.WriteLine($"Consumed record with key {records.Message.Key} " +
                            $"and value {records.Message.Value}");

                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("You have cancelled consumption");
                }
                finally
                {
                    consumer.Close();
                }
            }


        }

        public static void Producer()
        {
            var props = new ProducerConfig
            {
                BootstrapServers = "<CCLOUD-ADDRESS>",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "<CCLOUD_API_KEY>",
                SaslPassword = "<CCLOUD_API_SECRET>",
                ClientId = System.Net.Dns.GetHostName(),
                SslCaLocation = "cacert.pem"
            };

            Action<DeliveryReport<string, String>> onComplete = result =>
            Console.WriteLine(!result.Error.IsError
            ? $"Message Successfully Delivered" : $"Delivery Error: {result.Error.Reason}");

            using (var producer = new ProducerBuilder<string, string>(props).Build())
            {
                var targetAmount = 100;
                try
                {
                    for( int i = 0; i < targetAmount; i++ )
                    {
                        String UUID = System.Guid.NewGuid().ToString();
                        producer.Produce("CCloudTopic",
                        new Message<string, string> { Key = UUID,
                            Value = $"This is a cool value {UUID.Substring(UUID.Length - 2)}" },
                            onComplete);
                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
                finally
                {
                    producer.Flush(TimeSpan.FromSeconds(10));
                    producer.Dispose();
                }

            }
        }

        public static void Main(string[] args)
        {
            var mode = args[0];

            switch (mode)
            {
                case "produce":
                    Producer();
                    break;
                case "consume":
                    Consumer();
                    break;
                default:
                    Console.WriteLine("Not an acceptable parameter. Acceptable parameters are: produce|consume");
                    break;
            }

        }

    }
}
