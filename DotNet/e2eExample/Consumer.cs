﻿using System;
using Confluent.encryption_demo;
using Confluent.Encryption.Serializer.Crypto.Serializer.SchemaRegistry;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;

namespace encryption_demo

{
    public class Consumer
    {
        public static void Consume(string encryptionType)
        {
            var props = new ConsumerConfig
            {
                BootstrapServers = "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "AUGGUQ6G57NWNNOT",
                SaslPassword = "CHFVXF4ZhZdOa1EpR/VkYPjxwDZJsPYA8oNNvlneDMjGGE/R8XMmEn7siD9zumX5",
                ClientId = "DotNetConsumer",
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                SslCaLocation = "probe",
            };
            
            IAsyncDeserializer<User> UserDeserializer = Producer.GetEncryptedAvroConfig(encryptionType)
                .BuildSerde<SecuredDeserializers.SecuredAvro<User>>();
            
            string topic;

            if (encryptionType == "payload-encryption-config")
            {
                topic = "dotnet-payload-encrypted";
            }
            else
            {
                topic = "dotnet-field-encrypted";
            }

            using (var consumer = new ConsumerBuilder<string, User>(props)
                .SetValueDeserializer(UserDeserializer.AsSyncOverAsync())
                .Build())
            {
                consumer.Subscribe(topic);
                try
                {
                    while (true)
                    {
                        var records = consumer.Consume();
                        Console.WriteLine($"Consumed record with key {records.Message.Key} " +
                            $"and value for name: {records.Message.Value.name} and address: {records.Message.Value.address}");

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
    }
}