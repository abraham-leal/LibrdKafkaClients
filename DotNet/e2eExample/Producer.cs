using System;
using Confluent.encryption_demo;
using Confluent.Encryption.Serializer.Crypto.Serializer.Configuration;
using Confluent.Encryption.Serializer.Crypto.Serializer.Configuration.SchemaRegistry;
using Confluent.Encryption.Serializer.Crypto.Serializer.SchemaRegistry;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace encryption_demo

{
    public class Producer
    {
        public static CachedSchemaRegistryClient GetSchemaClient()
        {
            var schemaProps = new SchemaRegistryConfig
            {
                Url = "",
                BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                BasicAuthUserInfo = "", 
                SslCaLocation = "" 
            };
            
            return new CachedSchemaRegistryClient(schemaProps);
        }

        public static AvroFieldEncryptionConfigurationBuilder<User> GetEncryptedAvroConfig(string encryptionType)
        {
            return AvroFieldEncryptionConfigurationBuilder<User>
                .NewBuilder()
                .WithSchemaRegistry(GetSchemaClient())
                .WithConfigurationName(encryptionType)
                .WithConfigurationProvider(new SrConfigurationProvider(GetSchemaClient()));
        }

        private static void Produce(string encryptionType)
        {
            var props = new ProducerConfig
            {
                BootstrapServers = "", 
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "",
                SaslPassword = "", 
                ClientId = System.Net.Dns.GetHostName(),
                SslCaLocation = "probe"
            };

            IAsyncSerializer<User> UserSerializer = GetEncryptedAvroConfig(encryptionType)
                .BuildSerde<SecuredSerializers.SecuredAvro<User>>();
            
            string topic;

            if (encryptionType == "payload-encryption-config")
            {
                topic = "dotnet-payload-encrypted";
            }
            else
            {
                topic = "dotnet-field-encrypted";
            }
            
            using (var producer = new ProducerBuilder<Null, User>(props)
                .SetValueSerializer(UserSerializer)
                .Build())
            {
                var targetAmount = 1;
                try
                {
                    Random rnd = new Random();
                    for (int i = 0; i < targetAmount; i++)
                    {
                        var thisUser = new User
                        {
                            address = rnd.Next().ToString(),
                            name = "Abraham",
                            favorite_color = "Blue",
                            ethnicity = "Hispanic"
                        };

                        producer.ProduceAsync(topic, new Message<Null, User> {Value = thisUser})
                            .ContinueWith(task =>
                            {
                                if (!task.IsFaulted)
                                {
                                    Console.WriteLine($"produced to: {task.Result.TopicPartitionOffset}");
                                    return;
                                }
                                Console.WriteLine($"error producing message: {task.Exception.InnerException}");
                            });
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
            var fieldEncryptionRef = "field-encryption-configs";
            var payloadEncryptionRef = "payload-encryption-config";

            var mode = args[0];
            var encryptionType = args[1];

            string effectiveType;

            if (encryptionType == "payload")
            {
                effectiveType = payloadEncryptionRef;
            } else if (encryptionType == "field")
            {
                effectiveType = fieldEncryptionRef;
            }
            else
            {
                Console.WriteLine("Invalid or null encryption type. Must be one of: payload, field.");
                return;
            }
            switch (mode)
            {
                case "produce":
                    Produce(effectiveType);
                    break;
                case "consume":
                    Consumer.Consume(effectiveType);
                    break;
                default:
                    Console.WriteLine("Parameter given: " + mode);
                    Console.WriteLine("Not an acceptable parameter. Acceptable parameters are: produce|consume");
                    break;
            }
        }

    }
}
