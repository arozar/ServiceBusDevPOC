using System;
using MessagingPOC.Shared;
using Microsoft.ServiceBus.Messaging;

namespace MessagingPOC.Worker1
{
    class Program
    {

        static string TopicPath = "basetopic";

        private static string subscriptionName = "Worker1";

        static void Main(string[] args)
        {
#if DEBUG
            //if in dev scope to the current machine
            TopicPath = $"{TopicPath}-{Environment.MachineName}";
#endif
            // Create clients
            var factory = MessagingFactory.CreateFromConnectionString(Config.ConnectionString);
            var topicClient = factory.CreateTopicClient(TopicPath);
            var subscriptionClient = factory.CreateSubscriptionClient(TopicPath, subscriptionName);

            // Create a message pump for receiving messages
            subscriptionClient.OnMessage(MessagingHelpers.ProcessMessage,MessagingHelpers.CreateMessageOptions());

            // Send a message to all anyone listening for logging info
            var helloMessage = new BrokeredMessage($"{subscriptionName} Ready...");
            helloMessage.Label = subscriptionName;
            helloMessage.Properties.Add("Target", "Log");
            topicClient.Send(helloMessage);

            while (true)
            {
                string text = Console.ReadLine();
                if (text.Equals("exit")) break;

                // Send a chat message
                var responseMessage = new BrokeredMessage(text);
                responseMessage.Label = subscriptionName;
                topicClient.Send(responseMessage);

            }

            // Send a message to say you are leaving
            var goodbyeMessage = new BrokeredMessage($"{subscriptionName}Exiting...");
            goodbyeMessage.Label = subscriptionName;
            topicClient.Send(goodbyeMessage);

            // Close the factory and the clients it created
            factory.Close();
        }
    }
}
