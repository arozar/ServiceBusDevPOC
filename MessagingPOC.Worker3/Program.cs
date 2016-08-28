﻿using System;
using System.Collections.Generic;
using Microsoft.ServiceBus.Messaging;

namespace MessagingPOC.Worker3
{
    class Program
    {
        //ToDo: Enter a valid Serivce Bus connection string
        static string ConnectionString =
            "";

        static string TopicPath = "basetopic";

        private static string subscriptionName = "Worker3";

        static void Main(string[] args)
        {
#if DEBUG
            //if in dev scope to the current machine
            TopicPath = $"{TopicPath}-{Environment.MachineName}";
#endif
            // Create clients
            var factory = MessagingFactory.CreateFromConnectionString(ConnectionString);
            var topicClient = factory.CreateTopicClient(TopicPath);
            
            var subscriptionClient = factory.CreateSubscriptionClient(TopicPath, subscriptionName);

            // Create a message pump for receiving messages
            subscriptionClient.OnMessage(msg => ProcessMessage(msg));

            // Send a message to anyone listening for log info
            var readyMessage = new BrokeredMessage($"{subscriptionName} Ready...");
            readyMessage.Label = subscriptionName;
            readyMessage.Properties.Add("Target", "Log");
            topicClient.Send(readyMessage);

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
            var goodbyeMessage = new BrokeredMessage("Exiting...");
            goodbyeMessage.Label = subscriptionName;
            goodbyeMessage.Properties.Add("Target", "Log");
            topicClient.Send(goodbyeMessage);

            // Close the factory and the clients it created
            factory.Close();
        }

        static void ProcessMessage(BrokeredMessage message)
        {
            string target = message.Label;
            string text = message.GetBody<string>();

            Console.WriteLine(target + ">" + text);
        }
    }
}