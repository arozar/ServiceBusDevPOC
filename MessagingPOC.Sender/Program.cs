using System;
using System.Collections.Generic;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace MessagingPOC.Sender
{
    class Program
    {
        static string ConnectionString = "";
        static string TopicPath = "basetopic";

        static List<string> Subscriptions = new List<string>
        {
            "Worker1","Worker2","Worker3"
        };

        static void Main(string[] args)
        {
#if DEBUG
            //if in dev scope to the current machine
            TopicPath = $"{TopicPath}-{Environment.MachineName}";
#endif
            // Create a namespace manager to manage artifacts
            var manager = NamespaceManager.CreateFromConnectionString(ConnectionString);

            // Create a topic if it does not exist
            if (!manager.TopicExists(TopicPath))
            {
                var topicDescription = manager.CreateTopic(TopicPath);
#if DEBUG
                topicDescription.AutoDeleteOnIdle = TimeSpan.FromHours(1);
#endif
            }
            int workerNumber = 1;
            // Create subscriptions
            Subscriptions.ForEach(subscriptionName =>
            {
                var description = new SubscriptionDescription(TopicPath, subscriptionName);

#if DEBUG
                //if this is for dev then delete the queue/subscription when its not being used
                description.AutoDeleteOnIdle = TimeSpan.FromHours(1);
#endif
                //if the filter is changed will need to delete it from the topic.
                //Could test update function
                if (!manager.SubscriptionExists(TopicPath, subscriptionName))
                {
                    //create with filter
                    manager.CreateSubscription(description,new SqlFilter($"Target = 'Worker{workerNumber}' OR Target = 'Any'"));
                }
                workerNumber++;
            });

            var factory = MessagingFactory.CreateFromConnectionString(ConnectionString);
            var topicClient = factory.CreateTopicClient(TopicPath);

            if (!manager.SubscriptionExists(TopicPath,"Log"))
            {
                var loggingSubscriptionDescription = new SubscriptionDescription(TopicPath, "Log");
#if DEBUG
                //if this is for dev then delete the queue/subscription when its not being used
                loggingSubscriptionDescription.AutoDeleteOnIdle = TimeSpan.FromHours(1);
#endif
                manager.CreateSubscription(loggingSubscriptionDescription, new SqlFilter("Target = 'Log'"));
            }
            
            var subscriptionClient = factory.CreateSubscriptionClient(TopicPath, "Log");

            // Create a message pump for receiving messages
            subscriptionClient.OnMessage(msg => ProcessMessage(msg));

            // Send a message to all subscriptions just created
            var helloMessage = new BrokeredMessage("Sender initialized...");
            helloMessage.Properties.Add("Target","Any");
            helloMessage.Label = "Publisher";
            topicClient.Send(helloMessage);

            while (true)
            {
                Console.WriteLine("Select target: 1 - Worker 1, 2 - Worker 2, 3 - Worker 3, anything else for all");

                var target = Console.ReadLine();

                Console.WriteLine("Enter message");

                string text = Console.ReadLine();
                
                if (text.Equals("exit")) break;

                string messageTarget = "";

                switch (target)
                {
                    case "1":
                        messageTarget = "Worker1";
                        break;
                    case "2":
                        messageTarget = "Worker2";
                        break;
                    case "3":
                        messageTarget = "Worker3";
                        break;
                    default:
                        messageTarget = "Any";
                        break;
                }

                // Send a chat message
                var brokeredMessage = new BrokeredMessage(text);
                //add filterable property
                brokeredMessage.Properties.Add("Target", messageTarget);

                brokeredMessage.Label = messageTarget;

                topicClient.Send(brokeredMessage);
            }
            // Send a message to say you are leaving
            var shutDownMessage = new BrokeredMessage("Shutting down...");
            shutDownMessage.Label = "Publisher";
            topicClient.Send(shutDownMessage);

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
