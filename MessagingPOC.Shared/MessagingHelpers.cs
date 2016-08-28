using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace MessagingPOC.Shared
{
    public class MessagingHelpers
    {
        public static OnMessageOptions CreateMessageOptions(int concurrency = 5,bool autoComplete = false,int autoRenewTime = 30)
        {
            return new OnMessageOptions
            {
                MaxConcurrentCalls = concurrency,//set this to 1 if guaranteed ordering of messages is required or alter message creation strategy
                //max time that a lock can be held for taking into account renewals
                AutoRenewTimeout = TimeSpan.FromSeconds(autoRenewTime),
                AutoComplete = autoComplete
            };
        }

        public static void ProcessMessage(BrokeredMessage message)
        {
            try
            {
                processMessage(message);
            }
            catch (MessageLockLostException e)
            {
                //TODO test if this actually is a valid strategy
                message.RenewLock();
                processMessage(message);
            }
            catch (Exception e)
            {
                message.DeadLetter("Error processing Message", e.Message);
                Console.Write("Error processing message sending to deadletter queue");
            }
        }

        private static void processMessage(BrokeredMessage message)
        {
            string target = message.Label;
            string text = message.GetBody<string>();

            if(text == "error")
                throw new Exception("error caused by user input");

            Console.WriteLine(target + ">" + text);
            message.Complete();
        }
    }
}
