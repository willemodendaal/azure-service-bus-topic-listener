using System;
using System.IO;
using System.Text.RegularExpressions;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json.Linq;

namespace ServiceBusTopicListener
{
	class Program
	{
		
		static void Main(string[] args)
		{
			if (!ArgsAreValid(args))
			{
				OutputProgramIntroText();
				Console.WriteLine("Press Enter to exit...");
				Console.ReadLine();
				return;
			}

			string topicName = args[0];
			string subscriptionName = args[1];
			string connectionString = args[2];

			try
			{
				var subscriptionClient = SubscriptionClient.CreateFromConnectionString(connectionString, topicName, subscriptionName, ReceiveMode.ReceiveAndDelete);
				var options = new OnMessageOptions
				{
					AutoComplete = true,
					MaxConcurrentCalls = 1
				};

				Console.WriteLine($"Listening for messages on topic '{topicName}'...");
				Console.WriteLine("Press Ctrl+C to exit.");
				subscriptionClient.OnMessage(ReceiveMessage, options);
				Console.ReadLine();
			}
			catch (Exception exc)
			{
				Console.WriteLine($"General error. Exiting program.\n\nError Message: '{exc.Message}'\n\nStackTrace: '{exc.StackTrace}'");
			}
		}

		private static void OutputProgramIntroText()
		{
			Console.WriteLine("ServiceBusTopicListener");
			Console.WriteLine("Usage: ServiceBusTopicListener.exe <topicName> <subscriptionName> <connectionString>");
			Console.WriteLine(
				"eg: ServiceBusTopicListener.exe my_topic my_subscription Endpoint=sb://mysb.servicebus.windows.net/;SharedAccessKeyName=tempReaderAccessKey;SharedAccessKey=xxxxx");
		}

		private static void ReceiveMessage(BrokeredMessage receivedMessage)
		{
			try
			{
				var stream = receivedMessage.GetBody<Stream>();
				var reader = new StreamReader(stream);
				var s = reader.ReadToEnd();
				var jsonMsg = JObject.Parse(s);
				Console.WriteLine("Got msg: " + jsonMsg + "\n\n");
			}
			catch (Exception ex)
			{
				Console.WriteLine("*** Error reading individual message: " + ex.Message);
			}
		}

		private static bool ArgsAreValid(string[] args)
		{
			if (args == null || args.Length != 3)
			{
				return false;
			}
			
			if (!Regex.IsMatch(args[0], ValidTopicNamePattern))
			{
				Console.WriteLine($"Invalid topic name '{args[0]}'.\n\n");
				return false;
			}

			if (!Regex.IsMatch(args[1], SubscriptionNamePattern))
			{
				Console.WriteLine($"Invalid subscription name '{args[1]}'.\n\n");
				return false;
			}

			if (!Regex.IsMatch(args[2], ValidConnectionStringPattern))
			{
				Console.WriteLine($"Invalid service bus connection string '{args[2]}'.\n\n");
				return false;
			}

			return true;
		}

		private static string ValidTopicNamePattern = "^[a-zA-Z0-9_\\-.]+$";
		private static string SubscriptionNamePattern = "^[a-zA-Z0-9_\\-.]+$";
		private static string ValidConnectionStringPattern = "^Endpoint.+SharedAccessKeyName.+SharedAccessKey.+=$";
	}
}
