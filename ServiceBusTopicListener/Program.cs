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
			string connectionString = args[1];

			try
			{
				var subscriptionClient = SubscriptionClient.CreateFromConnectionString(connectionString, topicName, "testApp1", ReceiveMode.ReceiveAndDelete);
				var options = new OnMessageOptions
				{
					AutoComplete = true,
					MaxConcurrentCalls = 1
				};

				Console.WriteLine($"Listening for messages on topic '{topicName}'...");
				Console.WriteLine("Press Ctrl+C to exit.");
				subscriptionClient.OnMessage(ReceiveMessage, options);
			}
			catch (Exception exc)
			{
				Console.WriteLine($"General error. Exiting program.\n\nError Message: '{exc.Message}'\n\nStackTrace: '{exc.StackTrace}'");
			}
			Console.WriteLine("Press Enter to exit...");
			Console.ReadLine();
		}

		private static void OutputProgramIntroText()
		{
			Console.WriteLine("ServiceBusTopicListener");
			Console.WriteLine("Usage: ServiceBusTopicListener.exe <topicName> <connectionString>");
			Console.WriteLine(
				"eg: ServiceBusTopicListener.exe my_topic Endpoint=sb://mysb.servicebus.windows.net/;SharedAccessKeyName=tempReaderAccessKey;SharedAccessKey=xxxxx");
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
			if (args == null || args.Length != 2)
			{
				return false;
			}
			
			if (!Regex.IsMatch(args[0], ValidTopicNamePattern))
			{
				Console.WriteLine($"Invalid topic name '{args[0]}'.\n\n");
				return false;
			}

			if (!Regex.IsMatch(args[1], ValidConnectionStringPattern))
			{
				Console.WriteLine($"Invalid service bus connection string '{args[1]}'.\n\n");
				return false;
			}

			return true;
		}

		private static string ValidTopicNamePattern = "^[a-zA-Z0-9_\\-.]+$";
		private static string ValidConnectionStringPattern = "^Endpoint.+SharedAccessKeyName.+SharedAccessKey.+=$";
	}
}
