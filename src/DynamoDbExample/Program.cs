using System;
using System.Collections.Generic;
using System.IO;

using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

using Microsoft.Extensions.Configuration;

namespace DynamoDbExample
{
    public class Program
    {
        private static string tableName;
        private static AmazonDynamoDBClient client;
        private static IConfigurationRoot _configuration;

        public static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Path.GetFullPath(Directory.GetCurrentDirectory()))
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            _configuration = builder.Build();

            // be sure to use an existing DynamoDBv2 table; for this to run the PK must be named 'id' and be a string.
            tableName = _configuration["aws:tableName"];

            try
            {
                client = new AmazonDynamoDBClient(
                    _configuration["aws:accessKeyId"],
                    _configuration["aws:secretAccessKey"],
                    RegionEndpoint.GetBySystemName(_configuration["aws:region"]));

                var itemId = CreateItem();
                RetrieveItem(itemId);

                // Perform various updates.
                UpdateMultipleAttributes(itemId);
                UpdateExistingAttributeConditionally(itemId);

                // Delete item.
                DeleteItem(itemId);
                Console.WriteLine("To continue, press Enter");
                Console.ReadLine();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine("To continue, press Enter");
                Console.ReadLine();
            }
        }

        private static string CreateItem()
        {
            var request = new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>()
                {
                    { "id", new AttributeValue { S = Guid.NewGuid().ToString() } },
                    { "Title", new AttributeValue { S = "Book 201 Title" } },
                    { "ISBN", new AttributeValue { S = "11-11-11-11" } },
                    { "Authors", new AttributeValue { SS = new List<string> { "Author1", "Author2" } } },
                    { "Price", new AttributeValue { N = "20.00" } },
                    { "Dimensions", new AttributeValue { S = "8.5x11.0x.75" } },
                    { "InPublication", new AttributeValue { BOOL = false } }
                }
            };

            var result = client.PutItemAsync(request).Result;
            return request.Item["id"].S;
        }

        private static void RetrieveItem(string id)
        {
            var request = new GetItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue>()
                {
                    { "id", new AttributeValue { S = id } }
                },
                ProjectionExpression = "id, ISBN, Title, Authors",
                ConsistentRead = true
            };

            var response = client.GetItemAsync(request).Result;

            // Check the response.
            var attributeList = response.Item; // attribute list in the response.
            Console.WriteLine("\nPrinting item after retrieving it ............");
            PrintItem(attributeList);
        }

        private static void UpdateMultipleAttributes(string id)
        {
            var request = new UpdateItemRequest
            {
                Key = new Dictionary<string, AttributeValue>()
                {
                  { "id", new AttributeValue { S = id } }
                },

                // Perform the following updates:
                // 1) Add two new authors to the list
                // 1) Set a new attribute
                // 2) Remove the ISBN attribute
                ExpressionAttributeNames = new Dictionary<string, string>()
                {
                    { "#A", "Authors" },
                    { "#NA", "NewAttribute" },
                    { "#I", "ISBN" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    { ":auth", new AttributeValue { SS = { "Author YY", "Author ZZ" } } },
                    { ":new", new AttributeValue { S = "New Value" } }
                },

                UpdateExpression = "ADD #A :auth SET #NA = :new REMOVE #I",

                TableName = tableName,
                ReturnValues = "ALL_NEW" // Give me all attributes of the updated item.
            };
            var response = client.UpdateItemAsync(request).Result;

            // Check the response.
            var attributeList = response.Attributes; // attribute list in the response.
            // print attributeList.
            Console.WriteLine("\nPrinting item after multiple attribute update ............");
            PrintItem(attributeList);
        }

        private static void UpdateExistingAttributeConditionally(string id)
        {
            var request = new UpdateItemRequest
            {
                Key = new Dictionary<string, AttributeValue>()
                {
                    { "id", new AttributeValue { S = id } }
                },
                ExpressionAttributeNames = new Dictionary<string, string>()
                {
                    { "#P", "Price" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    { ":newprice", new AttributeValue { N = "22.00" } },
                    { ":currprice", new AttributeValue { N = "20.00" } }
                },

                // This updates price only if current price is 20.00.
                UpdateExpression = "SET #P = :newprice",
                ConditionExpression = "#P = :currprice",

                TableName = tableName,
                ReturnValues = "ALL_NEW" // Give me all attributes of the updated item.
            };
            var response = client.UpdateItemAsync(request).Result;

            // Check the response.
            var attributeList = response.Attributes; // attribute list in the response.
            Console.WriteLine("\nPrinting item after updating price value conditionally ............");
            PrintItem(attributeList);
        }

        private static void DeleteItem(string id)
        {
            var request = new DeleteItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue>()
                {
                    { "id", new AttributeValue { S = id } }
                },

                // Return the entire item as it appeared before the update.
                ReturnValues = "ALL_OLD",
                ExpressionAttributeNames = new Dictionary<string, string>()
                {
                    { "#IP", "InPublication" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    { ":inpub", new AttributeValue { BOOL = false } }
                },
                ConditionExpression = "#IP = :inpub"
            };

            var response = client.DeleteItemAsync(request).Result;

            // Check the response.
            var attributeList = response.Attributes; // Attribute list in the response.
            // Print item.
            Console.WriteLine("\nPrinting item that was just deleted ............");
            PrintItem(attributeList);
        }

        private static void PrintItem(Dictionary<string, AttributeValue> attributeList)
        {
            foreach (KeyValuePair<string, AttributeValue> kvp in attributeList)
            {
                string attributeName = kvp.Key;
                AttributeValue value = kvp.Value;

                Console.WriteLine(
                    attributeName + " " +
                    (value.S == null ? string.Empty : "S=[" + value.S + "]") +
                    (value.N == null ? string.Empty : "N=[" + value.N + "]") +
                    (value.SS == null ? string.Empty : "SS=[" + string.Join(",", value.SS.ToArray()) + "]") +
                    (value.NS == null ? string.Empty : "NS=[" + string.Join(",", value.NS.ToArray()) + "]"));
            }

            Console.WriteLine("************************************************");
        }
    }
}
