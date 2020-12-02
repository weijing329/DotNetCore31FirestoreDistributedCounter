using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Cloud.Firestore;

namespace DotNetCore31FirestoreDistributedCounter
{
  class Program
  {
    private static readonly Random s_rand = new Random();
    private static readonly object s_randLock = new object();
    private const string Usage = @"Usage:
    dotnet run command PROJECT_ID COUNTER_NAME [NUMBER_OF_SHARDS=100]

    Where command is one of
        initialize-distributed-counter
        test-distributed-counter
";

    /// <summary>
    /// Shard is a document that contains the count.
    /// </summary>
    [FirestoreData]
    public class Shard
    {
      [FirestoreProperty(name: "count")]
      public int Count { get; set; }
    }

    /// <summary>
    /// Create a given number of shards as a
    /// subcollection of specified document.
    /// </summary>
    /// <param name="docRef">The document reference <see cref="DocumentReference"/></param>
    private static async Task CreateCounterAsync(DocumentReference docRef, int numOfShards)
    {
      CollectionReference colRef = docRef.Collection("shards");
      var tasks = new List<Task>();
      // Initialize each shard with Count=0
      for (var i = 0; i < numOfShards; i++)
      {
        tasks.Add(colRef.Document(i.ToString()).SetAsync(new Shard() { Count = 0 }));
      }
      await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Increment a randomly picked shard by 1.
    /// </summary>
    /// <param name="docRef">The document reference <see cref="DocumentReference"/></param>
    /// <returns>The <see cref="Task"/></returns>
    private static async Task IncrementCounterAsync(DocumentReference docRef, int numOfShards)
    {
      int documentId;
      lock (s_randLock)
      {
        documentId = s_rand.Next(numOfShards);
      }
      var shardRef = docRef.Collection("shards").Document(documentId.ToString());
      await shardRef.UpdateAsync("count", FieldValue.Increment(1));
    }

    /// <summary>
    /// Get total count across all shards.
    /// </summary>
    /// <param name="docRef">The document reference <see cref="DocumentReference"/></param>
    /// <returns>The <see cref="int"/></returns>
    private static async Task<int> GetCountAsync(DocumentReference docRef)
    {
      var snapshotList = await docRef.Collection("shards").GetSnapshotAsync();
      return snapshotList.Sum(shard => shard.GetValue<int>("count"));
    }

    static void Main(string[] args)
    {
      if (args.Length < 3)
      {
        Console.Write(Usage);
        return;
      }
      string command = args[0].ToLower();

      string projectId = args[1];

      string counterName = args[2];

      int numberOfShards = 100;
      if (args.Length < 3)
      {
        try
        {
          numberOfShards = int.Parse(args[3]);
          if (numberOfShards < 1)
          {
            throw new ArgumentOutOfRangeException();
          }
        }
        catch (System.Exception)
        {
          Console.WriteLine("ERROR: NUMBER_OF_SHARDS must be positive integer value");
          Environment.Exit(-1);
        }
      }

      try
      {
        FirestoreDb db = FirestoreDb.Create(projectId);

        var docRef = db.Collection("DistributedCounters").Document(counterName);

        switch (command)
        {
          case "initialize-distributed-counter":
            Task.Run(() => CreateCounterAsync(docRef, numberOfShards)).WaitWithUnwrappedExceptions();
            Console.WriteLine("Distributed counter created.");
            break;

          case "test-distributed-counter":
            Stopwatch stopwatch = Stopwatch.StartNew();
            var countValue = Task.Run(() => GetCountAsync(docRef)).ResultWithUnwrappedExceptions();
            stopwatch.Stop();
            Console.WriteLine($"Get count value: {countValue}, in {stopwatch.ElapsedMilliseconds} ms.");

            stopwatch = Stopwatch.StartNew();
            Task.Run(() => IncrementCounterAsync(docRef, numberOfShards)).WaitWithUnwrappedExceptions();
            stopwatch.Stop();
            Console.WriteLine($"Distributed counter incremented, in {stopwatch.ElapsedMilliseconds} ms.");

            stopwatch = Stopwatch.StartNew();
            countValue = Task.Run(() => GetCountAsync(docRef)).ResultWithUnwrappedExceptions();
            stopwatch.Stop();
            Console.WriteLine($"Get count value: {countValue}, in {stopwatch.ElapsedMilliseconds} ms.");
            break;

          default:
            Console.Write(Usage);
            return;
        }
      }
      catch (System.Exception ex)
      {
        Console.WriteLine($"ERROR: {ex.Message}");
        Environment.Exit(-1);
      }
    }
  }
}
