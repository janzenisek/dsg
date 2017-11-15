using DST.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using uPLibrary.Networking.M2Mqtt;

namespace DST.GeneratorNet {
  public class Program {
    static void Main(string[] args) {
      Console.WriteLine("DataStreamGenerator v1.0");
      Console.WriteLine("------------------------\n");

      // parse configuration toml file
      string tomlFile = @"\configs\DSG_conf2.toml";
      Console.WriteLine($"parsing configuration file: {tomlFile}");
      var generatorConfig = ConfigurationParser.GetGeneratorConfig(tomlFile);
      var configDict = ConfigurationParser.GetSeriesConfigurations(tomlFile);
      var seriesDict = new Dictionary<string, DoubleSeries>();

      // convert to taskable jobs
      Console.WriteLine("convert configuration to jobs");
      foreach(var sc in configDict) {
        if(sc.Value.GetType() == typeof(ARSeriesConfig) || sc.Value.GetType() == typeof(ARMASeriesConfig) || sc.Value.GetType() == typeof(PFSeriesConfig)) {
          seriesDict.Add(sc.Key, new DoubleSeries());
        }
      }

      // setup environment
      Random rnd;
      if (generatorConfig.Seed >= 0) rnd = new Random(generatorConfig.Seed);
      else rnd = new Random();
      Random groupNameRnd = new Random(); // if ENV=development
      var group = generatorConfig.Id + "_" + groupNameRnd.Next(1, 10000);
      var brokerHostName = generatorConfig.BrokerHostName;

      // Stream or Generate
      switch (generatorConfig.Type.Trim().ToLower()) {
        case "stream":
          Stream(generatorConfig, group, seriesDict, configDict, rnd);
          break;
        case "generate":
          Generate(generatorConfig, seriesDict, rnd);
          break;
      }
    }

    static void Stream(GeneratorConfig generatorConfig, string group, Dictionary<string, DoubleSeries> seriesDict, Dictionary<string, SeriesConfig> configDict, Random rnd) {
      var tasks = new List<Task>();
      var tokenSource = new CancellationTokenSource();
      var token = tokenSource.Token;
      
      foreach(var item in seriesDict) {
        var task = Task.Factory.StartNew(() => {
          MqttClient client = new MqttClient(generatorConfig.BrokerHostName);
          try {
            client.Connect(Guid.NewGuid().ToString());
            var job = new StreamingJob() { Client = client, Token = token };
            var generator = new DoubleSeriesGenerator(group, generatorConfig, seriesDict, rnd, item.Value, configDict[item.Key]);
            generator.StreamSeries(job);
          }
          finally {
            client.Disconnect();
          }
        }, token);
        tasks.Add(task);
      }


      string input = null;
      bool completed = false;
      var sw = new Stopwatch();
      sw.Start();

      try {
        do {
          input = Reader.ReadLine(500);
          completed = Task.WaitAll(tasks.ToArray(), 10);
          if(sw.ElapsedMilliseconds > generatorConfig.Duration) {
            tokenSource.Cancel();
            Task.WaitAll(tasks.ToArray());
            completed = true;
          }
        } while (input == null && !completed);
        if (completed) {
          Console.WriteLine("\nGenerator's runtime is over. The generator calls it a night.");
        } else {
          tokenSource.Cancel();
          Task.WaitAll(tasks.ToArray());
        }
      }
      catch (AggregateException e) {
        Console.WriteLine("\nThe generator tasks has been cancelled. The generator rests.");
      }
      finally {
        tokenSource.Dispose();
      }
    }

    static void Generate(GeneratorConfig generatorConfig, Dictionary<string, DoubleSeries> series, Random rnd) {

    }
  }
}
