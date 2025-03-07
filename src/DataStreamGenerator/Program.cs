using DSG.Configuration;
using DSG.Utils;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using System;
using System.Diagnostics;
using System.Text;
using org.mariuszgromada.math.mxparser;

namespace DSG {
  public class Program {
    const string TYPE_STREAM_DATETIMEBASED_SINGLETHREADED = "stream1";
    const string TYPE_STREAM_DATETIMEBASED_MULTITASKED = "stream2";
    const string TYPE_GENERATE_DATETIMEBASED = "generate1";
    const string TYPE_GENERATE_EVENTCOUNTBASED = "generate2";

    static void Main(string[] args) {
      Console.WriteLine("DataStreamGenerator v0.1");
      Console.WriteLine("------------------------\n");

      var configFiles = new List<string>();
      if (args.Length > 0) {
        foreach (var path in args) {
          if (File.Exists(path)) {
            if (Path.GetExtension(path) == ".toml") {
              configFiles.Add(path);
            }
            else {
              Console.WriteLine($"Error: {path} does not point to a '.toml' file.");
            }
          }
          else {
            Console.WriteLine($"Error: {path} does not point to an existent file.");
          }
        }
      }
      else {
        Console.WriteLine($"Error: no arguments provided.");
      }

      License.iConfirmNonCommercialUse("anonymous");

      // parse configuration toml file
      GeneratorConfig generatorConfig = null;
      Dictionary<string, SeriesConfig> seriesConfigs = new Dictionary<string, SeriesConfig>();
      foreach (var file in configFiles) {
        Console.Write($"Parsing configuration file '{file}': ");
        if (generatorConfig == null) {
          generatorConfig = ConfigurationParser.GetGeneratorConfig(file);
          if (generatorConfig != null) Console.Write("generator configuration, ");
        }
        var configs = ConfigurationParser.GetSeriesConfigurations(file);
        Console.Write($"{configs.Count} series configurations");
        foreach (var config in configs) {
          if (!seriesConfigs.ContainsKey(config.Key)) {
            seriesConfigs.Add(config.Key, config.Value);
          }
        }
        Console.WriteLine();
      }

      if (generatorConfig == null) {
        Console.WriteLine("Error: no generator configuration has been parsed.");
      }
      else if (seriesConfigs.Count == 0) {
        Console.WriteLine("Error: no series configurations have been parsed.");
      }

      string gType = generatorConfig.Type.Trim();

      // setup series value dictionary
      var seriesDict = new Dictionary<string, DoubleSeries>();
      foreach (var sc in seriesConfigs) {
        if (sc.Value.GetType() == typeof(ARSeriesConfig)
          || sc.Value.GetType() == typeof(ARMASeriesConfig)
          || sc.Value.GetType() == typeof(ARIMASeriesConfig)
          || sc.Value.GetType() == typeof(MESeriesConfig)
          || sc.Value.GetType() == typeof(MECSeriesConfig)
          || sc.Value.GetType() == typeof(MEMCSeriesConfig)
          || sc.Value.GetType() == typeof(XFSeriesConfig)
          || (sc.Value.GetType() == typeof(XGSeriesConfig) // only supported if streaming
              && (gType == TYPE_STREAM_DATETIMEBASED_SINGLETHREADED
                  || gType == TYPE_STREAM_DATETIMEBASED_MULTITASKED))) {
          seriesDict.Add(sc.Key, new DoubleSeries(sc.Value.InitialValue));
        }
      }

      // stream or generate
      switch (gType) {
        case TYPE_STREAM_DATETIMEBASED_SINGLETHREADED:
          StreamSingleThreaded(generatorConfig, seriesDict, seriesConfigs);
          break;
        case TYPE_STREAM_DATETIMEBASED_MULTITASKED:
          StreamMultiTasked(generatorConfig, seriesDict, seriesConfigs);
          break;
        case TYPE_GENERATE_EVENTCOUNTBASED:
          GenerateEventCountBased(generatorConfig, seriesDict, seriesConfigs);
          break;
        case TYPE_GENERATE_DATETIMEBASED:
          GenerateDateTimeBased(generatorConfig, seriesDict, seriesConfigs);
          break;
      }
    }

    #region setup and init
    static IEnumerable<DoubleSeriesGenerator> SetupGenerators(GeneratorConfig generatorConfig, Dictionary<string, DoubleSeries> seriesDict, Dictionary<string, SeriesConfig> configDict) {
      Console.WriteLine("Setup generators");
      var generators = new List<DoubleSeriesGenerator>();
      foreach (var item in seriesDict) {
        var generator = new DoubleSeriesGenerator(generatorConfig, seriesDict, item.Value, configDict[item.Key], configDict);
        generators.Add(generator);
      }
      return generators.OrderBy(x => x.config.Rank).ToList();
    }

    // generate1, stream1 & stream2
    static void InitializeGeneratorsDateTimeBased(IEnumerable<DoubleSeriesGenerator> generators, GeneratorConfig generatorConfig, Dictionary<string, DoubleSeries> seriesDict, Dictionary<string, SeriesConfig> configDict) {
      DateTime internalTime = generatorConfig.StartDateTime;
      int interval = generatorConfig.Interval;

      // initialize series
      Console.WriteLine("Initialize series generation");
      int maxDelay = configDict.Values.Max(x => x.Delay);
      DateTime delayTime = generatorConfig.StartDateTime.AddMilliseconds(maxDelay * interval);
      while (internalTime <= delayTime) {
        foreach (var generator in generators) {
          if (generator.Time <= internalTime) {
            generator.GenerateSeriesNextValue();
          }
        }
        internalTime = internalTime.AddMilliseconds(interval);
      }

      // reset
      foreach (var generator in generators) {
        generator.SetTimeAndCount(generatorConfig.StartDateTime, 0);
      }
    }

    // generate2
    static void InitliazeGeneratorsEventCountBased(IEnumerable<DoubleSeriesGenerator> generators, GeneratorConfig generatorConfig, Dictionary<string, DoubleSeries> seriesDict, Dictionary<string, SeriesConfig> configDict) {
      // initialize series
      Console.WriteLine("Initialize series generation");
      int maxDelay = configDict.Values.Max(x => x.Delay);
      for (int i = 0; i < maxDelay; i++) {
        foreach (var generator in generators) {
          generator.GenerateSeriesNextValue();
        }
      }
    }
    #endregion setup and init

    #region stream
    // stream1
    static void StreamSingleThreaded(GeneratorConfig generatorConfig, Dictionary<string, DoubleSeries> seriesDict, Dictionary<string, SeriesConfig> configDict) {
      var tokenSource = new CancellationTokenSource();
      var token = tokenSource.Token;

      // setup and initialize generators
      var generators = SetupGenerators(generatorConfig, seriesDict, configDict);
      InitializeGeneratorsDateTimeBased(generators, generatorConfig, seriesDict, configDict);

      // setup and start streaming task
      Console.WriteLine("Setup and start streaming thread");
      Thread t = new Thread(() => StreamingThread(generatorConfig, generators, token));
      t.Start();
      Console.WriteLine("Streaming task started");

      // handle control task
      string input = null;
      bool completed = false;
      var sw = new Stopwatch();
      sw.Start();

      try {
        do {
          input = Reader.ReadLine(200);
          if (sw.ElapsedMilliseconds > generatorConfig.Duration) {
            completed = true;
          }
        } while (input == null && !completed);

        if (t.ThreadState != System.Threading.ThreadState.Stopped) {
          tokenSource.Cancel();
          if (!t.Join(5000)) t.Abort();
        }

        if (completed) Console.WriteLine("\nThe generator's runtime is over. The generator rests now.");
        else Console.WriteLine("\nThe generator has been cancelled. The generator rests now.");
      }
      catch (AggregateException) {
        Console.WriteLine("\nThe generator takes a nap.");
      }
      finally {
        tokenSource.Dispose();
      }
    }

    private static void StreamingThread(GeneratorConfig generatorConfig, IEnumerable<DoubleSeriesGenerator> generators, CancellationToken token) {
      DateTime internalTime = generatorConfig.StartDateTime;
      DateTime endTime = generatorConfig.StartDateTime.AddMilliseconds(generatorConfig.Duration);
      int interval = generatorConfig.Interval;
      
      var client = new MqttFactory().CreateManagedMqttClient();
      try {
        var options = new MqttClientOptionsBuilder()
          .WithClientId(Guid.NewGuid().ToString())
          .WithWebSocketServer("ws://127.0.0.1:5000/mqtt")
          .WithTcpServer(generatorConfig.BrokerHostName, generatorConfig.BrokerHostPort)
          .WithCleanSession(true);
        var mgOptions = new ManagedMqttClientOptionsBuilder()
          .WithAutoReconnectDelay(TimeSpan.FromSeconds(10))
          .WithClientOptions(options.Build())
          .Build();

        client.StartAsync(mgOptions).Wait(token);
        Thread.Sleep(500);

        while (internalTime < endTime && !token.IsCancellationRequested) {
          // generate and publish next messages
          if (generatorConfig.Shuffle) generators = generators.Shuffle(generatorConfig.Rnd);
          foreach (var generator in generators) {
            if (generator.Time <= internalTime) {
              string msg = generator.GenerateSeriesNextStreamingMessage();
              if (generator.config.Export) {
                var appMessage = new MqttApplicationMessageBuilder()
                  .WithTopic(generator.config.Topic)
                  .WithPayload(Encoding.UTF8.GetBytes(msg))
                  .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce)
                  .Build();
                var mappMessage = new ManagedMqttApplicationMessageBuilder()
                  .WithApplicationMessage(appMessage)
                  .Build();
                client.EnqueueAsync(mappMessage).Wait(token);                

              }
            }
          }
          internalTime = internalTime.AddMilliseconds(interval);
          Thread.Sleep(interval);
        }

        foreach (var generator in generators) {
          generator.TearDown();
        }

      }
      catch (Exception ex) {
        Console.WriteLine("Error: " + ex.Message);
      }
      finally {
        foreach (var generator in generators) {
          generator.TearDown();
        }
        client.StopAsync().Wait(token);        
        client.Dispose();
        client = null;
      }
    }

    // stream2
    static void StreamMultiTasked(GeneratorConfig generatorConfig, Dictionary<string, DoubleSeries> seriesDict, Dictionary<string, SeriesConfig> configDict) {
      var tasks = new List<Task>();
      var tokenSource = new CancellationTokenSource();
      var token = tokenSource.Token;

      // setup and initialize generators
      var generators = SetupGenerators(generatorConfig, seriesDict, configDict);
      InitializeGeneratorsDateTimeBased(generators, generatorConfig, seriesDict, configDict);

      Console.WriteLine("Convert series configurations to jobs");
      foreach (var generator in generators) {
        var task = Task.Factory.StartNew(() => {
          var client = new MqttFactory().CreateManagedMqttClient();
          try {
            var options = new MqttClientOptionsBuilder()
              .WithClientId(Guid.NewGuid().ToString())
              .WithWebSocketServer("ws://127.0.0.1:5000/mqtt")
              .WithTcpServer(generatorConfig.BrokerHostName, generatorConfig.BrokerHostPort)
              .WithCleanSession(true);
            var mgOptions = new ManagedMqttClientOptionsBuilder()
              .WithAutoReconnectDelay(TimeSpan.FromSeconds(10))
              .WithClientOptions(options.Build())
              .Build();

            client.StartAsync(mgOptions).Wait(token);
            Thread.Sleep(500);


            var job = new StreamingJob() { Client = client, Token = token };
            generator.StreamSeries(job);
          }
          finally {
            client.StopAsync().Wait(token);
            client.Dispose();
            client = null;
          }
        }, token);
        tasks.Add(task);
      }
      Console.WriteLine("All jobs started");

      string input = null;
      bool completed = false;
      var sw = new Stopwatch();
      sw.Start();

      try {
        do {
          input = Reader.ReadLine(500);
          completed = Task.WaitAll(tasks.ToArray(), 10);
          if (sw.ElapsedMilliseconds > generatorConfig.Duration) {
            tokenSource.Cancel();
            Task.WaitAll(tasks.ToArray());
            completed = true;
          }
        } while (input == null && !completed);
        if (completed) {
          Console.WriteLine("\nGenerator's runtime is over. The generator calls it a night.");
        }
        else {
          tokenSource.Cancel();
          Task.WaitAll(tasks.ToArray());
        }
      }
      catch (AggregateException) {
        Console.WriteLine("\nThe generator tasks has been cancelled. The generator rests.");
      }
      finally {
        tokenSource.Dispose();
      }
    }
    #endregion stream

    #region generate
    // generate1
    static void GenerateDateTimeBased(GeneratorConfig generatorConfig, Dictionary<string, DoubleSeries> seriesDict, Dictionary<string, SeriesConfig> configDict) {
      Stopwatch watch = new Stopwatch();
      watch.Start();

      // setup and initialize generators
      var generators = SetupGenerators(generatorConfig, seriesDict, configDict);
      InitializeGeneratorsDateTimeBased(generators, generatorConfig, seriesDict, configDict);

      DateTime internalTime = generatorConfig.StartDateTime;
      DateTime endTime = generatorConfig.StartDateTime.AddMilliseconds(generatorConfig.Duration);
      int interval = generatorConfig.Interval;
      string separator = generatorConfig.Separator;
      string format = "{0:N" + generatorConfig.DecimalPrecision + "}";

      // write to stream
      Console.WriteLine("Generate and write to file stream");
      using (StreamWriter sw = new StreamWriter(new FileStream(generatorConfig.OutputFilePath, FileMode.Create))) {
        var exports = generators.Where(x => x.config.Export).OrderBy(x => x.config.Rank).ToArray();
        if (generatorConfig.ExportIdAsHeader) {
          if (generatorConfig.ExportDateTime) {
            sw.Write($"DateTime");
            if (exports.Length > 0 || generatorConfig.ExportEventCount) sw.Write(separator);
          }
          if (generatorConfig.ExportEventCount) {
            sw.Write($"EventCount");
            if (exports.Length > 0) sw.Write(separator);
          }
          for (int c = 0; c < exports.Length; c++) {
            if (c > 0) sw.Write(separator);
            sw.Write($"{exports[c].config.Id}");

            // export headers for lags
            if (generatorConfig.ExportLags != null && generatorConfig.ExportLags.Any()) {
              for (int i = 0; i < generatorConfig.ExportLags.Length; i++) {
                sw.Write(separator);
                sw.Write($"{exports[c].config.Id}-{generatorConfig.ExportLags[i]}");
              }
            }
          }
          sw.WriteLine();
        }

        string exportStr = "";
        long counter = 0;
        var exportStrParts = new Dictionary<string, string>();
        while (internalTime < endTime) {
          exportStr = "";
          exportStrParts.Clear();

          if (generatorConfig.Shuffle) generators = generators.Shuffle(generatorConfig.Rnd).ToList();
          foreach (var generator in generators) {
            double? value = null;
            if (generator.Time <= internalTime) {
              value = generator.GenerateSeriesNextValue();
            }
            if (generator.config.Export && value.HasValue) {
              if (generatorConfig.ExportDiff) {
                //var lastValue = generator.GetValuesFromSeries(1);
                //double? diff = ComputeDiff(value, lastValue);
                double? diff = ComputeDiff(generator, generatorConfig.ExportDiffFPS, 0);
                exportStrParts.Add(generator.config.Id, string.Format(format, diff?.ToString()));
              }
              else if (generatorConfig.ExportMovingAvg) {
                double mavg = ComputeMovingAverage(generator, 0, generatorConfig.ExportMovingAvgWindowSize);
                exportStrParts.Add(generator.config.Id, string.Format(format, mavg));
              }
              else {
                exportStrParts.Add(generator.config.Id, string.Format(format, value?.ToString()));
              }

              if (generatorConfig.ExportLags != null && generatorConfig.ExportLags.Any()) {
                for (int i = 0; i < generatorConfig.ExportLags.Length; i++) {
                  int offset = generatorConfig.ExportLagOffset;
                  if (generatorConfig.ExportDiff) {
                    //var lastVal = generator.GetValuesFromSeries(generatorConfig.ExportLags[i] + 1);
                    //double? diff = ComputeDiff(val, lastVal);
                    double? diff = ComputeDiff(generator, generatorConfig.ExportDiffFPS, generatorConfig.ExportLags[i] + offset);
                    exportStrParts[generator.config.Id] += separator + string.Format(format, diff?.ToString());
                  }
                  else if (generatorConfig.ExportMovingAvg) {
                    double mavg = ComputeMovingAverage(generator, generatorConfig.ExportLags[i] + offset, generatorConfig.ExportMovingAvgWindowSize);
                    exportStrParts[generator.config.Id] += separator + string.Format(format, mavg);
                  }
                  else {
                    var val = generator.GetValuesFromSeries(generatorConfig.ExportLags[i] + offset);
                    exportStrParts[generator.config.Id] += separator + string.Format(format, val?.ToString());
                  }
                }
              }
            }
          }

          if (exportStrParts.Any()) {
            // build export string according to export relevance and rank
            for (int i = 0; i < exports.Length; i++) {
              if (exportStrParts.ContainsKey(exports[i].config.Id)) {
                exportStr += exportStrParts[exports[i].config.Id];
              }
              if (i < (exports.Length - 1)) exportStr += separator;
            }

            counter++;
            string prefix = "";
            if (generatorConfig.ExportDateTime) prefix += internalTime.ToString(generatorConfig.DateTimeFormat) + separator;
            if (generatorConfig.ExportEventCount) prefix += counter + separator;

            sw.Write($"{prefix}{exportStr}");
            sw.WriteLine();
          }

          internalTime = internalTime.AddMilliseconds(interval);
        }
      }

      watch.Stop();
      Console.WriteLine($"\nTime elapsed: {watch.ElapsedMilliseconds / 1000.0} sec.");
    }

    // generate2
    static void GenerateEventCountBased(GeneratorConfig generatorConfig, Dictionary<string, DoubleSeries> seriesDict, Dictionary<string, SeriesConfig> configDict) {
      Stopwatch watch = new Stopwatch();
      watch.Start();

      var generators = SetupGenerators(generatorConfig, seriesDict, configDict);
      InitliazeGeneratorsEventCountBased(generators, generatorConfig, seriesDict, configDict);

      string separator = generatorConfig.Separator;
      string format = "{0:N" + generatorConfig.DecimalPrecision + "}";

      // write to stream
      Console.WriteLine("Generate and write to file stream");
      using (StreamWriter sw = new StreamWriter(new FileStream(generatorConfig.OutputFilePath, FileMode.Create))) {
        var exports = generators.Where(x => x.config.Export).OrderBy(x => x.config.Rank).ToArray();
        if (generatorConfig.ExportIdAsHeader) {
          if (generatorConfig.ExportDateTime) {
            sw.Write($"DateTime");
            if (exports.Length > 0 || generatorConfig.ExportEventCount) sw.Write(separator);
          }
          if (generatorConfig.ExportEventCount) {
            sw.Write($"EventCount");
            if (exports.Length > 0) sw.Write(separator);
          }
          for (int c = 0; c < exports.Length; c++) {
            if (c > 0) sw.Write(separator);
            sw.Write($"{exports[c].config.Id}");

            // export headers for lags
            if (generatorConfig.ExportLags != null && generatorConfig.ExportLags.Any()) {
              for (int i = 0; i < generatorConfig.ExportLags.Length; i++) {
                sw.Write(separator);
                sw.Write($"{exports[c].config.Id}-{generatorConfig.ExportLags[i]}");
              }
            }
          }
          sw.WriteLine();
        }

        string exportStr = "";
        long counter = 0;
        var exportStrParts = new Dictionary<string, string>();
        for (int i = 0; i < generatorConfig.Duration; i++) {
          exportStr = "";
          exportStrParts.Clear();

          if (generatorConfig.Shuffle) generators = generators.Shuffle(generatorConfig.Rnd);
          foreach (var generator in generators) {
            double value = generator.GenerateSeriesNextValue();
            if (generator.config.Export) {
              exportStrParts.Add(generator.config.Id, string.Format(format, value));

              if (generatorConfig.ExportLags != null && generatorConfig.ExportLags.Any()) {
                for (int j = 0; j < generatorConfig.ExportLags.Length; j++) {
                  var val = generator.GetValuesFromSeries(generatorConfig.ExportLags[j]);
                  exportStrParts[generator.config.Id] += separator + val?.ToString();
                }
              }
            }
          }

          if (exportStrParts.Any()) {
            // build export string according to export relevance and rank
            for (int j = 0; j < exports.Length; j++) {
              if (exportStrParts.ContainsKey(exports[j].config.Id)) {
                exportStr += exportStrParts[exports[j].config.Id];
              }
              if (j < (exports.Length - 1)) exportStr += separator;
            }

            counter++;
            string prefix = "";
            if (generatorConfig.ExportEventCount) prefix += counter + separator;

            sw.Write($"{prefix}{exportStr}");
            sw.WriteLine();
          }
        }
      }

      watch.Stop();
      Console.WriteLine($"\nTime elapsed: {watch.ElapsedMilliseconds / 1000.0} sec.");
    }
    #endregion generate

    #region misc
    static double? ComputeDiff(double? value, double? lastValue) {
      if (value.HasValue && lastValue.HasValue) {
        return value.Value - lastValue.Value;
      }
      return null;
    }

    static double? ComputeDiff(DoubleSeriesGenerator generator, bool fps, int valueIdx) {
      double? value = generator.GetValuesFromSeries(valueIdx);
      if (!value.HasValue) return null;
      if (!fps) {
        double? lastValue = generator.GetValuesFromSeries(valueIdx + 1);
        if (lastValue.HasValue) return value.Value - lastValue.Value;
      }
      else {
        // five point stencil:
        // 2h = 0, h = -1, -h = -3, -2h = -4
        // f'(x) ~ (-f(x+2h) + 8*f(x+h) - 8*f(x-h) + f(x-2h)) / 12
        double hh = value.Value;
        double? h = generator.GetValuesFromSeries(valueIdx + 1);
        double? _h = generator.GetValuesFromSeries(valueIdx + 3);
        double? _hh = generator.GetValuesFromSeries(valueIdx + 4);
        if (!h.HasValue || !_h.HasValue || !_hh.HasValue) throw new InvalidOperationException("Cannot compute five point stencil derivative. Please increase the series' delay settings.");

        return (-hh + 8 * h.Value - 8 * _h + _hh) / 12.0;

      }
      return null;
    }

    static double ComputeMovingAverage(DoubleSeriesGenerator generator, int startIdx, int windowSize) {
      double sum = 0.0;
      for (int i = startIdx; i < (startIdx + windowSize); i++) {
        double? val = generator.GetValuesFromSeries(i);
        if (val.HasValue) sum += val.Value;
        else throw new InvalidOperationException("Cannot compute moving average. Please increase the series' delay settings.");
      }
      return sum / windowSize;
    }
    #endregion misc
  }
}