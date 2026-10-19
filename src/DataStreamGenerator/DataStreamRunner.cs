using DSG.DataStreamGenerator.Configuration;
using DSG.DataStreamGenerator.Utils;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using System.Diagnostics;
using System.Text;

namespace DSG.DataStreamGenerator {

  public class DataStreamRunner {
    public const string TYPE_STREAM_DATETIMEBASED_SINGLETHREADED = "stream1";
    public const string TYPE_STREAM_DATETIMEBASED_MULTITASKED = "stream2";
    public const string TYPE_GENERATE_DATETIMEBASED = "generate1";
    public const string TYPE_GENERATE_EVENTCOUNTBASED = "generate2";

    private readonly GeneratorConfig generatorConfig;
    private readonly IReadOnlyDictionary<string, SeriesConfig> seriesConfigDict;

    public DataStreamRunner(GeneratorConfig generatorConfig, IReadOnlyDictionary<string, SeriesConfig> seriesConfigs) {
      this.generatorConfig = generatorConfig ?? throw new ArgumentNullException(nameof(generatorConfig));
      this.seriesConfigDict = seriesConfigs ?? throw new ArgumentNullException(nameof(seriesConfigs));
      if (string.IsNullOrWhiteSpace(generatorConfig.Type)) throw new ArgumentException("Generator configuration must provide a type.", nameof(generatorConfig));
      if (seriesConfigs.Count == 0) throw new ArgumentException("At least one series configuration is required.", nameof(seriesConfigs));
    }

    public void Run(CancellationToken userCancellationToken = default) {
      string gType = generatorConfig.Type.Trim();

      // setup series value dictionary
      var seriesDict = new Dictionary<string, DoubleSeries>();
      foreach (var sc in seriesConfigDict) {
        if (sc.Value.GetType() == typeof(ARSeriesConfig)
          || sc.Value.GetType() == typeof(ARMASeriesConfig)
          || sc.Value.GetType() == typeof(ARIMASeriesConfig)
          || sc.Value.GetType() == typeof(MESeriesConfig)
          || sc.Value.GetType() == typeof(MECSeriesConfig)
          || sc.Value.GetType() == typeof(MEMCSeriesConfig)
          || sc.Value.GetType() == typeof(XFSeriesConfig)
          || sc.Value.GetType() == typeof(IntSeqSeriesConfig)
          || (sc.Value.GetType() == typeof(XGSeriesConfig) // only supported if streaming
              && (gType == TYPE_STREAM_DATETIMEBASED_SINGLETHREADED
                  || gType == TYPE_STREAM_DATETIMEBASED_MULTITASKED))) {
          seriesDict.Add(sc.Key, new DoubleSeries(sc.Value.InitialValue));
        }
      }

      // stream or generate
      switch (gType) {
        case TYPE_STREAM_DATETIMEBASED_SINGLETHREADED:
          StreamSingleThreaded(seriesDict, userCancellationToken);
          break;
        case TYPE_STREAM_DATETIMEBASED_MULTITASKED:
          StreamMultiTasked(seriesDict, userCancellationToken);
          break;
        case TYPE_GENERATE_EVENTCOUNTBASED:
          GenerateEventCountBased(seriesDict);
          break;
        case TYPE_GENERATE_DATETIMEBASED:
          GenerateDateTimeBased(seriesDict);
          break;
      }
    }

    #region setup and init
    private IEnumerable<DoubleSeriesGenerator> SetupGenerators(Dictionary<string, DoubleSeries> seriesDict) {
      Console.WriteLine("Setup generators");
      var generators = new List<DoubleSeriesGenerator>();
      foreach (var item in seriesDict) {
        var generator = new DoubleSeriesGenerator(generatorConfig, seriesDict, item.Value, seriesConfigDict[item.Key], new Dictionary<string, SeriesConfig>(seriesConfigDict));
        generators.Add(generator);
      }
      return generators.OrderBy(x => x.config.Rank).ToList();
    }

    // generate1, stream1 & stream2
    private void InitializeGeneratorsDateTimeBased(IEnumerable<DoubleSeriesGenerator> generators, Dictionary<string, DoubleSeries> seriesDict) {
      DateTime internalTime = generatorConfig.StartDateTime;
      int interval = generatorConfig.Interval;

      // initialize series
      Console.WriteLine("Initialize series generation");
      int maxDelay = seriesConfigDict.Values.Max(x => x.Delay);
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
    private void InitializeGeneratorsEventCountBased(IEnumerable<DoubleSeriesGenerator> generators, Dictionary<string, DoubleSeries> seriesDict) {
      // initialize series
      Console.WriteLine("Initialize series generation");
      int maxDelay = seriesConfigDict.Values.Max(x => x.Delay);
      for (int i = 0; i < maxDelay; i++) {
        foreach (var generator in generators) {
          generator.GenerateSeriesNextValue();
        }
      }
    }
    #endregion setup and init

    #region stream
    // stream1
    private void StreamSingleThreaded(Dictionary<string, DoubleSeries> seriesDict, CancellationToken userCancellationToken) {
      var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(userCancellationToken);
      var token = tokenSource.Token;

      // setup and initialize generators
      var generators = SetupGenerators(seriesDict);
      InitializeGeneratorsDateTimeBased(generators, seriesDict);

      Console.WriteLine("Setup and start streaming thread");
      Thread t = new Thread(() => StreamingThread(generators, token));
      t.Start();
      Console.WriteLine("Streaming task started");

      // handle control task
      bool completed = false;
      var sw = new Stopwatch();
      sw.Start();

      try {
        while (!token.IsCancellationRequested && sw.ElapsedMilliseconds <= generatorConfig.Duration) {
          Thread.Sleep(200);
        }
        completed = !token.IsCancellationRequested;

        if (t.ThreadState != System.Threading.ThreadState.Stopped) {
          tokenSource.Cancel();
          if (!t.Join(5000)) t.Join(5000);
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

    private void StreamingThread(IEnumerable<DoubleSeriesGenerator> generators, CancellationToken token) {
      DateTime internalTime = generatorConfig.StartDateTime;
      DateTime endTime = generatorConfig.StartDateTime.AddMilliseconds(generatorConfig.Duration);
      int interval = generatorConfig.Interval;

      var client = new MqttFactory().CreateManagedMqttClient();

      try {
        var options = new MqttClientOptionsBuilder()
          .WithClientId(Guid.NewGuid().ToString())
          .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
          //.WithWebSocketServer("ws://127.0.0.1:5000/mqtt")
          .WithTcpServer(generatorConfig.BrokerHostName, generatorConfig.BrokerHostPort)
          .WithCleanSession(true)
          ;
        var mgOptions = new ManagedMqttClientOptionsBuilder()
          .WithAutoReconnectDelay(TimeSpan.FromSeconds(10))
          .WithClientOptions(options.Build())
          .Build();

        client.StartAsync(mgOptions).Wait(token);
        Thread.Sleep(200);

        while (internalTime < endTime && !token.IsCancellationRequested) {
          // generate and publish next messages
          if (generatorConfig.Shuffle) generators = generators.Shuffle(generatorConfig.Rnd!);
          foreach (var generator in generators) {
            if (generator.Time <= internalTime) {
              string msg = generator.GenerateSeriesNextStreamingMessageString();
              //var msg = generator.GenerateSeriesNextStreamingMessage();
              if (generator.config.Export) {

                var appMessage = new MqttApplicationMessageBuilder()
                  .WithTopic(generator.config.Topic)
                  .WithResponseTopic(generator.config.Topic)
                  .WithPayload(Encoding.UTF8.GetBytes(msg))
                  //.WithPayload(msg)
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
        try
        {
          client.StopAsync().Wait(TimeSpan.FromSeconds(5));
        }
        catch (OperationCanceledException)
        {
        }
        client.Dispose();
        client = null;
      }
    }

    // stream2
    private void StreamMultiTasked(Dictionary<string, DoubleSeries> seriesDict, CancellationToken userCancellationToken) {
      var tasks = new List<Task>();
      var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(userCancellationToken);
      var token = tokenSource.Token;

      // setup and initialize generators
      var generators = SetupGenerators(seriesDict);
      InitializeGeneratorsDateTimeBased(generators, seriesDict);

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
            try
            {
              client.StopAsync().Wait(TimeSpan.FromSeconds(5));
            }
            catch (OperationCanceledException)
            {
            }
            client.Dispose();
            client = null;
          }
        }, token);
        tasks.Add(task);
      }
      Console.WriteLine("All jobs started");

      bool completed = false;
      var sw = new Stopwatch();
      sw.Start();

      try {
        while (true) {
          if (token.IsCancellationRequested) break;
          completed = Task.WaitAll(tasks.ToArray(), 10);
          if (completed) break;
          if (sw.ElapsedMilliseconds > generatorConfig.Duration) {
            tokenSource.Cancel();
            Task.WaitAll(tasks.ToArray());
            completed = true;
            break;
          }
          Thread.Sleep(10);
        }
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
    private void GenerateDateTimeBased(Dictionary<string, DoubleSeries> seriesDict) {
      Stopwatch watch = new Stopwatch();
      watch.Start();

      // setup and initialize generators
      var generators = SetupGenerators(seriesDict);
      InitializeGeneratorsDateTimeBased(generators, seriesDict);

      DateTime internalTime = generatorConfig.StartDateTime;
      DateTime endTime = generatorConfig.StartDateTime.AddMilliseconds(generatorConfig.Duration);
      int interval = generatorConfig.Interval;
      string? separator = generatorConfig.Separator;
      string format = "{0:N" + generatorConfig.DecimalPrecision + "}";

      // write to stream
      Console.WriteLine("Generate and write to file stream");
      using (StreamWriter sw = new StreamWriter(new FileStream(generatorConfig.OutputFilePath!, FileMode.Create))) {
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

          if (generatorConfig.Shuffle) generators = generators.Shuffle(generatorConfig.Rnd!).ToList();
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
    private void GenerateEventCountBased(Dictionary<string, DoubleSeries> seriesDict) {
      Stopwatch watch = new Stopwatch();
      watch.Start();

      var generators = SetupGenerators(seriesDict);
      InitializeGeneratorsEventCountBased(generators, seriesDict);

      string? separator = generatorConfig.Separator;
      string format = "{0:N" + generatorConfig.DecimalPrecision + "}";

      // write to stream
      Console.WriteLine("Generate and write to file stream");
      using (StreamWriter sw = new StreamWriter(new FileStream(generatorConfig.OutputFilePath!, FileMode.Create))) {
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

          if (generatorConfig.Shuffle) generators = generators.Shuffle(generatorConfig.Rnd!);
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
    private static double? ComputeDiff(double? value, double? lastValue) {
      if (value.HasValue && lastValue.HasValue) {
        return value.Value - lastValue.Value;
      }
      return null;
    }

    private static double? ComputeDiff(DoubleSeriesGenerator generator, bool fps, int valueIdx) {
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

    private static double ComputeMovingAverage(DoubleSeriesGenerator generator, int startIdx, int windowSize) {
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
