using DSG.Configuration;
using DSG.Utils;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using org.mariuszgromada.math.mxparser;
using System.Text;
using System.Text.Json;

namespace DSG {
  public class DoubleSeriesGenerator : Generator {

    public const double MAX_BUFFER_SIZE = 500;
    public const string ID_TIMEELAPSED = "t";
    public const string ID_EVENTCOUNT = "c";
    public const string ID_EVENTITERATOR = "i";
    public const char TIMELAG_SEPARATOR = '_';
    public const char CSV_SEPARATOR = ';';

    private DoubleSeries series;
    public SeriesConfig config;
    private Dictionary<string, DoubleSeries> seriesDict;

    private GeneratorConfig generatorConfig;
    private Random rnd;
    private IManagedMqttClient client;

    private DateTime time;
    public DateTime Time {
      get { return time; }
    }

    private long count;
    public long Count {
      get { return count; }
    }

    private long iter;
    public long Iter {
      get { return iter; }
    }

    private bool driversActive;
    private bool initialized;

    public DoubleSeriesGenerator(GeneratorConfig generatorConfig, Dictionary<string, DoubleSeries> seriesDict, DoubleSeries series, SeriesConfig config) {
      initialized = false;

      this.generatorConfig = generatorConfig;
      this.rnd = generatorConfig.Rnd;
      this.seriesDict = seriesDict;
      this.series = series;
      this.config = config;
      this.count = 0;
      this.iter = 0;

      WarmUp();
    }

    public void WarmUp() {
      dynamic c = config;
      driversActive = false;
      for (int i = 0; i < config.Delay; i++) {
        GenerateSeriesNextValue();
      }
      SetTimeAndCount(generatorConfig.StartDateTime, 0);
      driversActive = true;
    }

    public void TearDown() {
      if (client != null) {
        client.StopAsync().Wait();
        client.Dispose();
        client = null;
      }
    }

    public void SetTimeAndCount(DateTime startTime, long count) {
      this.time = startTime;
      this.count = count;
    }

    public void StreamSeries(StreamingJob job) {
      dynamic c = config;

      DateTime endDateTime = generatorConfig.StartDateTime.AddMilliseconds(generatorConfig.Duration);
      while (time < endDateTime) {
        job.Token.ThrowIfCancellationRequested();

        double xt = 0.0;
        try {
          lock (seriesDict)
            xt = ComputeNextX(rnd, series, c);
        }
        catch (Exception ex) {
          Console.WriteLine(ex.Message + "\n" + ex.Source);
        }

        double outlierCandidate = rnd.NextDouble();
        if (outlierCandidate < c.OutlierRatio2s) {
          xt = GenerateOutlier(rnd, xt, series.X, 2);
        }
        else if (outlierCandidate < c.OutlierRatio1s) {
          xt = GenerateOutlier(rnd, xt, series.X, 1);
        }

        string timestamp = time.ToString(generatorConfig.DateTimeFormat);
        string msg = JsonSerializer.Serialize(GenerateMessage(c, generatorConfig.Group, timestamp, xt));

        var message = new MqttApplicationMessageBuilder()
          .WithTopic(config.Topic)
          .WithPayload(Encoding.UTF8.GetBytes(msg))
          .Build();
        var task = job.Client.EnqueueAsync(message);
        //task.Wait(); 

        Thread.Sleep(config.Interval);

        // increase count and time
        iter++;
        count++;
        time = time.AddMilliseconds(config.Interval);
      }
      TearDown();
    }

    public string GenerateSeriesNextStreamingMessage() {
      double value = GenerateSeriesNextValue();

      string timestamp = time.ToString(generatorConfig.DateTimeFormat);

      return JsonSerializer.Serialize(GenerateMessage(config, generatorConfig.Group, timestamp, value));
    }

    private Message GenerateMessage(SeriesConfig c, string _group, string _timestamp, double _value) {
      return new Message()
      {
        id = c.Id,
        group = _group,
        rank = c.Rank,
        title = c.Title,
        timestamp = _timestamp,
        value = _value
      };
    }

    public double GenerateSeriesNextValue() {
      dynamic c = config;

      double xt = 0.0;
      try {
        // calculate next series value
        xt = ComputeNextX(rnd, series, c);

        // calculate possible outlier
        double outlierCandidate = rnd.NextDouble();
        if (outlierCandidate < c.OutlierRatio2s) {
          xt = GenerateOutlier(rnd, xt, series.X, 2);
        }
        else if (outlierCandidate < c.OutlierRatio1s) {
          xt = GenerateOutlier(rnd, xt, series.X, 1);
        }

        // increase iterator, count and time
        iter++;
        count++;
        time = time.AddMilliseconds(config.Interval);

      }
      catch (Exception ex) {
        Console.WriteLine(ex.Message + "\n" + ex.Source);
      }

      return xt;
    }

    #region next value computation
    private double ComputeNextX(Random rnd, DoubleSeries s, ARSeriesConfig c) {
      var ar_part = 0.0;

      // AR
      for (int j = 0, x_Count = s.X.Count; j < c.P.Length && j < s.X.Count; j++) {
        ar_part += c.P[j] * s.X[x_Count - 1 - j];
      }

      // Drivers
      var ar_part_drivers = 0.0;
      var ma_part_drivers = 0.0;
      lock (seriesDict)
        ComputeDriverParts(c.Drivers, out ar_part_drivers, out ma_part_drivers);

      var et = rnd.NextGaussian_BoxMuller(c.Mean, c.StdDev); // next noise/shock
      var xt = c.C + et + ar_part + ar_part_drivers + ma_part_drivers; // next sensor value

      lock (seriesDict) {
        if (s.X.Count >= MAX_BUFFER_SIZE) s.X.RemoveAt(0);
        if (s.E.Count >= MAX_BUFFER_SIZE) s.E.RemoveAt(0);
        s.E.Add(et);
        s.X.Add(xt);
      }

      return xt;

    }

    private double ComputeNextX(Random rnd, DoubleSeries s, ARMASeriesConfig c) {
      var ar_part = 0.0;
      var ma_part = 0.0;

      // AR
      for (int j = 0, x_Count = s.X.Count; j < c.P.Length && j < s.X.Count; j++) {
        ar_part += c.P[j] * s.X[x_Count - 1 - j];
      }

      // MA
      for (int j = 0, e_Count = s.E.Count; j < c.Q.Length && j < s.E.Count; j++) {
        ma_part += c.Q[j] * s.E[s.E.Count - 1 - j];
      }


      // Drivers
      var ar_part_drivers = 0.0;
      var ma_part_drivers = 0.0;
      lock (seriesDict)
        ComputeDriverParts(c.Drivers, out ar_part_drivers, out ma_part_drivers);

      var et = rnd.NextGaussian_BoxMuller(c.Mean, c.StdDev); // next noise/shock
      var xt = c.C + et + ar_part + ma_part + ar_part_drivers + ma_part_drivers; // next sensor value

      lock (seriesDict) {
        if (s.X.Count >= MAX_BUFFER_SIZE) s.X.RemoveAt(0);
        if (s.E.Count >= MAX_BUFFER_SIZE) s.E.RemoveAt(0);
        s.E.Add(et);
        s.X.Add(xt);
      }

      return xt;
    }

    private double ComputeNextX(Random rnd, DoubleSeries s, ARIMASeriesConfig c) {
      var ar_part = 0.0;
      var ma_part = 0.0;


      // AR
      for (int j = 0, x_Count = s.X.Count; j < c.P.Length && j < s.X.Count; j++) {
        ar_part += c.P[j] * s.X[x_Count - 1 - j];
      }

      // MA
      for (int j = 0, e_Count = s.E.Count; j < c.Q.Length && j < s.E.Count; j++) {
        ma_part += c.Q[j] * s.E[s.E.Count - 1 - j];
      }

      // Calculate differential part
      var et = rnd.NextGaussian_BoxMuller(c.Mean, c.StdDev); // next noise/shock
      var dt = c.C + et + ar_part + ma_part; // next i-time differenced sensor value

      lock (seriesDict) {
        if (s.E.Count >= MAX_BUFFER_SIZE) s.E.RemoveAt(0);
        if (s.D.Count >= MAX_BUFFER_SIZE) s.D.RemoveAt(0);
        s.E.Add(et);
        s.D.Add(dt);
      }

      // I      
      var i_part = ComputeIntegral(s.D.TakeLast(c.I + 1).ToList());

      // Drivers
      var ar_part_drivers = 0.0;
      var ma_part_drivers = 0.0;
      lock (seriesDict)
        ComputeDriverParts(c.Drivers, out ar_part_drivers, out ma_part_drivers);


      var xt = i_part + ar_part_drivers + ma_part_drivers; // next sensor value

      lock (seriesDict) {
        if (s.X.Count >= MAX_BUFFER_SIZE) s.X.RemoveAt(0);
        s.X.Add(xt);
      }

      return xt;
    }

    private double ComputeNextX(Random rnd, DoubleSeries s, MESeriesConfig c) {
      var args = ComputeArguments(c.Arguments);
      var expressionPart = ComputeFunction(c.GetExpression(), args.ToArray());

      var ar_part_drivers = 0.0;
      var ma_part_drivers = 0.0;
      lock (seriesDict)
        ComputeDriverParts(c.Drivers, out ar_part_drivers, out ma_part_drivers);

      var et = rnd.NextGaussian_BoxMuller(c.Mean, c.StdDev); // next noise
      var xt = c.C + expressionPart + ar_part_drivers + ma_part_drivers + et;

      lock (seriesDict) {
        if (s.X.Count >= MAX_BUFFER_SIZE)
          s.X.RemoveAt(0);
        s.X.Add(xt);
      }

      return xt;
    }

    private double ComputeNextX(Random rnd, DoubleSeries s, MECSeriesConfig c) {
      var args = ComputeArguments(c.Arguments);
      Argument[] argsArr = args.ToArray();
      bool cond = ComputeCondition(c.GetCondition(), argsArr);
      double expressionPart = 0.0;
      if (cond) {
        expressionPart = ComputeFunction(c.GetExpression(), argsArr);
      }
      else {
        expressionPart = ComputeFunction(c.GetExpressionF(), argsArr);
      }

      var ar_part_drivers = 0.0;
      var ma_part_drivers = 0.0;
      lock (seriesDict)
        ComputeDriverParts(c.Drivers, out ar_part_drivers, out ma_part_drivers);

      var et = rnd.NextGaussian_BoxMuller(c.Mean, c.StdDev); // next noise
      var xt = c.C + expressionPart + ar_part_drivers + ma_part_drivers + et;

      lock (seriesDict) {
        if (s.X.Count >= MAX_BUFFER_SIZE)
          s.X.RemoveAt(0);
        s.X.Add(xt);
      }

      return xt;
    }

    private double ComputeNextX(Random rnd, DoubleSeries s, MEMCSeriesConfig c) {
      var args = ComputeArguments(c.Arguments);
      Argument[] argsArr = args.ToArray();
      var conditions = c.GetConditions();
      var expressions = c.GetExpressions();

      double expressionPart = 0.0;
      int expressionIdx = expressions.Count - 1;
      bool conditionApplies = false;

      for (int i = 0; i < conditions.Count && !conditionApplies && i <= expressionIdx; i++) {
        if (ComputeCondition(conditions[i], argsArr)) {
          expressionIdx = i;
          conditionApplies = true;
        }
      }

      expressionPart = ComputeFunction(expressions[expressionIdx], argsArr);

      var ar_part_drivers = 0.0;
      var ma_part_drivers = 0.0;
      lock (seriesDict)
        ComputeDriverParts(c.Drivers, out ar_part_drivers, out ma_part_drivers);

      var et = rnd.NextGaussian_BoxMuller(c.Mean, c.StdDev); // next noise
      var xt = c.C + expressionPart + ar_part_drivers + ma_part_drivers + et;

      lock (seriesDict) {
        if (s.X.Count >= MAX_BUFFER_SIZE)
          s.X.RemoveAt(0);
        s.X.Add(xt);
      }

      return xt;
    }

    private double ComputeNextX(Random rnd, DoubleSeries s, XFSeriesConfig c) {
      if (!initialized) {
        initialized = true;
        if (!File.Exists(c.SourcePath)) {
          throw new FileNotFoundException($"Could not find source file for XFSeriesConfig at '{c.SourcePath}'");
        }

        using (var reader = new StreamReader(c.SourcePath)) {
          string[] indices = c.SourceIndex.Split(',');
          if (indices.Length == 2) {
            if (!String.IsNullOrWhiteSpace(indices[0])) { // read a row
              int rIdx = int.Parse(indices[0]);

              for (int i = 0; i < rIdx && !reader.EndOfStream; i++) {
                reader.ReadLine();
              }
              if (reader.EndOfStream) {
                throw new ArgumentException($"Could not read the specified row index '{c.SourceIndex}'.");
              }

              try {
                string line = reader.ReadLine();
                lock (seriesDict) {
                  s.S.AddRange(line.Split(CSV_SEPARATOR).Select(x => double.Parse(x)));
                }
              }
              catch (Exception ex) {
                throw new ArgumentException("Could not read and parse specified line.\n" + ex.Message);
              }

            }
            else if (!String.IsNullOrWhiteSpace(indices[1])) {// read a column
              int cIdx = int.Parse(indices[1]);
              lock (seriesDict) {
                while (!reader.EndOfStream) {
                  try {
                    var line = reader.ReadLine();
                    var values = line.Split(CSV_SEPARATOR).Select(x => double.Parse(x)).ToArray();
                    if (values.Length < (cIdx + 1)) {
                      throw new ArgumentException($"Could not read the specified column index '{c.SourceIndex}'.");
                    }
                    else {
                      s.S.Add(values[cIdx]);
                    }
                  }
                  catch (Exception ex) {
                    throw new ArgumentException("Could not read and parse specified line.\n" + ex.Message);
                  }
                }
              }
            }
            else {
              throw new ArgumentException($"Could not read the specified index '{c.SourceIndex}'.");
            }
          }
        }
      }

      double xt = 0.0;
      if (s.S.Count > 0) {
        var idx = count + c.Delay;
        idx = idx - idx / s.S.Count * s.S.Count;
        xt = s.S[(int)(idx)];
      }

      lock (seriesDict) {
        if (s.X.Count >= MAX_BUFFER_SIZE) s.X.RemoveAt(0);
        s.X.Add(xt);
      }

      return xt;
    }

    private double ComputeNextX(Random rnd, DoubleSeries s, XGSeriesConfig c) {
      if (!initialized) {
        initialized = true;

        if (client == null) {
          client = new MqttFactory().CreateManagedMqttClient();
        }

        var options = new MqttClientOptionsBuilder()
          .WithTcpServer(c.SourceBroker)
          .Build();
        var managedOptions = new ManagedMqttClientOptionsBuilder()
          .WithAutoReconnectDelay(TimeSpan.FromSeconds(5))
          .WithClientOptions(options)
          .Build();
        var connecting = client.StartAsync(managedOptions);
        connecting.Wait();

        var task = client.SubscribeAsync(c.SourceTopic, MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce);
        task.Wait();

        client.ApplicationMessageReceivedAsync += Client_ApplicationMessageReceivedAsync;

        //client = new MqttClient(c.SourceBroker);
        //client.Connect(Guid.NewGuid().ToString());
        //client.Subscribe(new string[] { c.SourceTopic }, new byte[] { MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE });
        //client.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;
      }

      double xt = 0.0;
      if (series.X.Count > 0) xt = series.X[series.X.Count - 1];
      return xt;
    }

    private Task Client_ApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs arg) {
      return Task.Factory.StartNew(() =>
      {
        var msg = JsonSerializer.Deserialize<Message>(Encoding.UTF8.GetString(arg.ApplicationMessage.Payload));

        lock (seriesDict) {
          if (series.X.Count >= MAX_BUFFER_SIZE) series.X.RemoveAt(0);
          series.X.Add(msg.value);
        }
      });
    }
    #endregion #region next value computation

    #region generation helper

    public double? GetValuesFromSeries(int timelag) {
      if (timelag < 0) return null;

      int idx = series.X.Count - 1 - timelag;
      if (idx >= 0 && series.X.Count - 1 >= idx) {
        return series.X[idx];
      }
      else {
        return null;
      }
    }

    private double GetValueFromSeries(string id, int timelag) {
      if (id == ID_EVENTCOUNT) {
        return (Count - timelag >= 0) ? Count - timelag : 0;
      }
      else if (id == ID_EVENTITERATOR) {
        return (Iter - timelag >= 0) ? Iter - timelag : 0;
      }
      else if (id == ID_TIMEELAPSED) {
        var msElapsed = Time.Subtract(generatorConfig.StartDateTime).Milliseconds - timelag * config.Interval;
        return (msElapsed >= 0) ? msElapsed : 0;
      }
      else if (seriesDict.ContainsKey(id) && seriesDict[id].X.Count > 0) {
        var seq = seriesDict[id].X;
        int idx = seq.Count - 1 - timelag;
        idx = (idx >= 0) ? idx : 0;
        return (seq.Count - 1 >= idx) ? seq.ElementAt(idx) : 0;
      }
      else {
        return 0;
      }
    }

    private double GenerateOutlier(Random rnd, double xt, List<double> series, double f1) {
      double outlier = xt;
      double stdDev = ComputeStdDev(series);
      bool up = series[series.Count - 1] - series[series.Count - 2] >= 0;
      //bool up = rnd.NextDouble() >= 0.5;

      double off = f1 * stdDev + Math.Abs(rnd.NextGaussian_BoxMuller(0.0, stdDev / 2.0));
      if (up) outlier += off;
      else outlier -= off;

      return outlier;
    }

    private void ComputeDriverParts(List<DriverConfig> drivers, out double ar_part_drivers, out double ma_part_drivers) {
      ar_part_drivers = 0.0;
      ma_part_drivers = 0.0;
      if (!driversActive) return;
      if (drivers == null || drivers.Count == 0) return;

      foreach (var driverConfig in drivers) {
        if (!seriesDict.ContainsKey(driverConfig.Id)) return;
        var driverSeries = seriesDict[driverConfig.Id];
        if (driverSeries == null) return;

        // AR Driver
        if (driverConfig.P != null) {
          for (int j = 0, x_Count = driverSeries.X.Count; j < driverConfig.P.Length && j < x_Count; j++) {
            ar_part_drivers += driverConfig.P[j] * driverSeries.X[x_Count - 1 - j];
          }
        }

        // MA Driver
        if (driverConfig.Q != null) {
          for (int j = 0, e_Count = driverSeries.E.Count; j < driverConfig.Q.Length && j < e_Count; j++) {
            ma_part_drivers += driverConfig.Q[j] * driverSeries.E[e_Count - 1 - j];
          }
        }
      }
    }

    private IEnumerable<Argument> ComputeArguments(string[] arguments) {
      var args = new List<Argument>();
      foreach (var arg in arguments) {
        int timelagIdx = arg.IndexOf(TIMELAG_SEPARATOR);
        if (timelagIdx == -1) {
          args.Add(ConfigurationParser.ParseArgument(arg, GetValueFromSeries(arg, 0)));
        }
        else {
          string varId = arg.Substring(0, timelagIdx);
          if (int.TryParse(arg.Substring(timelagIdx + 1, arg.Length - (timelagIdx + 1)), out int varTimelag)) {
            args.Add(ConfigurationParser.ParseArgument(arg, GetValueFromSeries(varId, varTimelag)));
          }
          else {
            throw new InvalidCastException($"Error at parsing timelag for argument {arg}");
          }
        }
      }
      return args;
    }

    public static double ComputeFunction(Expression e, Argument arg) {
      e.removeAllArguments();
      e.addArguments(arg);
      return e.calculate();
    }

    public static double ComputeFunction(Expression e, Argument[] args) {
      e.removeAllArguments();
      e.addArguments(args);
      return e.calculate();
    }

    public static bool ComputeCondition(Expression e, Argument arg) {
      e.removeAllArguments();
      e.addArguments(arg);
      return e.calculate() == 1.0;
    }

    public static bool ComputeCondition(Expression e, Argument[] args) {
      e.removeAllArguments();
      e.addArguments(args);
      return e.calculate() == 1.0;
    }

    #endregion generation helper

    #region general helper
    private double ComputeStdDev(List<double> series) {
      double avg = series.Average();
      double sumOfSquaredDeviations = series.Sum(val => (val - avg) * (val - avg));
      return Math.Sqrt(sumOfSquaredDeviations / (series.Count - 1));
    }

    private double ComputeVar(List<double> series) {
      double avg = series.Average();
      double sumOfSquaredDeviations = series.Sum(val => (val - avg) * (val - avg));
      return sumOfSquaredDeviations / (series.Count - 1);
    }

    private double ComputePR(List<double> series) {
      var time = Enumerable.Range(1, series.Count).ToList();

      double y_ = series.Average();
      double x_ = series.Count / 2.0;

      double yDiff = Math.Sqrt(series.Sum(val => (val - y_) * (val - y_)));
      double xDiff = Math.Sqrt(time.Sum(val => (val - x_) * (val - x_)));

      var numerator = Enumerable.Range(0, series.Count).Sum(i => (time[i] - x_) * (series[i] - y_));

      return numerator / (xDiff * yDiff);
    }

    // compute b in "y = a + bx", where x = time
    private double ComputeLR_B(List<double> series) {
      var time = Enumerable.Range(1, series.Count).ToList();

      double y_ = series.Average();
      double x_ = series.Count / 2.0;

      // Cov(x,y) = mean of products minus product of means
      var numerator = Enumerable.Range(0, series.Count).Sum(i => (time[i] - x_) * (series[i] - y_));
      // sum of squared deviations from mean
      var denominator = time.Sum(val => (val - x_) * (val - x_));
      return numerator / denominator;
    }

    private double ComputeIntegral(List<double> dseries) {
      if (dseries.Count > 1) {
        var iseries = new List<double>();
        for (int i = 1; i < dseries.Count; i++) {
          iseries.Add(ComputeIntegralTrapezoid(dseries[i - 1], dseries[i]));
        }
        return ComputeIntegral(iseries);
      }
      else {
        return dseries.FirstOrDefault();
      }
    }

    private double ComputeIntegralTrapezoid(double fa, double fb) {
      // T = (b - a) * ((fa + fb) / 2)
      // b - a = 1
      return fa + fb / 2.0;
    }
    #endregion general helper
  }
}
