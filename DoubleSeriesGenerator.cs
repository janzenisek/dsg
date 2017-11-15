using DST.Utils;
using Newtonsoft.Json;
using org.mariuszgromada.math.mxparser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DST.GeneratorNet {
  public class DoubleSeriesGenerator : Generator {

    public const double MAX_BUFFER_SIZE = 100;

    private Random rnd;
    private DoubleSeries series;
    private SeriesConfig config;
    private Dictionary<string, DoubleSeries> seriesDict;
    
    private string group;
    GeneratorConfig generatorConfig;

    public DoubleSeriesGenerator(string group, GeneratorConfig generatorConfig, Dictionary<string, DoubleSeries> seriesDict, Random rnd, DoubleSeries series, SeriesConfig config) {
      this.group = group;
      this.generatorConfig = generatorConfig;
      this.seriesDict = seriesDict;
      this.rnd = rnd;
      this.series = series;
      this.config = config;
    }
    
    public void StreamSeries(StreamingJob job) {
      if (config.GetType() == typeof(ARMASeriesConfig)) {
        var c = (ARMASeriesConfig)config;
        StreamSeries(job, c);
      } else if(config.GetType() == typeof(PFSeriesConfig)) {
        var c = (PFSeriesConfig)config;
        StreamSeries(job, c);
      }
    }

    private void StreamSeries(StreamingJob job, ARMASeriesConfig c) {
      DateTime dtts = new DateTime(1000, 1, 1, 0, 0, 0, 0);

      for (long i = 0; true; i++) {
        job.Token.ThrowIfCancellationRequested();

        double xt = 0.0;
        try {
          lock (seriesDict)
            xt = CalculateNextX(rnd, series, c);
        } catch(Exception ex) {
          Console.WriteLine(ex.Message + "\n" + ex.Source);
        }


        if (c.Export && i > c.Delay) {

          double outlierCandidate = rnd.NextDouble();
          if (outlierCandidate < c.OutlierRatio3s) {
            xt = GenerateOutlier(rnd, xt, series.X, 3, 5);
          } else if (outlierCandidate < c.OutlierRatio2s) {
            xt = GenerateOutlier(rnd, xt, series.X, 2, 3);
          }

          // string timestamp = DateTime.Now.ToString(DATETIME_FORMAT); // v1
          string timestamp = dtts.ToString(generatorConfig.DateTimeFormat); // v2
          var msg = JsonConvert.SerializeObject(GenerateMessage(c, group, timestamp, xt));
          job.Client.Publish(config.Topic, Encoding.UTF8.GetBytes(msg));
          Thread.Sleep(c.Interval);
          dtts = dtts.AddMilliseconds(c.Interval); // v2
        } else if(i > c.Delay) {
          Thread.Sleep(c.Interval);
        }
      }
    }

    private void StreamSeries(StreamingJob job, PFSeriesConfig c) {
      DateTime dtts = new DateTime(1000, 1, 1, 0, 0, 0, 0);
      string fHead = $"PF({string.Join(",", c.Arguments)})";
      Function f = ConfigurationParser.ParseFunction(fHead, c.Expression);

      for (long i = 0; true; i++) {
        job.Token.ThrowIfCancellationRequested();

        double xt = 0.0;
        lock(seriesDict)
          xt = CalculateNextX(rnd, series, c, f, fHead, i);

        if (c.Export && i > c.Delay) {

          double outlierCandidate = rnd.NextDouble();
          if (outlierCandidate < c.OutlierRatio3s) {
            xt = GenerateOutlier(rnd, xt, series.X, 3, 4);
          } else if (outlierCandidate < c.OutlierRatio2s) {
            xt = GenerateOutlier(rnd, xt, series.X, 2, 3);
          }

          // string timestamp = DateTime.Now.ToString(DATETIME_FORMAT); // v1
          string timestamp = dtts.ToString(generatorConfig.DateTimeFormat); // v2
          var msg = JsonConvert.SerializeObject(GenerateMessage(c, group, timestamp, xt));
          job.Client.Publish(config.Topic, Encoding.UTF8.GetBytes(msg));
          Thread.Sleep(c.Interval);
          dtts = dtts.AddMilliseconds(c.Interval); // v2
        } else if(i > c.Delay) {
          Thread.Sleep(c.Interval);
        }
      }
    }

    public List<double> GenerateSeries() {
      // TODO
      return null;
    }

    public void SliceSeries(List<DoubleSeries> series, int eventCount) {
      for (int i = 0; i < series.Count; i++) {
        series[i].X.RemoveRange(0, eventCount);
      }
    }

    public List<double> EvaluateSeries1(List<DoubleSeries> series) {
      int eventCount = series.Min(x => x.X.Count);
      var y = new List<double>();

      for (int i = 0; i < eventCount; i++) {

        double yt =
          1 / (series.ElementAt(0).X[i] * series.ElementAt(1).X[i] - series.ElementAt(2).X[i])
          + (series.ElementAt(3).X[i] / 3 - series.ElementAt(4).X[i] / 2 - series.ElementAt(5).X[i])
          + (series.ElementAt(6).X[i] / series.ElementAt(7).X[i] / series.ElementAt(8).X[i] / series.ElementAt(9).X[i]);
        y.Add(yt);
      }

      return y;

    }

    public List<double> EvaluateSeries2(List<DoubleSeries> series) {
      int eventCount = series.Min(x => x.X.Count);
      var y = new List<double>();

      var p0 = new[] { 0.00, 0.50, 0.75, 1.00 };
      var p6 = new[] { 0.50, 0.25 };

      for (int i = 0; i < eventCount; i++) {
        double yt = 0.0, x0 = 0.0, x6 = 0.0;

        for (int p = 0; i - p >= 0 && p < p0.Length; p++) {
          x0 += series.ElementAt(0).X[i - p] * p0[p];
        }

        for (int p = 0; i - p >= 0 && p < p6.Length; p++) {
          x6 += series.ElementAt(6).X[i - p] * p6[p];
        }

        yt = x0 * x6;
        y.Add(yt);
      }
      return y;
    }

    private double CalculateNextX(Random rnd, DoubleSeries s, ARMASeriesConfig c) {
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
        CalculateDriverParts(c.Drivers, out ar_part_drivers, out ma_part_drivers);

      var et = rnd.NextGaussian_BoxMuller(c.Mean, c.StdDev); // next noise/shock
      var xt = c.C + et + ar_part + ma_part + ar_part_drivers + ma_part_drivers; // next sensor value

      lock(seriesDict) {
        if (s.X.Count > MAX_BUFFER_SIZE) s.X.RemoveAt(0);
        if (s.E.Count > MAX_BUFFER_SIZE) s.E.RemoveAt(0);
        s.E.Add(et);
        s.X.Add(xt);
      }

      return xt;
    }

    private double CalculateNextX(Random rnd, DoubleSeries s, PFSeriesConfig c, Function f, string fHead, long i) {
      double t = i / c.TimeQuotient;
      var arg = ConfigurationParser.ParseArgument("t", t);
      var periodicPart = ConfigurationParser.ComputeFunction(fHead, f, arg);

      var ar_part_drivers = 0.0;
      var ma_part_drivers = 0.0;
      lock(seriesDict)
        CalculateDriverParts(c.Drivers, out ar_part_drivers, out ma_part_drivers);

      var et = rnd.NextGaussian_BoxMuller(c.Mean, c.StdDev); // next noise
      var xt = c.C + periodicPart + ar_part_drivers + ma_part_drivers + et;

      lock (seriesDict) {
        if (s.X.Count > MAX_BUFFER_SIZE) s.X.RemoveAt(0);
        s.X.Add(xt);
      }
      
      return xt;
    }

    private void CalculateDriverParts(List<DriverConfig> drivers, out double ar_part_drivers, out double ma_part_drivers) {
      ar_part_drivers = 0.0;
      ma_part_drivers = 0.0;
      if (drivers == null || drivers.Count == 0) return;

      foreach (var driverConfig in drivers) {
        if (!seriesDict.ContainsKey(driverConfig.Id)) return;
        var driverSeries = seriesDict[driverConfig.Id];
        if (driverSeries == null) return;

        // AR Driver
        if(driverConfig.P != null) {
          for (int j = 0, x_Count = driverSeries.X.Count; j < driverConfig.P.Length && j < x_Count; j++) {
            ar_part_drivers += driverConfig.P[j] * driverSeries.X[x_Count - 1 - j];
          }
        }

        // MA Driver
        if(driverConfig.Q != null) {
          for (int j = 0, e_Count = driverSeries.E.Count; j < driverConfig.Q.Length && j < e_Count; j++) {
            ma_part_drivers += driverConfig.Q[j] * driverSeries.E[e_Count - 1 - j];
          }
        }
      }
    }

    private double CalculateNextXSimple(Random rnd, DoubleSeries s, ARMASeriesConfig c) {

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

      var et = rnd.NextGaussian_BoxMuller(c.Mean, c.StdDev); // next noise/shock
      var xt = c.C + et + ar_part + ma_part; // next sensor value

      s.E.Add(et);
      s.X.Add(xt);

      return xt;
    }

    private double CalculateNextXSimple(Random rnd, DoubleSeries s, PFSeriesConfig c, Function f, string fHead, long i) {
      double t = i / c.TimeQuotient;
      var arg = ConfigurationParser.ParseArgument("t", t);
      var xt = ConfigurationParser.ComputeFunction(fHead, f, arg);
      s.X.Add(xt);
      return xt;
    }

    private Message GenerateMessage(SeriesConfig c, string _group, string _timestamp, double _value) {
      return new Message() {
        id = c.Id, group = _group, rank = c.Rank, title = c.Title, timestamp = _timestamp, value = _value
      };
    }

    private SlimMessage GenerateSlimMessage(SeriesConfig c, string _group, string _timestamp, double _value) {
      return new SlimMessage() {
        id = c.Id, value = _value
      };
    }

    private double GenerateOutlier(Random rnd, double xt, List<double> series, double f1, double f2) {
      double stdDev = ComputeStdDev(series);
      double avg = series.Average();
      double diff = (series.Count > 2) ? series[series.Count - 2] - series[series.Count - 3] : 0.0;
      double outlier = 0.0;
      if (diff >= 0.0) {
        outlier = avg + rnd.NextDouble(stdDev * f1, stdDev * f2);
      } else {
        outlier = avg - rnd.NextDouble(stdDev * f1, stdDev * f2);
      }
      return outlier;
    }

    private double ComputeStdDev(List<double> series) {
      double avg = series.Average();
      double sumOfSquaresOfDifferences = series.Sum(val => (val - avg) * (val - avg));
      return Math.Sqrt(sumOfSquaresOfDifferences / (series.Count - 1));
    }

  }
}
