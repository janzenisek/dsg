using DSG.Configuration;
using DSG.Utils;
using MQTTnet.Extensions.ManagedClient;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSG {
  public class IntSeriesGenerator {

    public const double MAX_BUFFER_SIZE = 500;
    public const string ID_TIMEELAPSED = "t";
    public const string ID_EVENTCOUNT = "c";
    public const string ID_EVENTITERATOR = "i";
    public const char TIMELAG_SEPARATOR = '_';
    public const char CSV_SEPARATOR = ';';
    public CultureInfo provider;
    public CancellationTokenSource cts;

    private IntSeries series;
    public SeriesConfig config;
    private Dictionary<string, IntSeries> seriesDict;
    private Dictionary<string, SeriesConfig> configDict;

    private GeneratorConfig generatorConfig;
    private IManagedMqttClient client;
    private Random rnd;

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

    public IntSeriesGenerator(GeneratorConfig generatorConfig, Dictionary<string, IntSeries> seriesDict, IntSeries series, SeriesConfig config, Dictionary<string, SeriesConfig> configDict) {
      provider = CultureInfo.InvariantCulture;
      initialized = false;
      cts = new CancellationTokenSource();

      this.generatorConfig = generatorConfig;
      this.rnd = generatorConfig.Rnd;
      this.seriesDict = seriesDict;
      this.series = series;
      this.config = config;
      this.configDict = configDict;
      this.count = series.X.Count;
      this.iter = series.X.Count;
      //this.count = 0;
      //this.iter = 0;

      WarmUp();
    }

    public void WarmUp() {
      dynamic c = config;
      driversActive = false;
      for (int i = 0; i < config.Delay; i++) {
        GenerateSeriesNextValue();
      }
      SetTimeAndCount(generatorConfig.StartDateTime, 1);
      driversActive = true;
    }

    public void TearDown() {
      if (client != null) {
        client.StopAsync().Wait(cts.Token);
        cts.Cancel();
        client.Dispose();
        client = null;
      }
    }

    public void SetTimeAndCount(DateTime startTime, long count) {
      this.time = startTime;
      this.count = count;
    }

    public double GenerateSeriesNextValue() {
      dynamic c = config;

      double xt = 0.0;
      try {
        // calculate next series value
        //xt = ComputeNextX(rnd, series, c);

        //// calculate possible outlier
        //double outlierCandidate = rnd.NextDouble();
        //if (outlierCandidate < c.OutlierRatio2s) {
        //  xt = GenerateOutlier(rnd, xt, series.X, 3);
        //}
        //else if (outlierCandidate < c.OutlierRatio1s) {
        //  xt = GenerateOutlier(rnd, xt, series.X, 2);
        //}

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
    private int ComputeNextX(Random rnd, DoubleSeries s, ARSeriesConfig c) {

      var xt = rnd.Next(1, 4);

      return xt;

    }


    #endregion next value computation

  }
}
