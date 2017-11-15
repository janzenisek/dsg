using System.Collections.Generic;
using System.Threading;

namespace DST.GeneratorNet {

  public class GeneratorConfig {
    public string Id { get; set; }
    public string Description { get; set; }
    public string Type { get; set; }
    public string BrokerHostName { get; set; }
    public string OutputFilePath { get; set; }
    public string DateTimeFormat { get; set; }
    public int Seed { get; set; }
    public long Duration { get; set; }
  }

  public class DriverConfig {
    public string Id { get; set; }
    public double[] P { get; set; }
    public double[] Q { get; set; }
  }

  public class SeriesConfig {
    public string Id { get; set; }
    public bool Export { get; set; }
    public int Delay { get; set; }
    public int Rank { get; set; }
    public string Title { get; set; }
    public string Topic { get; set; }
    public int Interval { get; set; }
  }

  public class ARSeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public double[] P { get; set; }
    public double OutlierRatio2s { get; set; }
    public double OutlierRatio3s { get; set; }
    public List<DriverConfig> Drivers { get; set; }
  }

  public class ARMASeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public double[] P { get; set; }
    public double[] Q { get; set; }
    public double OutlierRatio2s { get; set; }
    public double OutlierRatio3s { get; set; }
    public List<DriverConfig> Drivers { get; set; }
  }

  public class PFSeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public double TimeQuotient { get; set; }
    public string[] Arguments { get; set; }
    public string Expression { get; set; }
    public double OutlierRatio2s { get; set; }
    public double OutlierRatio3s { get; set; }
    public List<DriverConfig> Drivers { get; set; }
  }
}
