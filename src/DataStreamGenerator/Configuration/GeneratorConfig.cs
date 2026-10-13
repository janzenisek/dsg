using System.Text.Json.Serialization;

namespace DSG.DataStreamGenerator.Configuration {

  public class GeneratorConfig {
    public static readonly string ENV_DEVELOPMENT = "DEVELOPMENT";
    public static readonly string ENV_PRODUCTION = "PRODUCTION";

    public string Id { get; set; } = "";
    public string? Description { get; set; }
    public string Environment { get; set; } = ENV_DEVELOPMENT;
    public int Seed { get; set; } = -1;
    public int Interval { get; set; } = -1;
    public string? DateTimeFormat { get; set; }
    public DateTime StartDateTime { get; set; } = new DateTime(1, 1, 1, 0, 0, 0, 0);
    public int DecimalPrecision { get; set; } = -1;
    public int Duration { get; set; } = -1;
    public string Type { get; set; } = "";
    public bool Shuffle { get; set; }
    public bool Disclosure { get; set; }

    public string? BrokerHostName { get; set; }
    public int BrokerHostPort { get; set; } = -1;
    public string? OutputFilePath { get; set; }
    public string? Separator { get; set; }
    public bool ExportIdAsHeader { get; set; } = true;
    public bool ExportDateTime { get; set; } = true;
    public bool ExportEventCount { get; set; } = true;
    public bool ExportDiff { get; set; }
    public bool ExportDiffFPS { get; set; }
    public bool ExportMovingAvg { get; set; }
    public int ExportMovingAvgWindowSize { get; set; }
    public int ExportLagOffset { get; set; }
    public int[]? ExportLags { get; set; }

    [JsonIgnore]
    public Random? Rnd { get; set; }

    [JsonIgnore]
    public string? Group { get; set; }

    [JsonIgnore]
    public Dictionary<string, List<string>>? Genealogy;

    public void EnsureInitialized() {
      if (!string.IsNullOrEmpty(OutputFilePath)) OutputFilePath = Path.GetFullPath(OutputFilePath);

      if (Seed >= 0) Rnd = new Random(Seed);
      else Rnd = new Random();

      if (string.Equals(Environment, ENV_DEVELOPMENT, StringComparison.OrdinalIgnoreCase)) {
        Random groupNameRnd = new Random();
        Group = Id + "_" + groupNameRnd.Next(1, 10000);
      }
      else if (string.Equals(Environment, ENV_PRODUCTION, StringComparison.OrdinalIgnoreCase)) {
        Group = Id;
      }
    }
  }
}
