using DSG.Configuration;
using DSG.Utils;

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
          seriesDict.Add(sc.Key, new DoubleSeries());
        }
      }

      // stream or generate
      switch (gType) {
        case TYPE_STREAM_DATETIMEBASED_SINGLETHREADED:
          //StreamSingleThreaded(generatorConfig, seriesDict, seriesConfigs);
          break;
        case TYPE_STREAM_DATETIMEBASED_MULTITASKED:
          //StreamMultiTasked(generatorConfig, seriesDict, seriesConfigs);
          break;
        case TYPE_GENERATE_EVENTCOUNTBASED:
          //GenerateEventCountBased(generatorConfig, seriesDict, seriesConfigs);
          break;
        case TYPE_GENERATE_DATETIMEBASED:
          //GenerateDateTimeBased(generatorConfig, seriesDict, seriesConfigs);
          break;
      }
    }

    #region setup and init
    #endregion setup and init

    #region stream
    #endregion stream

    #region generate
    #endregion generate
  }
}