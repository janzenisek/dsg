using DSG.Configuration;
using DSG.DataStreamGenerator;
using DSG.DataStreamGenerator.Configuration;
using DSG.Utils;
using org.mariuszgromada.math.mxparser;

namespace DSG {
  public class Program {
    static void Main(string[] args) {
      Console.WriteLine("DataStreamGenerator.Console");
      Console.WriteLine("-------------------\n");

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
      GeneratorConfig? generatorConfig = null;
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
        return;
      }
      else if (seriesConfigs.Count == 0) {
        Console.WriteLine("Error: no series configurations have been parsed.");
        return;
      }

      var runner = new DataStreamRunner(generatorConfig, seriesConfigs);
      string gType = generatorConfig.Type.Trim();
      if (gType == DataStreamRunner.TYPE_STREAM_DATETIMEBASED_SINGLETHREADED
        || gType == DataStreamRunner.TYPE_STREAM_DATETIMEBASED_MULTITASKED) {
        var userCts = new CancellationTokenSource();
        Task.Run(() => {
          if (Reader.ReadLine() != null) {
            try { userCts.Cancel(); } catch (ObjectDisposedException) { }
          }
        });
        runner.Run(userCts.Token);
      }
      else {
        runner.Run();
      }
    }
  }
}
