using Nett;
using org.mariuszgromada.math.mxparser;
using System;
using System.Collections.Generic;

namespace DST.GeneratorNet {

  public class ConfigurationParser {

    public static GeneratorConfig GetGeneratorConfig(string tomlFile) {
      TomlTable config = Toml.ReadFile(tomlFile);
      var gConfig = new GeneratorConfig();
      gConfig.Id = config.Get<String>("Id");
      gConfig.Type = config.Get<String>("Type");
      gConfig.BrokerHostName = config.ContainsKey("BrokerHostName") ? config.Get<String>("BrokerHostName") : null;
      gConfig.OutputFilePath = config.ContainsKey("OutputFilePath") ? config.Get<String>("OutputFilePath") : null;
      gConfig.Description = config.Get<String>("Description");
      gConfig.DateTimeFormat = config.Get<String>("DateTimeFormat");
      gConfig.Seed = config.ContainsKey("Seed") ? config.Get<int>("Seed") : -1;
      gConfig.Duration = config.ContainsKey("Duration") ? config.Get<long>("Duration") : -1;
      return gConfig;
    }

    public static Dictionary<string, SeriesConfig> GetSeriesConfigurations(string tomlFile) {
      TomlTable config = Toml.ReadFile(tomlFile);
      var armaSeries = config.Get<List<ARMASeriesConfig>>("ARMA");
      var pfSeries = config.Get<List<PFSeriesConfig>>("PF");
      var seriesConfigs = new Dictionary<string, SeriesConfig>();

      foreach (var item in armaSeries) {
        seriesConfigs.Add(item.Id, item);
      }

      foreach (var item in pfSeries) {
        seriesConfigs.Add(item.Id, item);
      }

      return seriesConfigs;
    }

    public static Function ParseFunction(string fHead, string fBody) {
      var f = new Function($"{fHead} = {fBody}");
      if(!f.checkSyntax()) {
        Console.WriteLine("Please check syntax of function: " + f.getErrorMessage());
      }
      return f;
    }

    public static Argument ParseArgument(string argName, double argValue) {
      return new Argument($"{argName} = {argValue}");
    }

    public static double ComputeFunction(string fHead, Function f, Argument arg) {
      Expression e = new Expression(fHead, f, arg);
      return e.calculate();
    }

    public static double ComputeFunction(string fHead, Function f, Argument[] args) {
      Expression e = new Expression(fHead, f);
      e.addArguments(args);
      return e.calculate();
    }

    private static void TestConfigurationParser() {
      Console.WriteLine("DataStreamGenerator Configuration Parser .NET");
      Console.WriteLine("=============================================\n");

      string tomlFile = @"C:\Users\P41608\Desktop\DSG_conf1.toml";
      ConfigurationParser.GetSeriesConfigurations(tomlFile);
    }

    private static void TestMathParser() {
      string fHead = "PF(t)";
      double d = 5.0;
      string fBody = "10 * ( sin(t + sin(2*t)/2 + sin(5*t)/7) )";
      Function pf = new Function($"{fHead} = {fBody}");

      double n = 10000;
      for (int i = 0; i < n; i++) {
        Argument t = new Argument($"t = {i / d}");
        Expression e = new Expression(fHead, pf, t);
        Console.WriteLine($"{e.getExpressionString()} = {e.calculate()}");
      }
    }
  }
}
