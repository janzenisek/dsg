/* 
 * DataStreamGenerator
 * Author: Jan Zenisek
 * Date: 05/2018
 */

using Nett;
using org.mariuszgromada.math.mxparser;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace DSG.GeneratorDotNet {

  public class ConfigurationParser {

    public static DateTime DETAULT_DATETIME = new DateTime(1, 1, 1, 0, 0, 0, 0);
    public const string ENV_DEVELOPMENT = "DEVELOPMENT";
    public const string ENV_PRODUCTION = "PRODUCTION";

    public static GeneratorConfig GetGeneratorConfig(string tomlFile) {
      TomlTable config = Toml.ReadFile(tomlFile);
      if (!config.ContainsKey("Id")) return null;
      else if (!config.ContainsKey("Type")) throw new MissingFieldException("Please provide a type in the generator configuration.");
      var gConfig = new GeneratorConfig();

      gConfig.Id                = config.Get<String>("Id");
      gConfig.Type              = config.Get<String>("Type");
      gConfig.Description       = config.ContainsKey("Description") ? config.Get<String>("Description") : null;
      gConfig.Environment       = config.ContainsKey("Environment") ? config.Get<String>("Environment") : ENV_DEVELOPMENT;
      gConfig.DateTimeFormat    = config.ContainsKey("DateTimeFormat") ? config.Get<String>("DateTimeFormat") : null;
      gConfig.StartDateTime     = config.ContainsKey("StartDateTime") ? ParseDateTime(config.Get<String>("StartDateTime"), gConfig.DateTimeFormat) : DETAULT_DATETIME;
      gConfig.Seed              = config.ContainsKey("Seed") ? config.Get<int>("Seed") : -1;
      gConfig.Shuffle           = config.ContainsKey("Shuffle") ? config.Get<bool>("Shuffle") : false;
      gConfig.Interval          = config.ContainsKey("Interval") ? config.Get<int>("Interval") : -1;
      gConfig.DecimalPrecision  = config.ContainsKey("DecimalPrecision") ? config.Get<int>("DecimalPrecision") : -1;
      gConfig.Duration          = config.ContainsKey("Duration") ? config.Get<int>("Duration") : -1;
      gConfig.BrokerHostName    = config.ContainsKey("BrokerHostName") ? config.Get<String>("BrokerHostName") : null;
      gConfig.OutputFilePath    = config.ContainsKey("OutputFilePath") ? config.Get<String>("OutputFilePath") : null;
      gConfig.Separator         = config.ContainsKey("Separator") ? config.Get<String>("Separator") : null;
      gConfig.ExportIdAsHeader  = config.ContainsKey("ExportIdAsHeader") ? config.Get<bool>("ExportIdAsHeader") : true;
      gConfig.ExportDateTime    = config.ContainsKey("ExportDateTime") ? config.Get<bool>("ExportDateTime") : true;
      gConfig.ExportEventCount  = config.ContainsKey("ExportEventCount") ? config.Get<bool>("ExportEventCount") : true;
      gConfig.ExportLags        = config.ContainsKey("ExportLags") ? config.Get<int[]>("ExportLags") : null;

      if (gConfig.Seed >= 0) gConfig.Rnd = new Random(gConfig.Seed);
      else gConfig.Rnd = new Random();
      
      if(gConfig.Environment.ToUpper() == ENV_DEVELOPMENT) {
        Random groupNameRnd = new Random();
        gConfig.Group = gConfig.Id + "_" + groupNameRnd.Next(1, 10000);
      } else if(gConfig.Environment.ToUpper() == ENV_PRODUCTION) {
        gConfig.Group = gConfig.Id;
      }

      return gConfig;
    }

    public static Dictionary<string, SeriesConfig> GetSeriesConfigurations(string tomlFile) {
      TomlTable config = Toml.ReadFile(tomlFile);
      var arSeries = config.ContainsKey("AR") ? config.Get<List<ARSeriesConfig>>("AR") : new List<ARSeriesConfig>();
      var armaSeries = config.ContainsKey("ARMA") ? config.Get<List<ARMASeriesConfig>>("ARMA") : new List<ARMASeriesConfig>();
      var arimaSeries = config.ContainsKey("ARIMA") ? config.Get<List<ARMASeriesConfig>>("ARIMA") : new List<ARMASeriesConfig>();
      var meSeries = config.ContainsKey("ME") ? config.Get<List<MESeriesConfig>>("ME") : new List<MESeriesConfig>();
      var mecSeries = config.ContainsKey("MEC") ? config.Get<List<MECSeriesConfig>>("MEC") : new List<MECSeriesConfig>();
      var memcSeries = config.ContainsKey("MEMC") ? config.Get<List<MEMCSeriesConfig>>("MEMC") : new List<MEMCSeriesConfig>();
      var xfSeries = config.ContainsKey("XF") ? config.Get<List<XFSeriesConfig>>("XF") : new List<XFSeriesConfig>();
      var xgSeries = config.ContainsKey("XG") ? config.Get<List<XGSeriesConfig>>("XG") : new List<XGSeriesConfig>();

      var seriesConfigs = new Dictionary<string, SeriesConfig>();

      foreach(var item in arSeries) {
        seriesConfigs.Add(item.Id, item);
      }

      foreach (var item in armaSeries) {
        seriesConfigs.Add(item.Id, item);
      }

      foreach (var item in arimaSeries) {
        seriesConfigs.Add(item.Id, item);
      }

      foreach (var item in meSeries) {
        seriesConfigs.Add(item.Id, item);
      }

      foreach (var item in mecSeries) {
        seriesConfigs.Add(item.Id, item);
      }

      foreach (var item in memcSeries) {
        seriesConfigs.Add(item.Id, item);
      }

      foreach(var item in xfSeries) {
        seriesConfigs.Add(item.Id, item);
      }

      foreach (var item in xgSeries) {
        seriesConfigs.Add(item.Id, item);
      }

      return seriesConfigs;
    }

    public static DateTime ParseDateTime(string dt, string format) {
      if (string.IsNullOrWhiteSpace(dt) || string.IsNullOrWhiteSpace(format)) return new DateTime(1,1,1,0,0,0,0);
      var ci = new CultureInfo("en-US");
      return DateTime.ParseExact(dt, format, ci);
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
  }
}
