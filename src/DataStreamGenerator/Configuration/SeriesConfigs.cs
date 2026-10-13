using DSG.DataStreamGenerator.Utils;
using System.Text.Json.Serialization;

namespace DSG.DataStreamGenerator.Configuration {

  public class DriverConfig {
    public string Id { get; set; } = "";
    public double[]? P { get; set; }
    public double[]? Q { get; set; }
  }

  [JsonPolymorphic(TypeDiscriminatorPropertyName = "seriesType")]
  [JsonDerivedType(typeof(ARSeriesConfig), "AR")]
  [JsonDerivedType(typeof(ARMASeriesConfig), "ARMA")]
  [JsonDerivedType(typeof(ARIMASeriesConfig), "ARIMA")]
  [JsonDerivedType(typeof(MESeriesConfig), "ME")]
  [JsonDerivedType(typeof(MECSeriesConfig), "MEC")]
  [JsonDerivedType(typeof(MEMCSeriesConfig), "MEMC")]
  [JsonDerivedType(typeof(XFSeriesConfig), "XF")]
  [JsonDerivedType(typeof(XGSeriesConfig), "XG")]
  [JsonDerivedType(typeof(IntSeqSeriesConfig), "IS")]
  public class SeriesConfig {
    public string Id { get; set; } = "";
    public bool Export { get; set; }
    public int Delay { get; set; }
    public int Rank { get; set; }
    public string? Title { get; set; }
    public string? Topic { get; set; }
    public int Interval { get; set; }
    public double OutlierRatio1s { get; set; }
    public double OutlierRatio2s { get; set; }

    public double InitialValue { get; set; }
  }

  public class ARSeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public double[]? P { get; set; }
    public List<DriverConfig>? Drivers { get; set; }
  }

  public class ARMASeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public double[]? P { get; set; }
    public double[]? Q { get; set; }
    public List<DriverConfig>? Drivers { get; set; }
  }

  public class ARIMASeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public double[]? P { get; set; }
    public double[]? Q { get; set; }
    public int I { get; set; }
    public List<DriverConfig>? Drivers { get; set; }
  }

  public class MESeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public string[] Arguments { get; set; } = Array.Empty<string>();
    public string? Expression { get; set; }
    public List<DriverConfig>? Drivers { get; set; }

    [JsonIgnore]
    private org.mariuszgromada.math.mxparser.Expression? exp;
    internal org.mariuszgromada.math.mxparser.Expression GetExpression() {
      if (exp == null) {
        string fHead = $"ME_{Id}({string.Join(",", Arguments)})";
        var f = ExpressionHelper.ParseFunction(fHead, Expression ?? "");
        exp = new org.mariuszgromada.math.mxparser.Expression(fHead, f);
      }
      return exp;
    }
  }

  public class MECSeriesConfig : MESeriesConfig {
    public string? Condition { get; set; }
    public string? ExpressionF { get; set; }

    [JsonIgnore]
    private org.mariuszgromada.math.mxparser.Expression? exp2;
    internal org.mariuszgromada.math.mxparser.Expression GetExpressionF() {
      if (exp2 == null) {
        string fHead = $"MEC2_{Id}({string.Join(",", Arguments)})";
        var f = ExpressionHelper.ParseFunction(fHead, ExpressionF ?? "");
        exp2 = new org.mariuszgromada.math.mxparser.Expression(fHead, f);
      }
      return exp2;
    }

    [JsonIgnore]
    private org.mariuszgromada.math.mxparser.Expression? cond;
    internal org.mariuszgromada.math.mxparser.Expression GetCondition() {
      if (cond == null) {
        string fHead = $"MEMC__{Id}({string.Join(",", Arguments)})";
        var f = ExpressionHelper.ParseFunction(fHead, Condition ?? "");
        cond = new org.mariuszgromada.math.mxparser.Expression(fHead, f);
      }
      return cond;
    }
  }

  public class MEMCSeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public string[] Arguments { get; set; } = Array.Empty<string>();
    public string[]? Conditions { get; set; }
    public string[]? Expressions { get; set; }
    public List<DriverConfig>? Drivers { get; set; }

    [JsonIgnore]
    private List<org.mariuszgromada.math.mxparser.Expression>? conditions;
    internal List<org.mariuszgromada.math.mxparser.Expression> GetConditions() {
      if (conditions == null) {
        conditions = new List<org.mariuszgromada.math.mxparser.Expression>();
        for (int i = 0; i < (Conditions?.Length ?? 0); i++) {
          string fHead = $"MEMC_C_{Id}{i}({string.Join(",", Arguments)})";
          var f = ExpressionHelper.ParseFunction(fHead, Conditions![i]);
          conditions.Add(new org.mariuszgromada.math.mxparser.Expression(fHead, f));
        }
      }
      return conditions;
    }

    [JsonIgnore]
    private List<org.mariuszgromada.math.mxparser.Expression>? expressions;
    internal List<org.mariuszgromada.math.mxparser.Expression> GetExpressions() {
      if (expressions == null) {
        expressions = new List<org.mariuszgromada.math.mxparser.Expression>();
        for (int i = 0; i < (Expressions?.Length ?? 0); i++) {
          string fHead = $"MEMC_E_{Id}{i}({string.Join(",", Arguments)})";
          var f = ExpressionHelper.ParseFunction(fHead, Expressions![i]);
          expressions.Add(new org.mariuszgromada.math.mxparser.Expression(fHead, f));
        }
      }
      return expressions;
    }

  }

  public class XFSeriesConfig : SeriesConfig {
    public string? SourcePath { get; set; }
    public string? SourceIndex { get; set; }
  }

  public class XGSeriesConfig : SeriesConfig {
    public string? SourceBroker { get; set; }
    public string? SourceTopic { get; set; }
  }

  public class IntSeqSeriesConfig : SeriesConfig {
    public int Min { get; set; }
    public int Max { get; set; }
    public int MinLength { get; set; }
    public int MaxLength { get; set; }
    public string? Strategy { get; set; }
  }
}
