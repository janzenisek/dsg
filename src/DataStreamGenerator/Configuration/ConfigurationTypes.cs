using org.mariuszgromada.math.mxparser;

namespace DSG.Configuration {

  public class GeneratorConfig {
    public string Id { get; set; }
    public string Description { get; set; }
    public string Environment { get; set; }
    public int Seed { get; set; }
    public int Interval { get; set; }
    public string DateTimeFormat { get; set; }
    public DateTime StartDateTime { get; set; }
    public int DecimalPrecision { get; set; }
    public int Duration { get; set; }
    public string Type { get; set; }
    public bool Shuffle { get; set; }

    public string BrokerHostName { get; set; }
    public string OutputFilePath { get; set; }
    public string Separator { get; set; }
    public bool ExportIdAsHeader { get; set; }
    public bool ExportDateTime { get; set; }
    public bool ExportEventCount { get; set; }
    public int[] ExportLags { get; set; }

    public Random Rnd { get; set; }
    public string Group { get; set; }
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
    public double OutlierRatio1s { get; set; }
    public double OutlierRatio2s { get; set; }
  }

  public class ARSeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public double[] P { get; set; }
    public List<DriverConfig> Drivers { get; set; }
  }

  public class ARMASeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public double[] P { get; set; }
    public double[] Q { get; set; }
    public List<DriverConfig> Drivers { get; set; }
  }

  public class ARIMASeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public double[] P { get; set; }
    public double[] Q { get; set; }
    public int I { get; set; }
    public List<DriverConfig> Drivers { get; set; }
  }

  public class MESeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public string[] Arguments { get; set; }
    public string Expression { get; set; }
    public List<DriverConfig> Drivers { get; set; }

    private Expression exp;
    public Expression GetExpression() {
      if (exp == null) {
        string fHead = $"ME_{Id}({string.Join(",", Arguments)})";
        Function f = ConfigurationParser.ParseFunction(fHead, Expression);
        exp = new Expression(fHead, f);
      }
      return exp;
    }
  }

  public class MECSeriesConfig : MESeriesConfig {
    public string Condition { get; set; }
    public string ExpressionF { get; set; }

    private Expression exp2;
    public Expression GetExpressionF() {
      if (exp2 == null) {
        string fHead = $"MEC2_{Id}({string.Join(",", Arguments)})";
        Function f = ConfigurationParser.ParseFunction(fHead, ExpressionF);
        exp2 = new Expression(fHead, f);
      }
      return exp2;
    }

    private Expression cond;
    public Expression GetCondition() {
      if (cond == null) {
        string fHead = $"MEMC__{Id}({string.Join(",", Arguments)})";
        Function f = ConfigurationParser.ParseFunction(fHead, Condition);
        cond = new Expression(fHead, f);
      }
      return cond;
    }
  }

  public class MEMCSeriesConfig : SeriesConfig {
    public double C { get; set; }
    public double Mean { get; set; }
    public double StdDev { get; set; }
    public string[] Arguments { get; set; }
    public string[] Conditions { get; set; }
    public string[] Expressions { get; set; }
    public List<DriverConfig> Drivers { get; set; }

    private List<Expression> conditions;
    public List<Expression> GetConditions() {
      if (conditions == null) {
        conditions = new List<Expression>();
        for (int i = 0; i < Conditions.Length; i++) {
          string fHead = $"MEMC_C_{Id}{i}({string.Join(",", Arguments)})";
          Function f = ConfigurationParser.ParseFunction(fHead, Conditions[i]);
          conditions.Add(new Expression(fHead, f));
        }
      }
      return conditions;
    }

    private List<Expression> expressions;
    public List<Expression> GetExpressions() {
      if (expressions == null) {
        expressions = new List<Expression>();
        for (int i = 0; i < Expressions.Length; i++) {
          string fHead = $"MEMC_E_{Id}{i}({string.Join(",", Arguments)})";
          Function f = ConfigurationParser.ParseFunction(fHead, Expressions[i]);
          expressions.Add(new Expression(fHead, f));
        }
      }
      return expressions;
    }

  }

  public class XFSeriesConfig : SeriesConfig {
    public string SourcePath { get; set; }
    public string SourceIndex { get; set; }
  }

  public class XGSeriesConfig : SeriesConfig {
    public string SourceBroker { get; set; }
    public string SourceTopic { get; set; }
  }
}
