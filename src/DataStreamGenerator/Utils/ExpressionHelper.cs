using org.mariuszgromada.math.mxparser;

namespace DSG.DataStreamGenerator.Utils {

  internal static class ExpressionHelper {
    public static Function ParseFunction(string fHead, string fBody) {
      var f = new Function($"{fHead} = {fBody}");
      if (!f.checkSyntax()) {
        Console.WriteLine("Please check syntax of function: " + f.getErrorMessage());
      }
      return f;
    }

    public static Argument ParseArgument(string argName, double argValue) {
      return new Argument($"{argName} = {argValue}");
    }
  }
}
