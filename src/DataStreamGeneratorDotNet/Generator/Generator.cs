/* 
 * DataStreamGenerator
 * Author: Jan Zenisek
 * Date: 05/2018
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DSG.GeneratorDotNet {
  public abstract class Generator {

    public void WriteToStream(Dictionary<string, List<double>> data, StreamWriter sw, string separator, int precision, int eventCount) {
      var keys = data.Keys.ToList();
      for (int i = 0; i < data.Keys.Count; i++) {
        if (i > 0) sw.Write(separator);
        sw.Write($"{keys[i]}");
      }
      sw.WriteLine();


      for (int i = 0; i < eventCount; i++) {
        int j = 0;
        foreach (var key in data.Keys) {
          if (j > 0) sw.Write(separator);
          var value = (i < data[key]?.Count) ? Math.Round(data[key][i], precision).ToString() : "";
          sw.Write($"{value}");
          j++;
        }
        sw.WriteLine();
      }
      sw.WriteLine();
    }

    public static void WriteMatrixToStream(double[,] matrix, StreamWriter sw, string separator, int precision) {

      for (int i = 0; i < matrix.GetLength(0); i++) {
        for (int j = 0; j < matrix.GetLength(1); j++) {
          if (j > 0) sw.Write(separator);
          sw.Write(Math.Round(matrix[i, j], precision));
        }
        sw.WriteLine();
      }
    }

  }
}
