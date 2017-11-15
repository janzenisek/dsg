using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DST.Utils {

  public static class ExtensionMethods {
    public static double NextGaussian_BoxMuller(this Random rnd, double mean = 0.0, double stdDev = 1.0) {
      double u1 = rnd.NextDouble(); // uniform(0,1) random doubles
      double u2 = rnd.NextDouble();
      double rndStdNormal = Math.Cos(2.0 * Math.PI * u1) * Math.Sqrt(-2.0 * Math.Log(u2)); // random normal(0,1)
      return mean + stdDev * rndStdNormal;
    }

    public static double[] NextGaussians_BoxMuller(this Random rnd, double mean = 0.0, double stdDev = 1.0) {
      double u1 = rnd.NextDouble(); // uniform(0,1) random doubles
      double u2 = rnd.NextDouble();
      double rndStdNormal1 = Math.Sin(2.0 * Math.PI * u1) * Math.Sqrt(-2.0 * Math.Log(u2));
      double rndStdNormal2 = Math.Cos(2.0 * Math.PI * u1) * Math.Sqrt(-2.0 * Math.Log(u2));
      return new[] { mean + stdDev * rndStdNormal1, mean + stdDev * rndStdNormal2 };
    }

    public static double NextGaussian_Polar(this Random rnd) {
      double u1 = 0.0, u2 = 0.0, q = 0.0, p = 0.0;

      do {
        u1 = rnd.NextDouble(-1.0, 1.0);
        u2 = rnd.NextDouble(-1.0, 1.0);
        q = u1 * u1 + u2 * u2;
      } while (q == 0.0 || q > 1.0);

      p = Math.Sqrt(-2 * Math.Log(q) / q);
      return u1 * p;
    }

    public static double[] NextGaussians_Polar(this Random rnd) {
      double u1 = 0.0, u2 = 0.0, q = 0.0, p = 0.0;

      do {
        u1 = rnd.NextDouble(-1.0, 1.0);
        u2 = rnd.NextDouble(-1.0, 1.0);
        q = u1 * u1 + u2 * u2;
      } while (q == 0.0 || q > 1.0);

      p = Math.Sqrt(-2 * Math.Log(q) / q);
      return new[] { u1 * p, u2 * p };
    }

    public static double NextDouble(this Random rnd, double min, double max) {
      return rnd.NextDouble() * (max - min) + min;
    }

    public static IEnumerable<T> TakeLast<T>(this IEnumerable<T> source, int N) {
      return source.Skip(Math.Max(0, source.Count() - N));
    }

    public static IEnumerable<T> TakeLast<T>(this IEnumerable<T> source, double R) {
      if (R < 0 || R > 1.0) return null;
      var rn = (int)Math.Floor(source.Count() * R);
      return source.Skip(Math.Max(0, source.Count() - rn));
    }
    public static double ComputeStdDev(this List<double> series) {
      double avg = series.Average();
      double sumOfSquaresOfDifferences = series.Sum(val => (val - avg) * (val - avg));
      return Math.Sqrt(sumOfSquaresOfDifferences / (series.Count - 1));
    }
  }
}
