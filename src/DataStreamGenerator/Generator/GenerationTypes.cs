using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;

namespace DSG {

  public struct StreamingJob {
    public IManagedMqttClient Client { get; set; }
    public CancellationToken Token { get; set; }
  }

  public class DoubleSeries {
    public List<double> X { get; set; } // generated values
    public List<double> E { get; set; } // error terms (ARMA, ARIMA)
    public List<double> D { get; set; } // derivatives (ARIMA)
    public List<double> S { get; set; } // stored values (XF, XG)

    public DoubleSeries(double init) {
      X = new List<double>();
      X.Add(init);
      E = new List<double>();
      D = new List<double>();
      S = new List<double>();
    }
  }

  public struct Message {
    public string id;
    public string predecessorSource;
    public string group;
    public int rank;
    public string title;
    public string timestamp;
    public string systemTimestamp;
    public double value;
  }

  public struct SlimMessage {
    public string id;
    public double value;
  }

  public abstract class ConstrainedSeries<T> {
    public SortedList<DateTime, T> Buffer { get; set; }
    public int BufferSize { get; set; }

    protected ConstrainedSeries(int bufferSize) {
      BufferSize = bufferSize;
      Buffer = new SortedList<DateTime, T>();
    }

    public abstract void Push(DateTime timestamp, T item);
  }

  public class TimeConstrainedSeries<T> : ConstrainedSeries<T> {

    public TimeConstrainedSeries(int bufferSize) : base(bufferSize) {
    }

    public override void Push(DateTime timestamp, T item) {
      Buffer.Add(timestamp, item);

      var minDate = Buffer.Last().Key.AddMilliseconds(-BufferSize);
      var removeCandidates = Buffer.Select(x => x.Key).Where(x => x < minDate).ToList();
      foreach (var r in removeCandidates)
        Buffer.Remove(r);
    }
  }

  public class CapacityConstrainedSeries<T> : ConstrainedSeries<T> {

    public CapacityConstrainedSeries(int bufferSize) : base(bufferSize) {
    }

    public override void Push(DateTime timestamp, T item) {
      Buffer.Add(timestamp, item);
      if (Buffer.Count > BufferSize) Buffer.RemoveAt(0);
    }
  }
}
