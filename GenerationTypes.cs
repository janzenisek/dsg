using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using uPLibrary.Networking.M2Mqtt;

namespace DST.GeneratorNet {

  public struct StreamingJob {
    public MqttClient Client { get; set; }
    public CancellationToken Token { get; set; }
  }

  public class DoubleSeries {
    public List<double> X { get; set; }
    public List<double> E { get; set; }

    public DoubleSeries() {
      X = new List<double>();
      E = new List<double>();
    }
  }

  public struct Message {
    public string id;
    public string predecessorSource;
    public string group;
    public int rank;
    public string title;
    public string timestamp;
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
