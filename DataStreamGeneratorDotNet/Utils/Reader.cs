/* 
 * DataStreamGenerator
 * Author: Jan Zenisek
 * Date: 05/2018
 */

using System;
using System.Threading;

namespace DSG.Utils {
  // cf. https://stackoverflow.com/questions/57615/how-to-add-a-timeout-to-console-readline/9016896
  public class Reader {
    private static Thread inputThread;
    private static AutoResetEvent getInput, gotInput;
    private static string input;

    static Reader() {
      getInput = new AutoResetEvent(false);
      gotInput = new AutoResetEvent(false);
      inputThread = new Thread(reader);
      inputThread.IsBackground = true;
      inputThread.Start();
    }

    private static void reader() {
      while (true) {
        getInput.WaitOne();
        input = Console.ReadLine();
        gotInput.Set();
      }
    }

    public static string ReadLine(int timeOutMillisecs = Timeout.Infinite) {
      getInput.Set();
      bool success = gotInput.WaitOne(timeOutMillisecs);
      if (success)
        return input;
      else
        return null;
    }
  }
}
