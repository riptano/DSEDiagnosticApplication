using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticConsoleApplication
{
    public sealed class ConsoleDisplay
    {
        static DateTime ConsoleStartTime = DateTime.Now;
        public static readonly string ConsoleRunningTimerTag = "ConsoleRunningTimer";

        static readonly ConsoleWriter consoleWriter = new ConsoleWriter();
        static readonly Timer timer = new Timer(TimerCallback, consoleWriter, Timeout.Infinite, Timeout.Infinite);
        static readonly List<ConsoleDisplay> ConsoleDisplays = new List<ConsoleDisplay>();
        static bool SpinnerDefault = true;
        static bool EnableWriters = true;

        private long _counter = 0;
        private readonly Common.Patterns.Collections.ThreadSafe.List<string> _taskItems = new Common.Patterns.Collections.ThreadSafe.List<string>();

        public static void DisableAllConsoleWriter()
        {
            timer.Change(Timeout.Infinite, Timeout.Infinite);
            SpinnerDefault = false;
            EnableWriters = false;
        }

        public ConsoleDisplay(string displayString, int maxLines = 2, bool enableSpinner = true)
        {
            this.LineFormat = displayString;
            this.Spinner = enableSpinner && SpinnerDefault;
            if (EnableWriters)
            {
                consoleWriter.ReserveRwWriteConsoleSpace(ConsoleDisplays.Count.ToString(), maxLines, -1);
            }
            ConsoleDisplays.Add(this);
        }

        public string LineFormat
        {
            get;
            set;
        }

        public long Counter
        {
            get { return Common.Patterns.Threading.LockFree.Read(ref this._counter); }
            set { Common.Patterns.Threading.LockFree.Update(ref this._counter, value); }
        }

        public bool Spinner
        {
            get;
            set;
        }

        public long Increment(string taskItem = null)
        {
            if (!string.IsNullOrEmpty(taskItem))
            {
                if (EnableWriters)
                {
                    if (!this._taskItems.TryAdd(taskItem))
                    {
                        return -1;
                    }
                }
                else
                {
                    var lineStr = this.Line(taskItem, 1);

                    if(lineStr.Contains('{'))
                    {
                        lineStr = lineStr.Replace('{', '[').Replace('}', ']');
                    }

                    consoleWriter.WriteLine(lineStr);
                }
            }

            return Interlocked.Increment(ref this._counter);
        }

        public long Increment(string preFix, string msg, int maxMsgLength = 15)
        {
            return Increment(string.Format("{0} {1}",
                                            preFix,
                                            maxMsgLength <= 0 || string.IsNullOrEmpty(msg) || msg.Length < maxMsgLength
                                                ? msg
                                                : msg.Substring(0, maxMsgLength) + "..."));
        }

        public long Increment(Common.IFilePath filePath)
        {
            return this.Increment(filePath?.FileName);
        }

        public long Decrement(string taskItem = null)
        {
            if (!string.IsNullOrEmpty(taskItem))
            {
                if (EnableWriters)
                {
                    if (!this._taskItems.Remove(taskItem))
                    {
                        return -1;
                    }
                    else
                    {
                        var lineStr = this.Line(taskItem, 0);

                        if (lineStr.Contains('{'))
                        {
                            lineStr = lineStr.Replace('{', '[').Replace('}', ']');
                        }
                        consoleWriter.WriteLine(lineStr);
                    }
                }
            }

            var value = Interlocked.Decrement(ref this._counter);

            return value < 0 ? Common.Patterns.Threading.LockFree.Exchange(ref this._counter, 0) : value;
        }

        public long Decrement(Common.IFilePath filePath)
        {
            return this.Decrement(filePath?.FileName);
        }

        public bool TaskEnd(string taskItem)
        {
            return this._taskItems.Remove(taskItem);
        }

        public bool TaskEnd(Common.IFilePath filePath)
        {
            return this.TaskEnd(filePath?.FileName);
        }

        public int Pending
        {
            get { return this._taskItems.Count; }
        }

        public string Line(string item, int itemCount)
        {
            try
            {
                return string.Format(LineFormat,
                                        Common.Patterns.Threading.LockFree.Read(ref this._counter),
                                        itemCount,
                                        item);
            }
            catch
            { }
            return string.Empty;
        }

        public string Line(int pos, bool retry = true)
        {
            try
            {
                var taskItem = this._taskItems.Count == 0
                                       ? string.Empty
                                       : (pos >= 0 && pos < this._taskItems.Count) ? this._taskItems.ElementAtOrDefault(pos) : this._taskItems.LastOrDefault();

                return string.Format(LineFormat,
                                        Common.Patterns.Threading.LockFree.Read(ref this._counter),
                                        this._taskItems.Count,
                                        taskItem).Replace("{", "{{").Replace("}", "}}");
            }
            catch (System.ArgumentOutOfRangeException)
            {
                if (retry)
                {
                    return this.Line(-1, false);
                }
            }
            return string.Empty;
        }

        int _runningCnt = 0;
        int _displayLine = 0;

        public string Line()
        {
            var currCount = this._taskItems.Count;

            if (currCount > 1 && currCount == this._runningCnt)
            {
                var displayLine = this._displayLine++;
                if (displayLine >= currCount)
                {
                    this._displayLine = 0;
                }
                return this.Line(displayLine);
            }

            this._runningCnt = currCount;

            return this.Line(-1);
        }

        public void Terminate()
        {
            this.Terminated = true;
            this._taskItems.Clear();
        }

        public bool Terminated
        {
            get;
            private set;
        }

        public static void Start()
        {
            if (EnableWriters)
            {
                timer.Change(100, Timeout.Infinite);
                StopTimer = false;

                ConsoleWriter.ReWriteInfo rewriteInfo;
                if (!consoleWriter.TryGetReWriteInformation(ConsoleRunningTimerTag, out rewriteInfo))
                {
                    consoleWriter.ReserveRwWriteConsoleSpace(ConsoleRunningTimerTag, 2, -1);
                }
            }
        }

        public static void End()
        {
            timer.Change(Timeout.Infinite, Timeout.Infinite);
            StopTimer = true;
            consoleWriter.ClearSpinner();
            //consoleWriter.DisableSpinner();
        }

        public static ConsoleWriter Console { get { return consoleWriter; } }
        static long TimerEntry = 0;
        static bool StopTimer = false;

        static void TimerCallback(object state)
        {
            var consoleWriter = (ConsoleWriter)state;

            if (StopTimer)
            {
                timer.Change(Timeout.Infinite, Timeout.Infinite);
                return;
            }

            //timer.Change(Timeout.Infinite, Timeout.Infinite);
            consoleWriter.ReWrite(ConsoleRunningTimerTag, @"Running Time: {0:d\.hh\:mm\:ss}", DateTime.Now - ConsoleStartTime);

            try
            {
                if (System.Threading.Interlocked.Read(ref TimerEntry) > 10)
                {
                    return;
                }

                System.Threading.Interlocked.Increment(ref TimerEntry);

                for (int nIndex = 0; nIndex < ConsoleDisplays.Count; ++nIndex)
                {
                    if (ConsoleDisplays[nIndex].Terminated)
                    {
                        consoleWriter.ReWrite(nIndex.ToString(), ConsoleDisplays[nIndex].Line());
                    }
                    else
                    {
                        if (ConsoleDisplays[nIndex].Spinner && ConsoleDisplays[nIndex].Counter != 0)
                        {
                            consoleWriter.ReWriteAndTurn(nIndex.ToString(), ConsoleDisplays[nIndex].Line());
                        }
                        else
                        {
                            consoleWriter.ReWrite(nIndex.ToString(), ConsoleDisplays[nIndex].Line());
                        }
                    }
                }
            }
            finally
            {
                System.Threading.Interlocked.Decrement(ref TimerEntry);
                timer.Change(1000, Timeout.Infinite);
            }
        }
    }
}
