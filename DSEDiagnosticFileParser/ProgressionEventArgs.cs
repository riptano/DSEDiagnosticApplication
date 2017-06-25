using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticFileParser
{
    [System.Diagnostics.DebuggerNonUserCode]
    public sealed class ProgressionEventArgs : System.EventArgs
    {
        private static readonly Common.Patterns.QueueProcessorFST<ProgressionEventArgs> EventQueue = new Common.Patterns.QueueProcessorFST<ProgressionEventArgs>(false);

        [Flags]
        public enum Categories
        {
            Start = 0x0001,
            End = 0x0002,
            Cancel = 0x0004,

            DiagnosticFile = 0x0100,
            FileMapper = 0x0200,
            File = 0x0400,

            Collection = 0x10000,
            Process = 0x20000
        }

        #region properties

        public Categories Category { get; }

        public string StepName { get; }
        public int ThreadId { get; }
        public uint? Step { get; }
        public uint? NbrSteps { get; }
        public DateTime TimeStamp { get; }
        public string MessageString { get; }
        public object[] MessageArgs { get; }
        private object Sender { get; }
        #endregion

        public delegate void EventHandler(object sender, ProgressionEventArgs eventArgs);

        private static EventHandler ProgressionDelegate;

        public static event EventHandler OnProgression
        {
            // Explicit event definition with accessor methods
            add
            {
                ProgressionDelegate = (EventHandler)Delegate.Combine(ProgressionDelegate, value);

                if(ProgressionDelegate != null && !EventQueue.IsRunning)
                {
                    EventQueue.StartQueue(false);
                }
            }
            remove
            {
                ProgressionDelegate = (EventHandler)Delegate.Remove(ProgressionDelegate, value);

                if (ProgressionDelegate == null && EventQueue.IsRunning)
                {
                    EventQueue.StopQueue();
                }
            }
        }

        #region Constructor

        static ProgressionEventArgs()
        {
            EventQueue.OnProcessMessageEvent += EventQueue_OnProcessMessageEvent;
            EventQueue.OnExceptionEvent += EventQueue_OnExceptionEvent;
        }

        public ProgressionEventArgs(Categories category,
                                    string stepName,
                                    int threadId,
                                    uint? step,
                                    uint? nbrSteps,
                                    DateTime timeStamp,
                                    string messageString,
                                    object[] messageArgs,
                                    object sender)
        {
            this.Category = category;
            this.StepName = stepName;
            this.Step = step;
            this.NbrSteps = nbrSteps;
            this.TimeStamp = timeStamp;
            this.MessageString = messageString;
            this.MessageArgs = messageArgs;
            this.Sender = sender;
        }

        #endregion

        public string Message()
        {
            if (this.MessageArgs == null) return this.MessageString;
            return string.Format(this.MessageString, this.MessageArgs);
        }

        #region queue
        public static bool StartQueue()
        {
            return EventQueue.StartQueue(true);
        }

        public static bool StopQueue()
        {
            return EventQueue.StopQueue();
        }

        public static bool SuspendQueue()
        {
            return EventQueue.Suspend();
        }

        public static bool ResumeQueue()
        {
            return EventQueue.Resume();
        }

        public static long QueueCount()
        {
            return EventQueue.Count;
        }

        private static void EventQueue_OnProcessMessageEvent(Common.Patterns.QueueProcessorFST<ProgressionEventArgs> sender, Common.Patterns.QueueProcessorFST<ProgressionEventArgs>.MessageEventArgs processMessageArgs)
        {
            var invokeDelegate = ProgressionDelegate;

            if (invokeDelegate != null)
            {
                invokeDelegate(processMessageArgs.ProcessedMessage.Sender, processMessageArgs.ProcessedMessage);
            }
        }

        private static void EventQueue_OnExceptionEvent(Common.Patterns.QueueProcessorFST<ProgressionEventArgs> sender, Common.Patterns.QueueProcessorFST<ProgressionEventArgs>.ExceptionEventArgs exceptionEventArgs)
        {
            DSEDiagnosticLogger.Logger.Instance.Error("Exception occurred in Progression Event Queue", exceptionEventArgs.Exception);
            exceptionEventArgs.ThrowException = false;
        }


        #endregion

        #region Invoke Event Static Methods

        public static bool InvokeEvent(DiagnosticFile sender,
                                        Categories category,
                                        string stepName,
                                        int? threadId = null,
                                        uint? step = null,
                                        uint? nbrSteps = null,
                                        DateTime? timeStamp = null,
                                        string messageString = null,
                                        params object[] messageArgs)
        {
            if (ProgressionDelegate != null)
            {
                category |= Categories.DiagnosticFile;

                EventQueue.Enqueue(new ProgressionEventArgs(category,
                                                                stepName,
                                                                threadId.HasValue ? threadId.Value : System.Threading.Thread.CurrentThread.ManagedThreadId,
                                                                step,
                                                                nbrSteps,
                                                                timeStamp.HasValue ? timeStamp.Value : DateTime.Now,
                                                                messageString,
                                                                messageArgs,
                                                                sender));
                return true;
            }

            return false;
        }

        public static bool InvokeEvent(IEnumerable<DiagnosticFile> sender,
                                        Categories category,
                                        string stepName,
                                        int? threadId = null,
                                        uint? step = null,
                                        uint? nbrSteps = null,
                                        DateTime? timeStamp = null,
                                        string messageString = null,
                                        params object[] messageArgs)
        {
            if (ProgressionDelegate != null)
            {
                category |= Categories.DiagnosticFile;

                EventQueue.Enqueue(new ProgressionEventArgs(category,
                                                                stepName,
                                                                threadId.HasValue ? threadId.Value : System.Threading.Thread.CurrentThread.ManagedThreadId,
                                                                step,
                                                                nbrSteps,
                                                                timeStamp.HasValue ? timeStamp.Value : DateTime.Now,
                                                                messageString,
                                                                messageArgs,
                                                                sender));
                return true;
            }

            return false;
        }

        public static bool InvokeEvent(FileMapper sender,
                                        Categories category,
                                        string stepName,
                                        int? threadId = null,
                                        uint? step = null,
                                        uint? nbrSteps = null,
                                        DateTime? timeStamp = null,
                                        string messageString = null,
                                        params object[] messageArgs)
        {
            if (ProgressionDelegate != null)
            {
                category |= Categories.FileMapper;

                EventQueue.Enqueue(new ProgressionEventArgs(category,
                                                                stepName,
                                                                threadId.HasValue ? threadId.Value : System.Threading.Thread.CurrentThread.ManagedThreadId,
                                                                step,
                                                                nbrSteps,
                                                                timeStamp.HasValue ? timeStamp.Value : DateTime.Now,
                                                                messageString,
                                                                messageArgs,
                                                                sender));
                return true;
            }

            return false;
        }

        public static bool InvokeEvent(IEnumerable<FileMapper> sender,
                                        Categories category,
                                        string stepName,
                                        int? threadId = null,
                                        uint? step = null,
                                        uint? nbrSteps = null,
                                        DateTime? timeStamp = null,
                                        string messageString = null,
                                        params object[] messageArgs)
        {
            if (ProgressionDelegate != null)
            {
                category |= Categories.FileMapper;

                EventQueue.Enqueue(new ProgressionEventArgs(category,
                                                                stepName,
                                                                threadId.HasValue ? threadId.Value : System.Threading.Thread.CurrentThread.ManagedThreadId,
                                                                step,
                                                                nbrSteps,
                                                                timeStamp.HasValue ? timeStamp.Value : DateTime.Now,
                                                                messageString,
                                                                messageArgs,
                                                                sender));
                return true;
            }

            return false;
        }

        public static bool InvokeEvent(Common.IFilePath sender,
                                        Categories category,
                                        string stepName,
                                        int? threadId = null,
                                        uint? step = null,
                                        uint? nbrSteps = null,
                                        DateTime? timeStamp = null,
                                        string messageString = null,
                                        params object[] messageArgs)
        {
            if (ProgressionDelegate != null)
            {
                category |= Categories.File;

                EventQueue.Enqueue(new ProgressionEventArgs(category,
                                                                stepName,
                                                                threadId.HasValue ? threadId.Value : System.Threading.Thread.CurrentThread.ManagedThreadId,
                                                                step,
                                                                nbrSteps,
                                                                timeStamp.HasValue ? timeStamp.Value : DateTime.Now,
                                                                messageString,
                                                                messageArgs,
                                                                sender));
                return true;
            }

            return false;
        }

        public static bool HasAssignedEvent()
        {
            return ProgressionDelegate != null;
        }

        #endregion //end of Invoke Event Static Methods

    }
}
