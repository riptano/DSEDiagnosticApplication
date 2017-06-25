using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DSEDiagnosticFileParser
{

    [System.Diagnostics.DebuggerNonUserCode]
    public sealed class ExceptionEventArgs : System.EventArgs
    {
        #region Properties

        public System.Exception Exception { get; }

        public System.Threading.CancellationTokenSource CancellationTokenSource { get; }

        public object[] AssociatedObjects { get; }

        public int ThreadId { get;  }

        #endregion //end of Properties

        public delegate void EventHandler(object sender, ExceptionEventArgs eventArgs);

        #region Constructors

        private ExceptionEventArgs()
        {
        }

        public ExceptionEventArgs(System.Exception exception,
                                    System.Threading.CancellationTokenSource cancellationTokenSource,
                                    object[] associatedObjects,
                                    int threadId)

        {
            this.Exception = exception;
            this.AssociatedObjects = associatedObjects;
            this.CancellationTokenSource = cancellationTokenSource;
            this.ThreadId = threadId;
        }


        #endregion //end of Constructors

        #region Invoke Event Static Methods

        public static bool InvokeEvent(DiagnosticFile sender,
                                        System.Exception exception,
                                        System.Threading.CancellationTokenSource cancellationTokenSource,
                                        object[] associatedObjects,
                                        EventHandler invokeDelegate)
        {
            if (invokeDelegate != null)
            {
                invokeDelegate(sender, new ExceptionEventArgs(exception,
                                                                cancellationTokenSource,
                                                                associatedObjects,
                                                                System.Threading.Thread.CurrentThread.ManagedThreadId));
                return true;
            }

            return false;
        }

        public static bool InvokeEvent(string sender,
                                        System.Exception exception,
                                        System.Threading.CancellationTokenSource cancellationTokenSource,
                                        object[] associatedObjects,
                                        EventHandler invokeDelegate)
        {
            if (invokeDelegate != null)
            {
                invokeDelegate(sender, new ExceptionEventArgs(exception,
                                                                cancellationTokenSource,
                                                                associatedObjects,
                                                                System.Threading.Thread.CurrentThread.ManagedThreadId));
                return true;
            }

            return false;
        }

        public static bool InvokeEvent(FileMapper sender,
                                        System.Exception exception,
                                        System.Threading.CancellationTokenSource cancellationTokenSource,
                                        object[] associatedObjects,
                                        EventHandler invokeDelegate)
        {
            if (invokeDelegate != null)
            {
                invokeDelegate(sender, new ExceptionEventArgs(exception,
                                                                cancellationTokenSource,
                                                                associatedObjects,
                                                                System.Threading.Thread.CurrentThread.ManagedThreadId));
                return true;
            }

            return false;
        }



        public static bool HasAssignedEvent(EventHandler invokeDelegate)
        {
            return invokeDelegate != null;
        }
        #endregion //end of Invoke Event Static Methods
    } //end of Event Argument Class ExceptionEventArgs
}
