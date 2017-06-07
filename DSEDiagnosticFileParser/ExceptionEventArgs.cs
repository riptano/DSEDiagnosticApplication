using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DSEDiagnosticFileParser
{

    [System.Diagnostics.DebuggerNonUserCode]
    public class ExceptionEventArgs : System.EventArgs
    {
        #region Properties

        public System.Exception Exception { get; protected set; }

        public System.Threading.CancellationTokenSource CancellationTokenSource { get; protected set; }

        public object[] AssociatedObjects { get; protected set; }


        #endregion //end of Properties

        public delegate void EventHandler(object sender, ExceptionEventArgs eventArgs);

        #region Constructors

        protected ExceptionEventArgs()
        {
        }

        public ExceptionEventArgs(System.Exception exception,
                                    System.Threading.CancellationTokenSource cancellationTokenSource,
                                    object[] associatedObjects)

        {
            this.Exception = exception;
            this.AssociatedObjects = associatedObjects;
            this.CancellationTokenSource = cancellationTokenSource;
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
                                                                associatedObjects));
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
                                                                associatedObjects));
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
                                                                associatedObjects));
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
