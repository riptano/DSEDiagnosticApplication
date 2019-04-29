using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticInsightsES
{
    public sealed class ESException : Exception
    {
        public ESException() { }

        public ESException(Type queryClass, Exception innerException, Elasticsearch.Net.ServerError serverError, string message = null)
            : base(message == null && serverError != null
                        ? string.Format("Query Class: {0}, Error Type: {1} Error Status: {2} Error Reason: {3}",
                                            queryClass.Name,
                                            serverError.Error.Type,
                                            serverError.Status,
                                            serverError.Error.Reason)
                      : message,
                  innerException)
        {
            this.ServerError = serverError;
        }

        public sealed class ExceptionEventArgs : EventArgs
        {

            private ExceptionEventArgs()
            { }

            public ExceptionEventArgs(System.Exception exception,
                                        bool throwException = true,
                                        bool logException = false,
                                        bool treatAsWarning = false,
                                        string additionalInformation = null,
                                        object additionalData = null)
            {
                this.Exception = exception;
                this.LogException = logException;
                this.ThrowException = throwException;
                this.AdditionalInfo = additionalInformation;
                this.AdditionalData = additionalData;
                this.TreatAsWarning = treatAsWarning;
            }

            public System.Exception Exception
            {
                get;
                set;
            }

            public bool ThrowException
            {
                get;
                set;
            }

            public string AdditionalInfo
            {
                get;
                set;
            }
            
            public bool LogException
            {
                get;
                set;
            }

            /// <summary>
            /// Logs exception as a warning (only if LogException is true)
            /// </summary>
            public bool TreatAsWarning
            {
                get;
                set;
            }

            public object AdditionalData
            {
                get;
                set;
            }

            public delegate void Delegate(Nest.IResponse sender, ExceptionEventArgs eventArgs);
        }

        public Elasticsearch.Net.ServerError ServerError { get; }
    }
}
