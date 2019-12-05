using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;

namespace DSEDiagnosticLogger
{
    public sealed class Logger : IDisposable
    {
        private static readonly Lazy<Logger> LazyInstance = new Lazy<Logger>(() => new Logger());
        private log4net.ILog LazyLog4NetInstance = null;

        public struct Log4NetInfo
        {
            public log4net.Core.LoggingEvent[] LoggingEvents;
        }

        static Logger()
        {
        }

        internal Logger()
        {
            this.LazyLog4NetInstance = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        }

        #region Public Properties

        public static Logger Instance { get { return LazyInstance.Value; } }

        public log4net.ILog Log4NetInstance
        {
            get { return this.LazyLog4NetInstance; }
            set { this.LazyLog4NetInstance = value; }
        }

        #endregion

        #region Events

        /// <summary>
        ///     Called before the event is logged. If the AllowLogging property is set to false the event is not logged. The default is to log (true).
        /// </summary>
        public event LoggingEventArgs.EventHandler OnLoggingEvent;

        internal static bool InvokeEvent(log4net.Core.LoggingEvent[] loggingEvents)
        {
            var invokeDelegate = Instance.OnLoggingEvent;

            if (invokeDelegate != null)
            {
                var eventArgs = new LoggingEventArgs(new Log4NetInfo() { LoggingEvents = loggingEvents });

                invokeDelegate(Instance, eventArgs);
                return eventArgs.AllowLogging;
            }

            return true;
        }

        #endregion

        #region Log4Net Methods

        public string GetSetEnvVarLoggerFile(bool setEnvValue = true)
        {
            var rootAppender = Log4NetInstance.Logger.Repository.GetAppenders()
                                          .OfType<log4net.Appender.FileAppender>()
                                         .FirstOrDefault();

            string filename = rootAppender != null ? rootAppender.File : string.Empty;

            if(setEnvValue)
                Environment.SetEnvironmentVariable("DSEDiagnosticLoggerFilePath", filename, EnvironmentVariableTarget.User);

            return filename;
        }

        public void SetDebugLevel()
        {
            if(((log4net.Repository.Hierarchy.Hierarchy)Log4NetInstance.Logger.Repository).Root.Level != log4net.Core.Level.Debug)
            {
                ((log4net.Repository.Hierarchy.Hierarchy)Log4NetInstance.Logger.Repository).Root.Level = log4net.Core.Level.Debug;
                ((log4net.Repository.Hierarchy.Hierarchy)Log4NetInstance.Logger.Repository).RaiseConfigurationChanged(EventArgs.Empty);
            }            
        }

        //
        // Summary:
        //     Checks if this logger is enabled for the log4net.Core.Level.Debug level.
        //
        // Remarks:
        //     This function is intended to lessen the computational cost of disabled log debug
        //     statements.
        //     For some ILog interface log, when you write:
        //     log.Debug("This is entry number: " + i );
        //     You incur the cost constructing the message, string construction and concatenation
        //     in this case, regardless of whether the message is logged or not.
        //     If you are worried about speed (who isn't), then you should write:
        //     if (log.IsDebugEnabled) { log.Debug("This is entry number: " + i ); }
        //     This way you will not incur the cost of parameter construction if debugging is
        //     disabled for log. On the other hand, if the log is debug enabled, you will incur
        //     the cost of evaluating whether the logger is debug enabled twice. Once in log4net.ILog.IsDebugEnabled
        //     and once in the Debug(object). This is an insignificant overhead since evaluating
        //     a logger takes about 1% of the time it takes to actually log. This is the preferred
        //     style of logging.
        //     Alternatively if your logger is available statically then the is debug enabled
        //     state can be stored in a static variable like this:
        //     private static readonly bool isDebugEnabled = log.IsDebugEnabled;
        //     Then when you come to log you can write:
        //     if (isDebugEnabled) { log.Debug("This is entry number: " + i ); }
        //     This way the debug enabled state is only queried once when the class is loaded.
        //     Using a private static readonly variable is the most efficient because it is
        //     a run time constant and can be heavily optimized by the JIT compiler.
        //     Of course if you use a static readonly variable to hold the enabled state of
        //     the logger then you cannot change the enabled state at runtime to vary the logging
        //     that is produced. You have to decide if you need absolute speed or runtime flexibility.
        public bool IsDebugEnabled { get { return Log4NetInstance.IsDebugEnabled; } }
        //
        // Summary:
        //     Checks if this logger is enabled for the log4net.Core.Level.Error level.
        //
        // Remarks:
        //     For more information see log4net.ILog.IsDebugEnabled.
        public bool IsErrorEnabled { get { return Log4NetInstance.IsErrorEnabled; } }
        //
        // Summary:
        //     Checks if this logger is enabled for the log4net.Core.Level.Fatal level.
        //
        // Remarks:
        //     For more information see log4net.ILog.IsDebugEnabled.
        public bool IsFatalEnabled { get { return Log4NetInstance.IsFatalEnabled; } }
        //
        // Summary:
        //     Checks if this logger is enabled for the log4net.Core.Level.Info level.
        //
        // Remarks:
        //     For more information see log4net.ILog.IsDebugEnabled.
        public bool IsInfoEnabled { get { return Log4NetInstance.IsInfoEnabled; } }
        //
        // Summary:
        //     Checks if this logger is enabled for the log4net.Core.Level.Warn level.
        //
        // Remarks:
        //     For more information see log4net.ILog.IsDebugEnabled.
        public bool IsWarnEnabled { get { return Log4NetInstance.IsWarnEnabled; } }


        //
        // Summary:
        //     Log a message object with the log4net.Core.Level.Debug level.
        //
        // Parameters:
        //   message:
        //     The message object to log.
        //
        // Remarks:
        //     This method first checks if this logger is DEBUG enabled by comparing the level
        //     of this logger with the log4net.Core.Level.Debug level. If this logger is DEBUG
        //     enabled, then it converts the message object (passed as parameter) to a string
        //     by invoking the appropriate log4net.ObjectRenderer.IObjectRenderer. It then proceeds
        //     to call all the registered appenders in this logger and also higher in the hierarchy
        //     depending on the value of the additivity flag.
        //     WARNING Note that passing an System.Exception to this method will print the name
        //     of the System.Exception but no stack trace. To print a stack trace use the Debug(object,Exception)
        //     form instead.
        public void Debug(object message)
        {
            Log4NetInstance.Debug(message);
        }
        //
        // Summary:
        //     Log a message object with the log4net.Core.Level.Debug level including the stack
        //     trace of the System.Exception passed as a parameter.
        //
        // Parameters:
        //   message:
        //     The message object to log.
        //
        //   exception:
        //     The exception to log, including its stack trace.
        //
        // Remarks:
        //     See the Debug(object) form for more detailed information.
        public void Debug(object message, Exception exception)
        {
            Log4NetInstance.Debug(message, exception);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Debug level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   args:
        //     An Object array containing zero or more objects to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Debug(object,Exception) methods instead.
        public void DebugFormat(string format, params object[] args)
        {
            Log4NetInstance.DebugFormat(format, args);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Debug level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Debug(object,Exception) methods instead.
        public void DebugFormat(string format, object arg0)
        {
            Log4NetInstance.DebugFormat(format, arg0);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Debug level.
        //
        // Parameters:
        //   provider:
        //     An System.IFormatProvider that supplies culture-specific formatting information
        //
        //   format:
        //     A String containing zero or more format items
        //
        //   args:
        //     An Object array containing zero or more objects to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Debug(object,Exception) methods instead.
        public void DebugFormat(IFormatProvider provider, string format, params object[] args)
        {
            Log4NetInstance.DebugFormat(provider, format, args);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Debug level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        //   arg1:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Debug(object,Exception) methods instead.
        public void DebugFormat(string format, object arg0, object arg1)
        {
            Log4NetInstance.DebugFormat(format, arg0, arg1);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Debug level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        //   arg1:
        //     An Object to format
        //
        //   arg2:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Debug(object,Exception) methods instead.
        public void DebugFormat(string format, object arg0, object arg1, object arg2)
        {
            Log4NetInstance.DebugFormat(format, arg0, arg1, arg2);
        }
        //
        // Summary:
        //     Logs a message object with the log4net.Core.Level.Error level.
        //
        // Parameters:
        //   message:
        //     The message object to log.
        //
        // Remarks:
        //     This method first checks if this logger is ERROR enabled by comparing the level
        //     of this logger with the log4net.Core.Level.Error level. If this logger is ERROR
        //     enabled, then it converts the message object (passed as parameter) to a string
        //     by invoking the appropriate log4net.ObjectRenderer.IObjectRenderer. It then proceeds
        //     to call all the registered appenders in this logger and also higher in the hierarchy
        //     depending on the value of the additivity flag.
        //     WARNING Note that passing an System.Exception to this method will print the name
        //     of the System.Exception but no stack trace. To print a stack trace use the Error(object,Exception)
        //     form instead.
        public void Error(object message)
        {
            Log4NetInstance.Error(message);
        }
        //
        // Summary:
        //     Log a message object with the log4net.Core.Level.Error level including the stack
        //     trace of the System.Exception passed as a parameter.
        //
        // Parameters:
        //   message:
        //     The message object to log.
        //
        //   exception:
        //     The exception to log, including its stack trace.
        //
        // Remarks:
        //     See the Error(object) form for more detailed information.
        public void Error(object message, Exception exception)
        {
            Log4NetInstance.Error(message, exception);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Error level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   args:
        //     An Object array containing zero or more objects to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Error(object) methods instead.
        public void ErrorFormat(string format, params object[] args)
        {
            Log4NetInstance.ErrorFormat(format, args);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Error level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Error(object,Exception) methods instead.
        public void ErrorFormat(string format, object arg0)
        {
            Log4NetInstance.ErrorFormat(format, arg0);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Error level.
        //
        // Parameters:
        //   provider:
        //     An System.IFormatProvider that supplies culture-specific formatting information
        //
        //   format:
        //     A String containing zero or more format items
        //
        //   args:
        //     An Object array containing zero or more objects to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Error(object) methods instead.
        public void ErrorFormat(IFormatProvider provider, string format, params object[] args)
        {
            Log4NetInstance.ErrorFormat(provider, format, args);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Error level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        //   arg1:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Error(object,Exception) methods instead.
        public void ErrorFormat(string format, object arg0, object arg1)
        {
            Log4NetInstance.ErrorFormat(format, arg0, arg1);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Error level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        //   arg1:
        //     An Object to format
        //
        //   arg2:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Error(object,Exception) methods instead.
        public void ErrorFormat(string format, object arg0, object arg1, object arg2)
        {
            Log4NetInstance.ErrorFormat(format, arg0, arg1, arg2);
        }
        //
        // Summary:
        //     Log a message object with the log4net.Core.Level.Fatal level.
        //
        // Parameters:
        //   message:
        //     The message object to log.
        //
        // Remarks:
        //     This method first checks if this logger is FATAL enabled by comparing the level
        //     of this logger with the log4net.Core.Level.Fatal level. If this logger is FATAL
        //     enabled, then it converts the message object (passed as parameter) to a string
        //     by invoking the appropriate log4net.ObjectRenderer.IObjectRenderer. It then proceeds
        //     to call all the registered appenders in this logger and also higher in the hierarchy
        //     depending on the value of the additivity flag.
        //     WARNING Note that passing an System.Exception to this method will print the name
        //     of the System.Exception but no stack trace. To print a stack trace use the Fatal(object,Exception)
        //     form instead.
        public void Fatal(object message)
        {
            Log4NetInstance.Fatal(message);
        }
        //
        // Summary:
        //     Log a message object with the log4net.Core.Level.Fatal level including the stack
        //     trace of the System.Exception passed as a parameter.
        //
        // Parameters:
        //   message:
        //     The message object to log.
        //
        //   exception:
        //     The exception to log, including its stack trace.
        //
        // Remarks:
        //     See the Fatal(object) form for more detailed information.
        public void Fatal(object message, Exception exception)
        {
            Log4NetInstance.Fatal(message, exception);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Fatal level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Fatal(object,Exception) methods instead.
        public void FatalFormat(string format, object arg0)
        {
            Log4NetInstance.FatalFormat(format, arg0);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Fatal level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   args:
        //     An Object array containing zero or more objects to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Fatal(object) methods instead.
        public void FatalFormat(string format, params object[] args)
        {
            Log4NetInstance.FatalFormat(format, args);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Fatal level.
        //
        // Parameters:
        //   provider:
        //     An System.IFormatProvider that supplies culture-specific formatting information
        //
        //   format:
        //     A String containing zero or more format items
        //
        //   args:
        //     An Object array containing zero or more objects to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Fatal(object) methods instead.
        public void FatalFormat(IFormatProvider provider, string format, params object[] args)
        {
            Log4NetInstance.FatalFormat(provider, format, args);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Fatal level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        //   arg1:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Fatal(object,Exception) methods instead.
        public void FatalFormat(string format, object arg0, object arg1)
        {
            Log4NetInstance.FatalFormat(format, arg0, arg1);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Fatal level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        //   arg1:
        //     An Object to format
        //
        //   arg2:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Fatal(object,Exception) methods instead.
        public void FatalFormat(string format, object arg0, object arg1, object arg2)
        {
            Log4NetInstance.FatalFormat(format, arg0, arg1, arg2);
        }
        //
        // Summary:
        //     Logs a message object with the log4net.Core.Level.Info level.
        //
        // Parameters:
        //   message:
        //     The message object to log.
        //
        // Remarks:
        //     This method first checks if this logger is INFO enabled by comparing the level
        //     of this logger with the log4net.Core.Level.Info level. If this logger is INFO
        //     enabled, then it converts the message object (passed as parameter) to a string
        //     by invoking the appropriate log4net.ObjectRenderer.IObjectRenderer. It then proceeds
        //     to call all the registered appenders in this logger and also higher in the hierarchy
        //     depending on the value of the additivity flag.
        //     WARNING Note that passing an System.Exception to this method will print the name
        //     of the System.Exception but no stack trace. To print a stack trace use the Info(object,Exception)
        //     form instead.
        public void Info(object message)
        {
            Log4NetInstance.Info(message);
        }
        //
        // Summary:
        //     Logs a message object with the INFO level including the stack trace of the System.Exception
        //     passed as a parameter.
        //
        // Parameters:
        //   message:
        //     The message object to log.
        //
        //   exception:
        //     The exception to log, including its stack trace.
        //
        // Remarks:
        //     See the Info(object) form for more detailed information.
        public void Info(object message, Exception exception)
        {
            Log4NetInstance.Info(message, exception);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Info level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Info(object,Exception) methods instead.
        public void InfoFormat(string format, object arg0)
        {
            Log4NetInstance.InfoFormat(format, arg0);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Info level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   args:
        //     An Object array containing zero or more objects to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Info(object) methods instead.
        public void InfoFormat(string format, params object[] args)
        {
            Log4NetInstance.InfoFormat(format, args);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Info level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        //   arg1:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Info(object,Exception) methods instead.
        public void InfoFormat(string format, object arg0, object arg1)
        {
            Log4NetInstance.InfoFormat(format, arg0, arg1);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Info level.
        //
        // Parameters:
        //   provider:
        //     An System.IFormatProvider that supplies culture-specific formatting information
        //
        //   format:
        //     A String containing zero or more format items
        //
        //   args:
        //     An Object array containing zero or more objects to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Info(object) methods instead.
        public void InfoFormat(IFormatProvider provider, string format, params object[] args)
        {
            Log4NetInstance.InfoFormat(provider, format, args);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Info level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        //   arg1:
        //     An Object to format
        //
        //   arg2:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Info(object,Exception) methods instead.
        public void InfoFormat(string format, object arg0, object arg1, object arg2)
        {
            Log4NetInstance.InfoFormat(format, arg0, arg1, arg2);
        }
        //
        // Summary:
        //     Log a message object with the log4net.Core.Level.Warn level.
        //
        // Parameters:
        //   message:
        //     The message object to log.
        //
        // Remarks:
        //     This method first checks if this logger is WARN enabled by comparing the level
        //     of this logger with the log4net.Core.Level.Warn level. If this logger is WARN
        //     enabled, then it converts the message object (passed as parameter) to a string
        //     by invoking the appropriate log4net.ObjectRenderer.IObjectRenderer. It then proceeds
        //     to call all the registered appenders in this logger and also higher in the hierarchy
        //     depending on the value of the additivity flag.
        //     WARNING Note that passing an System.Exception to this method will print the name
        //     of the System.Exception but no stack trace. To print a stack trace use the Warn(object,Exception)
        //     form instead.
        public void Warn(object message)
        {
            Log4NetInstance.Warn(message);
        }
        //
        // Summary:
        //     Log a message object with the log4net.Core.Level.Warn level including the stack
        //     trace of the System.Exception passed as a parameter.
        //
        // Parameters:
        //   message:
        //     The message object to log.
        //
        //   exception:
        //     The exception to log, including its stack trace.
        //
        // Remarks:
        //     See the Warn(object) form for more detailed information.
        public void Warn(object message, Exception exception)
        {
            Log4NetInstance.Warn(message, exception);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Warn level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Warn(object,Exception) methods instead.
        public void WarnFormat(string format, object arg0)
        {
            Log4NetInstance.WarnFormat(format, arg0);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Warn level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   args:
        //     An Object array containing zero or more objects to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Warn(object) methods instead.
        public void WarnFormat(string format, params object[] args)
        {
            Log4NetInstance.WarnFormat(format, args);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Warn level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        //   arg1:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Warn(object,Exception) methods instead.
        public void WarnFormat(string format, object arg0, object arg1)
        {
            Log4NetInstance.WarnFormat(format, arg0, arg1);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Warn level.
        //
        // Parameters:
        //   provider:
        //     An System.IFormatProvider that supplies culture-specific formatting information
        //
        //   format:
        //     A String containing zero or more format items
        //
        //   args:
        //     An Object array containing zero or more objects to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Warn(object) methods instead.
        public void WarnFormat(IFormatProvider provider, string format, params object[] args)
        {
            Log4NetInstance.WarnFormat(provider, format, args);
        }
        //
        // Summary:
        //     Logs a formatted message string with the log4net.Core.Level.Warn level.
        //
        // Parameters:
        //   format:
        //     A String containing zero or more format items
        //
        //   arg0:
        //     An Object to format
        //
        //   arg1:
        //     An Object to format
        //
        //   arg2:
        //     An Object to format
        //
        // Remarks:
        //     The message is formatted using the String.Format method. See String.Format(string,
        //     object[]) for details of the syntax of the format string and the behavior of
        //     the formatting.
        //     This method does not take an System.Exception object to include in the log event.
        //     To pass an System.Exception use one of the Warn(object,Exception) methods instead.
        public void WarnFormat(string format, object arg0, object arg1, object arg2)
        {
            Log4NetInstance.WarnFormat(format, arg0, arg1, arg2);
        }

        #endregion

        #region Dispose Methods

        public bool Disposed { get; private set; }

        // Implement IDisposable.
        // Do not make this method virtual.
        // A derived class should not be able to override this method.
        public void Dispose()
        {
        	Dispose(true);
        	// This object will be cleaned up by the Dispose method.
        	// Therefore, you should call GC.SupressFinalize to
        	// take this object off the finalization queue
        	// and prevent finalization code for this object
        	// from executing a second time.
        	GC.SuppressFinalize(this);
        }

        // Dispose(bool disposing) executes in two distinct scenarios.
        // If disposing equals true, the method has been called directly
        // or indirectly by a user's code. Managed and unmanaged resources
        // can be disposed.
        // If disposing equals false, the method has been called by the
        // runtime from inside the finalizer and you should not reference
        // other objects. Only unmanaged resources can be disposed.
        private void Dispose(bool disposing)
        {
        	// Check to see if Dispose has already been called.
        	if(!this.Disposed)
        	{

        		if(disposing)
        		{
        			// Dispose all managed resources.                    
        		}

        		//Dispose of all unmanaged resources

        		// Note disposing has been done.
        		this.Disposed = true;
        	}
        }
        #endregion //end of Dispose Methods
    }
}
