using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Globalization;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticLog4NetParser
{
    public sealed class LogMessages : ILogMessages
    {
        private readonly static string[] LogLevels = new string[] { "DEBUG ", "INFO ", "WARN ", "ERROR ", "FATAL " };

        public LogMessages(IFilePath logFile, string log4netConversionPattern, INode node = null)
        {
            if (string.IsNullOrEmpty(log4netConversionPattern)) throw new ArgumentNullException("log4netConversionPattern");
            if (logFile == null) throw new ArgumentNullException("logFile");

            this.LogFile = logFile;
            this.Log4NetConversionPattern = log4netConversionPattern;

            var log4netImporter = Log4NetPatternImporter.GenerateRegularGrammarElement(this.Log4NetConversionPattern);

            this.Log4NetConversionPatternRegExLine = CreateRegEx("LineRegEx", log4netImporter);
            this.DetermineLogDateTime(log4netImporter);
            this.Node = node;
            this._timeZoneInstance = this.Node?.Machine.TimeZone;
            this.IANATimeZoneName = this._timeZoneInstance?.Name;
        }

        public LogMessages(IFilePath logFile, string log4netConversionPattern, string ianaTimeZoneName)
            :this(logFile, log4netConversionPattern)
        {
            this._timeZoneInstance = Common.TimeZones.Find(ianaTimeZoneName, Common.Patterns.TimeZoneInfo.ZoneNameTypes.IANATZName);

            if(this._timeZoneInstance == null)
            {
                Logger.Instance.WarnFormat("{0}\t{1}\tCould not fine IANA Timezone \"{2}\". Log4Net LogDateTimewTZOffset property will use a timezone of UTC.",
                                            this.Node,
                                            logFile,
                                            ianaTimeZoneName);
            }
            else
            {
                this.IANATimeZoneName = this._timeZoneInstance.Name;
            }
        }

        public INode Node { get; private set; }
        public string IANATimeZoneName { get; private set; }
        public string Log4NetConversionPattern { get; private set; }
        public Regex Log4NetConversionPatternRegExLine { get; private set; }

        public IFilePath LogFile { get; private set; }

        private List<LogMessage> _logMessages = new List<LogMessage>();

        public IEnumerable<ILogMessage> Messages { get { return this._logMessages; } }

        public DateTimeRange LogTimeRange
        {
            get
            {
                return this._startingLogDateTime.IsMinOrMaxValue()
                            ? DateTimeRange.EmptyMaxMin
                            : new DateTimeRange(this._startingLogDateTime, this._endingLogDateTime);
            }
        }

        private List<string> _errors = new List<string>();
        public IEnumerable<string> Errors { get { return this._errors; } }

        public LogMessage AddMessage(string logLine, uint logLinePos)
        {
            logLine = logLine?.Trim();

            if (string.IsNullOrEmpty(logLine))
            {
                return null;
            }

            var lineMatch = this.Log4NetConversionPatternRegExLine.Match(logLine);
            LogMessage logMessage = null;

            if (lineMatch.Success)
            {
                logMessage = new LogMessage(logLinePos);
            }
            else if (this._lastMessage != null)
            {
                if (LogLevels.Any(l => logLine.StartsWith(l, StringComparison.InvariantCultureIgnoreCase)))
                {
                    var error = string.Format("{0}\t{1}\tInvalid line detected ({2}) in Log4Net Log file at line number {3}. Line ignored.",
                                                this.Node,
                                                this.LogFile.PathResolved,
                                                logLine,
                                                logLinePos);
                    Logger.Instance.Warn(error);
                    this._errors.Add(error);
                    return null;
                }

                this._lastMessage.AddExtraMessage(logLine);
                return this._lastMessage;
            }
            else
            {
                var error = string.Format("{0}\t{1}\tInvalid line detected ({2}) in Log4Net Log file at line number {3}. Line ignored.",
                                            this.Node,
                                            this.LogFile.PathResolved,
                                            logLine,
                                            logLinePos);

                Logger.Instance.Warn(error);
                this._errors.Add(error);
                return null;
            }

            this._lastMessage = logMessage;

            #region Log TimeStamp or Time
            if (this._logTimestampValue)
            {
                long timeStamp;

                if (long.TryParse(lineMatch.Groups["Timestamp"].Value.Trim(), out timeStamp))
                {
                    logMessage.LogTimeSpan = TimeSpan.FromMilliseconds(timeStamp);
                }
                else
                {
                    var error = string.Format("{0}\t{1}\tInvalid Timestamp detected ({2}) in line \"{3}\" within Log4Net Log file at line number {4}. Line ignored.",
                                                this.Node,
                                                this.LogFile.PathResolved,
                                                lineMatch.Groups["Timestamp"].Value,
                                                logLine,
                                                logLinePos);
                    Logger.Instance.Warn(error);
                    this._errors.Add(error);
                    return null;
                }
            }
            else
            {
                DateTime timeStamp;

                if (string.IsNullOrEmpty(this._datetimeFormat)
                        ? DateTime.TryParse(lineMatch.Groups["Time"].Value.Trim(), out timeStamp)
                        : DateTime.TryParseExact(lineMatch.Groups["Time"].Value.Trim(),
                                                    this._datetimeFormat,
                                                    CultureInfo.InvariantCulture,
                                                    DateTimeStyles.None,
                                                    out timeStamp))

                {
                    logMessage.LogDateTime = timeStamp;

                    if(this._timeZoneInstance == null)
                    {
                        logMessage.LogDateTimewTZOffset = new DateTimeOffset(logMessage.LogDateTime, TimeSpan.Zero);
                    }
                    else
                    {
                        logMessage.LogDateTimewTZOffset = Common.TimeZones.ConvertToOffset(logMessage.LogDateTime, this._timeZoneInstance);
                    }

                    this._endingLogDateTime = timeStamp;
                    if (this._startingLogDateTime == DateTime.MinValue)
                    {
                        this._startingLogDateTime = timeStamp;
                    }
                }
                else
                {
                    var error = string.Format("{0}\t{1}\tInvalid DateTime detected ({2}) in line \"{3}\" within Log4Net Log file at line number {4}{5}. Line ignored.",
                                                this.Node,
                                                this.LogFile.PathResolved,
                                                lineMatch.Groups["Time"].Value,
                                                logLine,
                                                logLinePos,
                                                string.IsNullOrEmpty(this._datetimeFormat) ? string.Empty : this._datetimeFormat);
                    Logger.Instance.Warn(error);
                    this._errors.Add(error);
                    return null;
                }
            }
            #endregion
            #region Log Level
            {
                LogLevels logLevel;

                if (Enum.TryParse(lineMatch.Groups["Level"].Value.Trim(), true, out logLevel))
                {
                    logMessage.Level = logLevel;
                }
                else
                {
                    var error = string.Format("{0}\t{1}\tInvalid Log Level detected ({2}) in line \"{3}\" within Log4Net Log file at line number {4}. Line ignored.",
                                                this.Node,
                                                this.LogFile.PathResolved,
                                                lineMatch.Groups["Level"].Value,
                                                logLine,
                                                logLinePos);
                    Logger.Instance.Warn(error);
                    this._errors.Add(error);
                    return null;
                }
            }
            #endregion
            #region Log File Line
            {
                int fileLine;

                if (int.TryParse(lineMatch.Groups["Line"].Value.Trim(), out fileLine))
                {
                    logMessage.FileLine = fileLine;
                }
                else
                {
                    var error = string.Format("{0}\t{1}\tInvalid Log File Line detected ({2}) in line \"{3}\" within Log4Net Log file at line number {4}. Line ignored.",
                                                this.Node,
                                                this.LogFile.PathResolved,
                                                lineMatch.Groups["Line"].Value,
                                                logLine,
                                                logLinePos);
                    Logger.Instance.Warn(error);
                    this._errors.Add(error);
                    return null;
                }
            }
            #endregion
            #region Remaining Log string fields
            logMessage.FileName = lineMatch.Groups["File"].Value.Trim();
            logMessage.ThreadId = lineMatch.Groups["Thread"].Value.Trim();
            logMessage.Message = lineMatch.Groups["Message"].Value.Trim();
            #endregion

            this._logMessages.Add(logMessage);
            return logMessage;
        }

        #region IDisposable
        public void Dispose()
        {
            if (this._logMessages != null) this._logMessages.Clear();
            if (this._errors != null) this._errors.Clear();
            this.Log4NetConversionPatternRegExLine = null;
            this._logMessages = null;
            this._lastMessage = null;
            this._errors = null;
        }

        #endregion

        #region private members

        LogMessage _lastMessage;
        string _datetimeFormat;
        bool _logTimestampValue = false;
        DateTime _startingLogDateTime;
        DateTime _endingLogDateTime;
        Common.Patterns.TimeZoneInfo.IZone _timeZoneInstance = null;

        static Regex CreateRegEx(string field, IDictionary<string, Log4NetPatternImporter.OutputField> outputFields)
        {
            Log4NetPatternImporter.OutputField outputField;

            if (outputFields.TryGetValue(field, out outputField))
            {
                return new Regex(outputField.Code.ToString(),
                                    RegexOptions.Compiled
                                        | RegexOptions.IgnorePatternWhitespace
                                        | RegexOptions.IgnoreCase);
            }

            return null;
        }

        void DetermineLogDateTime(IDictionary<string, Log4NetPatternImporter.OutputField> outputFields)
        {
            Log4NetPatternImporter.OutputField outputField;

            if (outputFields.TryGetValue("Time", out outputField))
            {
                this._logTimestampValue = false;
                this._datetimeFormat = outputField.Code?.ToString();
            }
            else if (outputFields.TryGetValue("Timestamp", out outputField))
            {
                this._logTimestampValue = true;
                this._datetimeFormat = outputField.Code?.ToString();
            }
            else
            {
                throw new ArgumentOutOfRangeException(string.Format("Could not find the \"Time\" field in the Log4Net Conversion Pattern string ({0}) for file \"{1}\".",
                                                                        this.Log4NetConversionPattern,
                                                                        this.LogFile.PathResolved));
            }
        }
        #endregion
    }
}
