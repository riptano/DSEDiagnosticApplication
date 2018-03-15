using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;
using Common;
using DSEDiagnosticLog4NetParser;
using CTS = Common.Patterns.Collections.ThreadSafe;
using IMMLogValue = Common.Patterns.Collections.MemoryMapped.IMMValue<DSEDiagnosticLibrary.ILogEvent>;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class file_cassandra_log4net : DiagnosticFile
    {

        [Flags]
        public enum DefaultLogLevelHandlers
        {
            Disabled = 0,
            Debug = 0x0001,
            Informational = 0x0002,
            Warning = 0x0004,
            Error = 0x0008,
            Fatal = 0x0010,
            Exception = 0x0020,
            Default = Warning | Error | Fatal | Exception,
            All = Debug | Informational | Warning | Error | Fatal | Exception
        }
        public static DefaultLogLevelHandlers DefaultLogLevelHandling = LibrarySettings.DefaultLogLevelHandlingInit;
        public static DateTimeOffsetRange LogTimeRange = null;

        struct CurrentSessionLineTick
        {
            public DateTime LogTick;
            public LogCassandraEvent CurrentSession;
        }

        struct LateDDLResolution
        {
            public string KSName;
            public string DDLSSTableName;
            public IEnumerable<string> DDLNames;
        }

        private CurrentSessionLineTick _priorSessionLineTick;
        private List<LogCassandraEvent> _sessionEvents = new List<LogCassandraEvent>();
        private Dictionary<string,LogCassandraEvent> _openSessions = new Dictionary<string, LogCassandraEvent>();
        private List<LogCassandraEvent> _unsedOpenSessionEvents = new List<LogCassandraEvent>();
        private Dictionary<string, Stack<LogCassandraEvent>> _lookupSessionLabels = new Dictionary<string, Stack<LogCassandraEvent>>();
        private List<LogCassandraEvent> _orphanedSessionEvents = new List<LogCassandraEvent>();
        CLogLineTypeParser[] _parser = null;
        List<LogCassandraEvent> _logEvents = new List<LogCassandraEvent>();
        List<Tuple<LogCassandraEvent, LateDDLResolution>> _lateResolutionEvents = new List<Tuple<LogCassandraEvent, LateDDLResolution>>();

        public file_cassandra_log4net(CatagoryTypes catagory,
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node,
                                    string defaultClusterName,
                                    string defaultDCName,
                                    Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName,targetDSEVersion)
        {
            if (this.Node != null)
            {
                this.Cluster = this.Node.Cluster;
            }

            if (this.Cluster == null)
            {
                if (!string.IsNullOrEmpty(defaultClusterName))
                {
                    this.Cluster = Cluster.TryGetCluster(defaultClusterName);
                }

                if (this.Cluster == null)
                {
                    this.Cluster = Cluster.GetCurrentOrMaster();
                }
            }

            if (this.Node != null)
            {
                this.DataCenter = this.Node.DataCenter;
            }

            if (this.DataCenter == null && !string.IsNullOrEmpty(defaultDCName))
            {
                this.DataCenter = Cluster.TryGetDataCenter(defaultDCName, this.Cluster);
            }

            this._parser = LibrarySettings.Log4NetParser.DetermineParsers(targetDSEVersion);
            this._dcKeyspaces = this.Cluster.GetKeyspaces(this.DataCenter);

            this._result = new LogResults(this);

            this._nodeStrId = this.Node.Id.GetHashCode().ToString();

            Logger.Instance.DebugFormat("Loaded class \"{0}\"{{File{{{1}}}, Parser{{{2}}} }}",
                                            this.GetType().Name,
                                            this.File,
                                            this._parser == null ? string.Empty : string.Join(", ", this._parser.Select(l => l.ToString())));
        }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class LogResults : IResult
        {
            private LogResults() { }
            internal LogResults(file_cassandra_log4net fileInstance)
            {
                this.Path = fileInstance.File;
                this.Node = fileInstance.Node;
                this.OrphanedSessionEvents = fileInstance._orphanedSessionEvents;
            }

            public readonly IEnumerable<LogCassandraEvent> OrphanedSessionEvents;

            #region IResult
            [JsonConverter(typeof(DSEDiagnosticLibrary.IPathJsonConverter))]
            public IPath Path { get; private set; }
            [JsonIgnore]
            public Cluster Cluster { get { return this.DataCenter?.Cluster; } }
            [JsonIgnore]
            public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
            public INode Node { get; private set; }
            public int NbrItems { get; private set; }

            [JsonIgnore]
            public IEnumerable<IParsed> Results { get { return this.ResultsTask?.Result.Select(e => e.GetValue()); } }

            #endregion

            [JsonIgnore]
            public Task<IEnumerable<IMMLogValue>> ResultsTask { get; private set; }

            internal int RunLogResultsTask(INode node, List<LogCassandraEvent> eventList)
            {
                this.NbrItems = eventList.Count;
                this.ResultsTask = System.Threading.Tasks.Task.Factory.StartNew(() =>
                                    {
                                        var mmEvents = new List<IMMLogValue>(eventList.Count);

                                        eventList.ForEach(e =>
                                        {
                                            mmEvents.Add(node.AssociateItem(e));
                                        });

                                        eventList.Clear();
                                        return (IEnumerable<IMMLogValue>) mmEvents;
                                    });
                return this.NbrItems;
            }
        }

        [JsonProperty(PropertyName="Results")]
        private readonly LogResults _result;

        public Cluster Cluster { get; private set; }
        public IDataCenter DataCenter { get; private set; }
        [JsonProperty(PropertyName="Keyspaces")]
        private IEnumerable<IKeyspace> _dcKeyspaces;

        public override IResult GetResult()
        {
            return this._result;
        }

        /// <summary>
        /// Clears all log related caches. Should only be called when log processing is complete.
        /// </summary>
        public static new void ClearCaches()
        {
            LogLinesHash.Clear();
        }

        public override uint ProcessFile()
        {
            this.CancellationToken.ThrowIfCancellationRequested();
            DateTimeOffsetRange logFileDateRange = null;
            DateTimeOffset? lastShutdown = null;
            var nodeRestarts = new List<DateTimeOffsetRange>();

            using (var logFileInstance = new ReadLogFile(this.File, LibrarySettings.Log4NetConversionPattern, this.CancellationToken, this.Node, LogTimeRange))
            {
                Tuple<Regex, Match, Regex, Match, CLogLineTypeParser> matchItem;
                LogCassandraEvent logEvent;
                bool firstLine = true;
                bool ignoreLogEvent = false;
                bool handleExceptions = DefaultLogLevelHandling.HasFlag(DefaultLogLevelHandlers.Exception);
                bool disableLogLevelHandling = DefaultLogLevelHandling == DefaultLogLevelHandlers.Disabled
                                                || DefaultLogLevelHandling == DefaultLogLevelHandlers.Exception;
                
                logFileInstance.ProcessedLogLineAction += (IReadLogFile readLogFileInstance, ILogMessages alreadyParsedLines, ILogMessage logMessage) =>
                {
                    readLogFileInstance.CancellationToken.ThrowIfCancellationRequested();

                    if (firstLine)
                    {
                        firstLine = false;

                        //Need to load any orphaned events from prior log files
                        var firstMsgTime = logMessage.LogDateTime;
                        var possibleOpenSessions = this.Node.LogFiles.Where(f => firstMsgTime < f.LogDateRange.Max)
                                                                            .SelectMany(f => f.OrphanedEvents)
                                                                            .OrderBy(o => o.EventTimeLocal);

                        possibleOpenSessions.ForEach(o => this._openSessions[o.SessionTieOutId] = (LogCassandraEvent)o);
                    }

                    //Exception messages will be invoked twice. Once on the original log event (no exception info) and than with the exception information for the original logMessage instance. 
                    if (logMessage.ExtraMessages.HasAtLeastOneElement())
                    {
                        if (handleExceptions && !ignoreLogEvent)
                        {
                            logEvent = this.PorcessException(logMessage, this._logEvents.LastOrDefault());

                            if (logEvent != null && !ReferenceEquals(logEvent, this._logEvents.LastOrDefault()))
                                this._logEvents.Add(logEvent);
                        }

                        return;
                    }

                    if (this.DuplicateLogEventFound(logMessage))
                    {
                        return;
                    }                    
                    
                    matchItem = CLogTypeParser.FindMatch(this._parser, logMessage);

                    logEvent = null;
                    ignoreLogEvent = false;

                    if (matchItem == null)
                    {
                        if (!disableLogLevelHandling)
                        {
                            if ((logMessage.Level == LogLevels.Fatal
                                        && (DefaultLogLevelHandling & DefaultLogLevelHandlers.Fatal) == DefaultLogLevelHandlers.Fatal)
                                    || (logMessage.Level == LogLevels.Error
                                        && (DefaultLogLevelHandling & DefaultLogLevelHandlers.Error) == DefaultLogLevelHandlers.Error)
                                    || (logMessage.Level == LogLevels.Warn
                                        && (DefaultLogLevelHandling & DefaultLogLevelHandlers.Warning) == DefaultLogLevelHandlers.Warning)
                                    || (logMessage.Level == LogLevels.Info
                                        && (DefaultLogLevelHandling & DefaultLogLevelHandlers.Informational) == DefaultLogLevelHandlers.Informational)
                                    || (logMessage.Level == LogLevels.Debug
                                        && (DefaultLogLevelHandling & DefaultLogLevelHandlers.Debug) == DefaultLogLevelHandlers.Debug))
                            {
                                logEvent = this.PorcessUnHandledEventLevel(logMessage);

                                if (logEvent != null)
                                    this._logEvents.Add(logEvent);
                            }
                        }
                    }
                    else if (matchItem.Item5.IgnoreEvent)
                    {
                        ignoreLogEvent = true;
                        return;
                    }
                    else
                    {
                        logEvent = this.PorcessMatch(logMessage, matchItem);

                        if (logEvent != null)
                        {
                            this._logEvents.Add(logEvent);

                            if((logEvent.Class & EventClasses.GC) == EventClasses.GC && this.Node.Machine.Java.GCType == null)
                            {
                                object gcType;

                                if (logEvent.LogProperties.TryGetValue("GC", out gcType))
                                    this.Node.Machine.Java.GCType = gcType as string;
                            }
                            else if((logEvent.Class & EventClasses.NodeDetection) == EventClasses.NodeDetection && logEvent.SubClass != null)
                            {
                                if(logEvent.SubClass.StartsWith("shutdown"))
                                {
                                    lastShutdown = logEvent.EventTime;
                                }
                                else if (logEvent.SubClass.StartsWith("startup"))
                                {
                                    if(lastShutdown.HasValue)
                                    {
                                        nodeRestarts.Add(new DateTimeOffsetRange(lastShutdown.Value, logEvent.EventTime));
                                    }
                                    lastShutdown = null;
                                }
                            }
                        }
                    }                    
                };

                using (var logMessages = logFileInstance.ProcessLogFile())
                {
                    this.NbrItemsParsed = unchecked((int)logFileInstance.LinesRead);

                    logFileDateRange = logMessages.LogTimeRange;
                }
            }

            this._orphanedSessionEvents.AddRange(this._unsedOpenSessionEvents.Where(s => !s.EventTimeEnd.HasValue).Select(s => { s.MarkAsOrphaned(); return s; }));
            this._orphanedSessionEvents.AddRange(this._openSessions.Where(s => !s.Value.EventTimeEnd.HasValue).Select(s => { s.Value.MarkAsOrphaned(); return s.Value; }));

            this.Node.LogFiles.ForEach(f =>
            {
                var removeItems = f.OrphanedEvents.Where(o => (o.Class & EventClasses.Orphaned) == 0);
                removeItems.ForEach(r => f.OrphanedEvents.Remove(r));
            });

            {
                var logFileInfo = new LogFileInfo(this.File,
                                                    logFileDateRange,
                                                    this._logEvents.Count,
                                                    this._orphanedSessionEvents.Count == 0 ? null : this._orphanedSessionEvents,
                                                    this._logEvents.Count == 0 ? null : new DateTimeOffsetRange(this._logEvents.First().EventTime, this._logEvents.Last().EventTime),
                                                    nodeRestarts.Count == 0 ? null : nodeRestarts);

                var fndOverlapppingLogs = this.Node.LogFiles.Where(l => l.LogDateRange.IsBetween(logFileInfo.LogDateRange));

                if (fndOverlapppingLogs.HasAtLeastOneElement())
                {
                    Logger.Instance.WarnFormat("MapperId<{0}>\t{1}\t{2}\tDetected overlapping of logs for Event Date Range {3} with logs {{{4}}} ",
                                                    this.MapperId,
                                                    this.Node,
                                                    this.File.PathResolved,
                                                    logFileInfo.LogDateRange,
                                                    string.Join(", ", fndOverlapppingLogs));
                    ++this.NbrWarnings;
                }
                this.Node.AssociateItem(logFileInfo);
            }

            this._lookupSessionLabels.Clear();
            this._openSessions.Clear();
            this._unsedOpenSessionEvents.Clear();

            return (uint) this._result.RunLogResultsTask(this.Node, this._logEvents);
        }

        private LogCassandraEvent PorcessUnHandledEventLevel(ILogMessage logMessage)
        {
            var logProperties = new Dictionary<string, object>();
            string topLevelException;
            var exceptionPaths = new List<string>();

            topLevelException = this.DetermineExceptionInfo(logMessage, logProperties, exceptionPaths, false);

            UnitOfMeasure duration;
            DateTime? eventDurationTime;
            string logId;
            List<string> sstableFilePaths;
            IKeyspace primaryKS;
            IDDLStmt primaryDDL;
            List<IDDLStmt> ddlInstances;
            IEnumerable<INode> assocatedNodes;
            IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges;
            LateDDLResolution? lateDDLresolution;
            string subClass;

            this.DetermineProperties(logMessage,
                                        logProperties,
                                        out lateDDLresolution,
                                        out logId,
                                        out subClass,
                                        out primaryKS,
                                        out primaryDDL,
                                        out duration,
                                        out eventDurationTime,
                                        out sstableFilePaths,
                                        out ddlInstances,
                                        out assocatedNodes,
                                        out tokenRanges);

            var logEvent = new LogCassandraEvent(this.File,
                                                    this.Node,
                                                    (uint)logMessage.FileLine,
                                                    EventClasses.NotHandled, //If changed, update PorcessException method
                                                    logMessage.LogDateTime,
                                                    logMessage.LogDateTimewTZOffset,
                                                    logMessage.Message,
                                                    EventTypes.SingleInstance,
                                                    logId,
                                                    subClass,
                                                    null, //parentEvents,
                                                    this.Node.Machine.TimeZone,
                                                    primaryKS,
                                                    primaryDDL,
                                                    duration.NaN
                                                        ? (TimeSpan?)(eventDurationTime.HasValue ? eventDurationTime.Value - logMessage.LogDateTime : (TimeSpan?)null)
                                                        : (TimeSpan?)duration,
                                                    logProperties.Count == 0 ? (IReadOnlyDictionary<string, object>)null : logProperties,
                                                    sstableFilePaths,
                                                    ddlInstances,
                                                    assocatedNodes,
                                                    tokenRanges,
                                                    null //sessionId,
                                                    );

            logEvent.SetException(topLevelException, exceptionPaths.Count == 0 ? null : exceptionPaths, false);

            return logEvent;
        }

        /*
         INFO [SharedPool-Worker-2] 2016-07-25 04:25:35,012  Message.java:532 - Unexpected exception during request; channel = [id: 0x40c292ba, /10.0.0.2:42705 :> /<1ocal node>:9042]
        java.io.IOException: Error while read(...): Connection reset by peer

        ERROR [SharedPool-Worker-4] 2016-07-25 04:35:35,123  Message.java:617 - Unexpected exception during request; channel = [id: 0xd30bc260, /10.0.0.2:55526 => /10.0.0.1:9042]
        java.lang.RuntimeException: org.apache.cassandra.exceptions.ReadTimeoutException: Operation timed out - received only 0 responses.
        Caused by: org.apache.cassandra.exceptions.ReadTimeoutException: Operation timed out - received only 0 responses

        ERROR [SharedPool-Worker-4] 2016-07-25 04:35:35,123  Message.java:617 - Unexpected exception during request; channel = [id: 0xd30bc260, /10.0.0.2:55526 => /10.0.0.1:9042]
        java.lang.RuntimeException: org.apache.cassandra.exceptions.ReadTimeoutException: Operation timed out - received only 0 responses.


        ERROR [SharedPool-Worker-5] 2016-07-25 04:45:35,234  Message.java:617 - Unexpected exception during request; channel = [id: 0x16977ddb, /10.0.0.2:61415 => /10.0.0.1:9042]
        com.google.common.util.concurrent.UncheckedExecutionException: java.lang.RuntimeException: org.apache.cassandra.exceptions.UnavailableException: Cannot achieve consistency level LOCAL_ONE
        Caused by: org.apache.cassandra.exceptions.ReadTimeoutException: Operation timed out - received only 0 responses.

        ERROR [SSTableBatchOpen:2] 2016-07-25 05:15:35,590  DebuggableThreadPoolExecutor.java:227 - Error in ThreadPoolExecutor
        java.lang.AssertionError: Data component is missing for sstable/apps/cassandra/data/data2/system/local-7ad54392bcdd35a684174e047860b377/system-local-ka-11048

        ERROR [SharedPool-Worker-1] 2016-09-28 19:18:25,277  CqlSolrQueryExecutor.java:375 - No response after timeout: 60000
        org.apache.solr.common.SolrException: No response after timeout: 60000

        ERROR [MessagingService-Incoming-/10.12.49.27] 2016-09-28 18:53:54,898  JVMStabilityInspector.java:106 - JVM state determined to be unstable.  Exiting forcefully due to:
        java.lang.OutOfMemoryError: Java heap space

         ERROR [Repair#508:1] 2017-08-28 14:48:16,321  SystemDistributedKeyspace.java:208 - Error executing query UPDATE system_distributed.repair_history SET status = 'SUCCESS', finished_at = toTimestamp(now()) WHERE keyspace_name = 'rts_data' AND columnfamily_name = 'minmax_by_logid_mnemonic' AND id = eb437f00-8bff-11e7-8f3d-cf2f446dbb84
         org.apache.cassandra.exceptions.UnavailableException: Cannot achieve consistency level ONE
        */

        private LogCassandraEvent PorcessException(ILogMessage logMessage, LogCassandraEvent lastLogEvent)
        {
            var logProperties = new Dictionary<string, object>();           
            string topLevelException;
            var exceptionPaths = new List<string>();
            
            topLevelException = this.DetermineExceptionInfo(logMessage, logProperties, exceptionPaths);

            UnitOfMeasure duration;
            DateTime? eventDurationTime;
            string logId;
            List<string> sstableFilePaths;
            IKeyspace primaryKS;
            IDDLStmt primaryDDL;
            List<IDDLStmt> ddlInstances;
            IEnumerable<INode> assocatedNodes;
            IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges;
            LateDDLResolution? lateDDLresolution;
            string subClass;
            
            this.DetermineProperties(logMessage,
                                        logProperties,
                                        out lateDDLresolution,
                                        out logId,
                                        out subClass,
                                        out primaryKS,
                                        out primaryDDL,
                                        out duration,
                                        out eventDurationTime,
                                        out sstableFilePaths,
                                        out ddlInstances,
                                        out assocatedNodes,
                                        out tokenRanges);

            if(lastLogEvent != null && lastLogEvent.Class == EventClasses.NotHandled)
            {
                lastLogEvent = null;
                this._logEvents.RemoveAt(this._logEvents.Count - 1);
            }

            LogCassandraEvent logEvent = lastLogEvent != null
                                                && lastLogEvent.LineNbr == (uint)logMessage.FileLine
                                            ? lastLogEvent
                                            : null;

            if (logEvent == null)
            {
                logEvent = new LogCassandraEvent(this.File,
                                                    this.Node,
                                                    (uint)logMessage.FileLine,
                                                    EventClasses.Exception,
                                                    logMessage.LogDateTime,
                                                    logMessage.LogDateTimewTZOffset,
                                                    logMessage.Message,
                                                    EventTypes.SingleInstance,
                                                    logId,
                                                    subClass,
                                                    null, //parentEvents,
                                                    this.Node.Machine.TimeZone,
                                                    primaryKS,
                                                    primaryDDL,
                                                    duration.NaN
                                                        ? (TimeSpan?)(eventDurationTime.HasValue ? eventDurationTime.Value - logMessage.LogDateTime : (TimeSpan?)null)
                                                        : (TimeSpan?)duration,
                                                    logProperties.Count == 0 ? (IReadOnlyDictionary<string, object>)null : logProperties,
                                                    sstableFilePaths,
                                                    ddlInstances,
                                                    assocatedNodes,
                                                    tokenRanges,
                                                    null //sessionId,
                                                    );

                logEvent.SetException(topLevelException, exceptionPaths.Count == 0 ? null : exceptionPaths);
            }
            else
            {
                logEvent.SetException(topLevelException,
                                        exceptionPaths.Count == 0 ? null : exceptionPaths,
                                        logProperties.Count == 0 ? (IReadOnlyDictionary<string, object>)null : logProperties);
                //logEvent = null;
            }

            return logEvent;
        }

        private string DetermineExceptionInfo(ILogMessage logMessage, Dictionary<string, object> logProperties, List<string> exceptionPaths, bool processExceptionString = true)
        {
            string topLefvelException = null;
            var msgList = new List<string>() { logMessage.Message };
            msgList.AddRange(logMessage.ExtraMessages);

            foreach(var exceptionLine in msgList)
            {
                //Skip stack
                if (exceptionLine.TrimStart().StartsWith("at ", StringComparison.OrdinalIgnoreCase)
                        || exceptionLine.TrimStart().StartsWith("..."))
                    continue;
               
                string exceptClass;
                string exceptDescription;

                if(processExceptionString)
                {
                    var exceptionDesc = ParseExceptionString(exceptionLine);
                    exceptClass = exceptionDesc.Item1;
                    exceptDescription = exceptionDesc.Item2;
                }
                else
                {
                    exceptClass = null;
                    exceptDescription = exceptionLine.Trim();
                }

                if (topLefvelException == null && !string.IsNullOrEmpty(exceptClass))
                    topLefvelException = exceptClass;

                if (exceptDescription.IndexOf("/<1ocal node>") >= 0)
                {
                    var ipAddress = this.Node.Id.Addresses.FirstOrDefault()?.ToString();

                    if(!string.IsNullOrEmpty(ipAddress))
                        exceptDescription = exceptDescription.Replace("/<1ocal node>", string.Format("/{0}", ipAddress));
                }

                var matches = string.IsNullOrEmpty(exceptDescription) ? null : LibrarySettings.LogExceptionRegExMatches.Matches(exceptDescription);

                if (matches != null)
                {
                    foreach (Match match in matches)
                    {
                        UpdateMatchProperties(LibrarySettings.LogExceptionRegExMatches, match, logProperties, true);
                    }

                    if (logProperties.ContainsKey("NODE"))
                    {
                        var assocNodes = logProperties["NODE"];

                        if (assocNodes is IEnumerable<object>)
                        {                            
                            ((IEnumerable<object>)assocNodes)
                                .Where(n => n is System.Net.IPEndPoint)
                                .Cast<System.Net.IPEndPoint>()
                                .ForEach(ep => exceptDescription = exceptDescription.Replace(ep.Port.ToString(), "XXXX"));
                        }
                        else
                        {
                            var endPoint = assocNodes as System.Net.IPEndPoint;

                            if (endPoint != null)
                            {
                                exceptDescription = exceptDescription.Replace(endPoint.Port.ToString(), "XXXX");                                
                            }
                        }
                    }
                }

                var clsPath = string.Format("{0}({1})", exceptClass ?? logMessage.Level.ToString(), exceptDescription ?? string.Empty);

                if (exceptionPaths.Count > 1)
                {
                    var lstPath = exceptionPaths.Last();

                    if(lstPath != clsPath)
                    {
                        var emptyDesc = lstPath.EndsWith("()");

                        if(emptyDesc)
                        {
                            if (clsPath.StartsWith(lstPath.Substring(0, lstPath.Length - 1)))
                            {
                                exceptionPaths[exceptionPaths.Count - 1] = clsPath;
                            }
                            else
                            {
                                exceptionPaths.Add(clsPath);
                            }
                        }
                        else
                        {
                            exceptionPaths.Add(clsPath);
                        }
                    }
                }
                else
                {
                    exceptionPaths.Add(clsPath);
                }
            }

            return topLefvelException;
        }

        /// <summary>
        /// Takes a line and looks for the exception and description
        /// 
        /// cases like &quot;com.google.common.util.concurrent.UncheckedExecutionException: java.lang.RuntimeException: org.apache.cassandra.exceptions.UnavailableException: Cannot achieve consistency level LOCAL_ONE&quot;
        /// will return &quot;org.apache.cassandra.exceptions.UnavailableException&quot; and &quot;Cannot achieve consistency level LOCAL_ONE&quot;
        /// </summary>
        /// <param name="exceptionLine"></param>
        /// <returns>
        /// Returns the exception as the first item and the exception&apos;s description. If no exception detected, the first item is null.
        /// </returns>
        private static Tuple<string, string> ParseExceptionString(string exceptionLine)
        {
            if(exceptionLine.StartsWith("caused by:",StringComparison.OrdinalIgnoreCase))
            {
                exceptionLine = exceptionLine.Substring(9).TrimStart();
            }

            var exceptionInfo = StringFunctions.Split(exceptionLine,
                                                        ':',
                                                        StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                        StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries | StringFunctions.SplitBehaviorOptions.StringTrimEachElement | StringFunctions.SplitBehaviorOptions.MismatchedWithinDelimitersTreatAsSingleElement);

            if(exceptionInfo.Last() == "{}")
            {
                exceptionInfo = new List<string>(exceptionInfo.Take(exceptionInfo.Count - 1));
            }

            //Find first exception with a "valid" description 
            var validDescIndex = exceptionInfo.IndexOf(i => !i.EndsWith("exception", StringComparison.OrdinalIgnoreCase)
                                                            && !i.EndsWith("error", StringComparison.OrdinalIgnoreCase)
                                                            && !i.EndsWith("assertion", StringComparison.OrdinalIgnoreCase));

            return validDescIndex <= 0
                        ? (exceptionInfo.Count > 1 ? new Tuple<string, string>(exceptionInfo.Last(), string.Empty) : new Tuple<string, string>(null, exceptionLine.Trim()))                        
                        : new Tuple<string, string>(exceptionInfo[validDescIndex - 1], string.Join(": ", exceptionInfo.Skip(validDescIndex)));
        }

        /* INFO  [CompactionExecutor:749] 2017-02-11 00:17:43,927  CompactionTask.java:141 - Compacting [SSTableReader(path='/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156636-Data.db'), SSTableReader(path='/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156635-Data.db'), SSTableReader(path='/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156638-Data.db'), SSTableReader(path='/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156637-Data.db')]
            INFO  [CompactionExecutor:747] 2017-02-11 00:17:43,941  ColumnFamilyStore.java:905 - Enqueuing flush of compactions_in_progress: 1320 (0%) on-heap, 0 (0%) off-heap
            INFO  [MemtableFlushWriter:880] 2017-02-11 00:17:43,942  Memtable.java:347 - Writing Memtable-compactions_in_progress@707819311(0.149KiB serialized bytes, 9 ops, 0%/0% of on/off-heap limit)
            INFO  [MemtableFlushWriter:880] 2017-02-11 00:17:43,946  Memtable.java:382 - Completed flushing /data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-tmp-ka-156640-Data.db (0.000KiB) for commitlog position ReplayPosition(segmentId=1486705955830, position=14267865)
            INFO  [CompactionExecutor:749] 2017-02-11 00:17:43,975  CompactionTask.java:274 - Compacted 4 sstables to [/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156639,].  444 bytes to 64 (~14% of original) in 48ms = 0.001272MB/s.  5 total partitions merged to 1.  Partition merge counts were {1:1, 2:2, }

            INFO  [CompactionExecutor:810] 2017-02-11 01:42:30,054  CompactionTask.java:141 - Compacting [SSTableReader(path='/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156701-Data.db'), SSTableReader(path='/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156702-Data.db'), SSTableReader(path='/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156703-Data.db'), SSTableReader(path='/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156704-Data.db'), SSTableReader(path='/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156700-Data.db'), SSTableReader(path='/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156699-Data.db')]
            INFO  [CompactionExecutor:809] 2017-02-11 01:42:30,095  ColumnFamilyStore.java:905 - Enqueuing flush of compactions_in_progress: 165 (0%) on-heap, 0 (0%) off-heap
            INFO  [MemtableFlushWriter:914] 2017-02-11 01:42:30,096  Memtable.java:347 - Writing Memtable-compactions_in_progress@1885049902(0.008KiB serialized bytes, 1 ops, 0%/0% of on/off-heap limit)
            INFO  [MemtableFlushWriter:914] 2017-02-11 01:42:30,097  Memtable.java:382 - Completed flushing /data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-tmp-ka-156706-Data.db (0.000KiB) for commitlog position ReplayPosition(segmentId=1486705955849, position=5679993)
            INFO  [CompactionExecutor:807] 2017-02-11 01:42:30,119  ColumnFamilyStore.java:905 - Enqueuing flush of compactions_in_progress: 165 (0%) on-heap, 0 (0%) off-heap
            INFO  [MemtableFlushWriter:916] 2017-02-11 01:42:30,119  Memtable.java:347 - Writing Memtable-compactions_in_progress@1191566985(0.008KiB serialized bytes, 1 ops, 0%/0% of on/off-heap limit)
            INFO  [MemtableFlushWriter:916] 2017-02-11 01:42:30,122  Memtable.java:382 - Completed flushing /data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-tmp-ka-156707-Data.db (0.000KiB) for commitlog position ReplayPosition(segmentId=1486705955849, position=5680064)
            INFO  [CompactionExecutor:810] 2017-02-11 01:42:30,129  CompactionTask.java:274 - Compacted 6 sstables to [/data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-156705,].  993 bytes to 457 (~46% of original) in 73ms = 0.005970MB/s.  5 total partitions merged to 4.  Partition merge counts were {1:3, 2:1, }

            Open SSTable event
            INFO  [ValidationExecutor:2] 2017-02-11 00:00:29,417  SSTableReader.java:475 - Opening /data/cassandra/data/usprodrst/taxtype_31-699c56608e9a11e6953741bf7da4d7d7/snapshots/0213b9a0-f017-11e6-a2c9-555b697ee5b0/usprodrst-taxtype_31-ka-33 (14170 bytes)

            Reversed
            INFO  [MemtableFlushWriter:2168] 2017-02-11 23:59:52,790  Memtable.java:382 - Completed flushing /data/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-tmp-ka-159454-Data.db (0.000KiB) for commitlog position ReplayPosition(segmentId=1486705956468, position=30163060)
            INFO  [MemtableFlushWriter:2165] 2017-02-11 23:59:52,790  Memtable.java:347 - Writing Memtable-compactions_in_progress@561831731(0.173KiB serialized bytes, 9 ops, 0%/0% of on/off-heap limit)

        */

        private LogCassandraEvent PorcessMatch(ILogMessage logMessage, Tuple<Regex, Match, Regex, Match, CLogLineTypeParser> matchItem)
        {
            if(matchItem.Item5.IncrementRunningCount(this.Node))
            {
                Logger.Instance.WarnFormat("MapperId<{0}>\t{1}\t{2}\tCasandra Log Event {4} at {3:yyyy-MM-dd HH:mm:ss,fff} reached the maximum total occurence count. This event will be ignored.",
                                                    this.MapperId,
                                                    this.Node,
                                                    this.File.PathResolved,
                                                    logMessage.LogDateTime,
                                                    matchItem.Item5.EventClass);
                return null;
            }

            var logProperties = new Dictionary<string, object>();

            if(matchItem.Item2 != null)
            {
                UpdateMatchProperties(matchItem.Item1, matchItem.Item2, logProperties);
            }

            if (matchItem.Item4 != null)
            {
                UpdateMatchProperties(matchItem.Item3, matchItem.Item4, logProperties);
            }

            List<LogCassandraEvent> parentEvents = null;
            CLogLineTypeParser.SessionInfo sessionInfo = new CLogLineTypeParser.SessionInfo();
            string sessionId = null;
            LogCassandraEvent sessionEvent = null;
            var eventType = matchItem.Item5.EventType;
            EventClasses eventClass = matchItem.Item5.EventClass;
            var sessionParentAction = matchItem.Item5.SessionParentAction;
            bool orphanedSession = false;

            UnitOfMeasure duration;
            DateTime? eventDurationTime;
            string logId;
            List<string> sstableFilePaths;            
            IKeyspace primaryKS;
            IDDLStmt primaryDDL;            
            List<IDDLStmt> ddlInstances;
            IEnumerable<INode> assocatedNodes;
            IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges;
            LateDDLResolution? lateDDLresolution;
            string subClass;
            
            this.DetermineProperties(logMessage,
                                        logProperties,
                                        out lateDDLresolution,
                                        out logId,
                                        out subClass,
                                        out primaryKS,
                                        out primaryDDL,
                                        out duration,
                                        out eventDurationTime,
                                        out sstableFilePaths,
                                        out ddlInstances,
                                        out assocatedNodes,
                                        out tokenRanges);
            
            subClass = subClass
                        ?? matchItem.Item5.DetermineSubClass(this.Cluster,
                                                                this.Node,
                                                                primaryKS,
                                                                logProperties,
                                                                logMessage);

            #region Session Instance

            if ((eventType & EventTypes.SessionItem) == EventTypes.SessionItem)
            {
                sessionInfo = matchItem.Item5.DetermineSessionInfo(this.Cluster,
                                                                        this.Node,
                                                                        primaryKS,
                                                                        logProperties,
                                                                        logMessage);

                var eventInfo = sessionInfo.GetLogEvent(this._openSessions, this._lookupSessionLabels);

                sessionEvent = eventInfo.Item1;
                sessionId = eventInfo.Item2;

                if (sessionEvent == null)
                {
                    if ((eventType & EventTypes.SessionBeginOrItem) == EventTypes.SessionBeginOrItem)
                    {
                        eventType &= ~EventTypes.SessionBeginOrItem;
                        eventType |= EventTypes.SessionBegin;
                    }
                    
                    if ((eventType & EventTypes.SessionBegin) == EventTypes.SessionBegin)
                    {
                        sessionId = sessionInfo.SessionId ?? sessionInfo.SessionLookup;
                    }
                    else
                    {
                        if ((eventType & EventTypes.SessionIgnore) == EventTypes.SessionIgnore)
                        {
                            return null;
                        }

                        if ((eventType & EventTypes.SingleInstance) == EventTypes.SingleInstance)
                        {
                            eventType = EventTypes.SingleInstance;
                        }
                        else
                        {
                            orphanedSession = true;
                            eventClass |= EventClasses.Orphaned;
                        }                        
                    }
                }
                else
                {
                    if ((eventType & EventTypes.SingleInstance) == EventTypes.SingleInstance)
                    {
                        eventType &= ~EventTypes.SingleInstance;

                        if (eventType == EventTypes.Unkown)
                            eventType = EventTypes.SessionItem;
                    }

                    if ((eventType & EventTypes.SessionIgnore) == EventTypes.SessionIgnore)
                    {
                        eventType &= ~EventTypes.SessionIgnore;

                        eventType |= EventTypes.SessionItem; //Need to re-add Session Item since it is removed on the ignore
                    }

                    if ((eventType & EventTypes.SessionBeginOrItem) == EventTypes.SessionBeginOrItem)
                    {
                        eventType &= ~EventTypes.SessionBeginOrItem;
                        eventType |= EventTypes.SessionItem;
                    }                    

                    logId = sessionEvent.Id.ToString();

                    if (parentEvents == null)
                    {
                        parentEvents = new List<LogCassandraEvent>();
                    }

                    if (sessionEvent.ParentEvents.HasAtLeastOneElement())
                    {
                        if (sessionParentAction != CLogLineTypeParser.SessionParentActions.IgnoreParents)
                        {
                            parentEvents.AddRange(sessionEvent.ParentEvents.Select(e => (LogCassandraEvent)e.GetValue()));
                        }                                                
                    }

                    if ((sessionEvent.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin)
                    {
                        if (sessionParentAction != CLogLineTypeParser.SessionParentActions.IgnoreCurrent)
                        {
                            parentEvents.Add(sessionEvent);
                        }
                    }
                }
            }

            if(sessionEvent != null
                && primaryDDL == null
                && sessionEvent.Keyspace != null
                && lateDDLresolution.HasValue
                && (lateDDLresolution.Value.KSName == null || sessionEvent.Keyspace.Equals(lateDDLresolution.Value.KSName)))
            {
                primaryKS = sessionEvent.Keyspace;

                ddlInstances = lateDDLresolution.Value.DDLNames.SelectMany(n => Cluster.TryGetTableIndexViewsbyString(n, this.Cluster, this.DataCenter, primaryKS.Name)).ToList();

                if(lateDDLresolution.Value.DDLSSTableName != null)
                {
                    primaryDDL = Cluster.TryGetTableIndexViewbyString(lateDDLresolution.Value.DDLSSTableName, this.Cluster, this.DataCenter, primaryKS.Name);
                }
                lateDDLresolution = null;
            }

            #endregion
            #region Log Level
            switch (logMessage.Level)
            {
                case LogLevels.Unknown:
                    break;
                case LogLevels.Debug:
                    break;
                case LogLevels.Info:
                    eventClass |= EventClasses.Information;
                    break;
                case LogLevels.Warn:
                    eventClass |= EventClasses.Warning;
                    break;
                case LogLevels.Error:
                    eventClass |= EventClasses.Error;
                    break;
                case LogLevels.Fatal:
                    eventClass |= EventClasses.Fatal;
                    break;
                default:
                    break;
            }

            if(logMessage.ExtraMessages != null && logMessage.ExtraMessages.HasAtLeastOneElement())
            {
                eventClass |= EventClasses.Exception;
            }
            #endregion

            LogCassandraEvent logEvent = null;

            #region Generate Log Event
            if (sessionEvent == null)
            {
                if ((eventType & EventTypes.SessionItem) != EventTypes.SessionItem && eventDurationTime.HasValue)
                {
                    logEvent = new LogCassandraEvent(this.File,
                                                        this.Node,
                                                        (uint)logMessage.FileLine,
                                                        eventClass,
                                                        logMessage.LogDateTime,
                                                        logMessage.LogDateTimewTZOffset,
                                                        eventDurationTime.Value,
                                                        logMessage.Message,
                                                        eventType,
                                                        logId,
                                                        subClass,
                                                        parentEvents,
                                                        this.Node.Machine.TimeZone,
                                                        primaryKS,
                                                        primaryDDL,
                                                        logProperties,
                                                        sstableFilePaths,
                                                        ddlInstances,
                                                        assocatedNodes,
                                                        tokenRanges,
                                                        sessionId,
                                                        matchItem.Item5.Product);
                }
                else if ((eventType & EventTypes.SessionItem) != EventTypes.SessionItem && !duration.NaN)
                {
                    logEvent = new LogCassandraEvent(this.File,
                                                        this.Node,
                                                        (uint)logMessage.FileLine,
                                                        eventClass,
                                                        logMessage.LogDateTime,
                                                        logMessage.LogDateTimewTZOffset,
                                                        duration,
                                                        logMessage.Message,
                                                        eventType,
                                                        logId,
                                                        subClass,
                                                        parentEvents,
                                                        this.Node.Machine.TimeZone,
                                                        primaryKS,
                                                        primaryDDL,
                                                        logProperties,
                                                        sstableFilePaths,
                                                        ddlInstances,
                                                        assocatedNodes,
                                                        tokenRanges,
                                                        sessionId,
                                                        matchItem.Item5.Product);
                }
                else if ((eventType & EventTypes.SessionDefinedByDuration) == EventTypes.SessionDefinedByDuration && (!duration.NaN || eventDurationTime.HasValue))
                {
                    logEvent = new LogCassandraEvent(this.File,
                                                        this.Node,
                                                        (uint)logMessage.FileLine,
                                                        eventClass,
                                                        logMessage.LogDateTime,
                                                        logMessage.LogDateTimewTZOffset,
                                                        eventDurationTime.HasValue ? eventDurationTime.Value : (logMessage.LogDateTime - (TimeSpan) duration),
                                                        logMessage.Message,
                                                        eventType,
                                                        logId,
                                                        subClass,
                                                        parentEvents,
                                                        this.Node.Machine.TimeZone,
                                                        primaryKS,
                                                        primaryDDL,
                                                        logProperties,
                                                        sstableFilePaths,
                                                        ddlInstances,
                                                        assocatedNodes,
                                                        tokenRanges,
                                                        sessionId,
                                                        matchItem.Item5.Product);
                }
                else
                {
                    logEvent = new LogCassandraEvent(this.File,
                                                        this.Node,
                                                        (uint)logMessage.FileLine,
                                                        eventClass,
                                                        logMessage.LogDateTime,
                                                        logMessage.LogDateTimewTZOffset,
                                                        logMessage.Message,
                                                        eventType,
                                                        logId,
                                                        subClass,
                                                        parentEvents,
                                                        this.Node.Machine.TimeZone,
                                                        primaryKS,
                                                        primaryDDL,
                                                        (TimeSpan?)duration,
                                                        logProperties,
                                                        sstableFilePaths,
                                                        ddlInstances,
                                                        assocatedNodes,
                                                        tokenRanges,
                                                        sessionId,
                                                        matchItem.Item5.Product);
                }
            }
            else
            {
                if ((eventType & EventTypes.SessionEnd) == EventTypes.SessionEnd)
                {
                    logEvent = new LogCassandraEvent(this.File,
                                                        this.Node,
                                                        (uint)logMessage.FileLine,
                                                        eventClass,
                                                        logMessage.LogDateTime,
                                                        logMessage.LogDateTimewTZOffset,
                                                        sessionEvent.EventTime,
                                                        logMessage.Message,
                                                        eventType,
                                                        logId,
                                                        subClass,
                                                        parentEvents,
                                                        this.Node.Machine.TimeZone,
                                                        primaryKS,
                                                        primaryDDL,
                                                        logProperties,
                                                        sstableFilePaths,
                                                        ddlInstances,
                                                        assocatedNodes,
                                                        tokenRanges,
                                                        sessionId,
                                                        matchItem.Item5.Product);

                    if (logEvent.ParentEvents.Count() > 1)
                    {
                        ((LogCassandraEvent)(logEvent.ParentEvents
                                            .LastOrDefault(p => p.GetValue().SessionTieOutId == logEvent.SessionTieOutId
                                                                    || (logEvent.SessionTieOutId != null && p.GetValue().SessionTieOutId.StartsWith(logEvent.SessionTieOutId + ','))))
                                            ?.GetValue())
                                            ?.UpdateEndingTimeStamp(logEvent.EventTimeLocal);
                    }
                    ((LogCassandraEvent) (logEvent.ParentEvents
                                                .FirstOrDefault())
                                                ?.GetValue())
                                                ?.UpdateEndingTimeStamp(logEvent.EventTimeLocal);
                }
                else
                {
                    logEvent = new LogCassandraEvent(this.File,
                                                        this.Node,
                                                        (uint)logMessage.FileLine,
                                                        eventClass,
                                                        logMessage.LogDateTime,
                                                        logMessage.LogDateTimewTZOffset,
                                                        logMessage.Message,
                                                        eventType,
                                                        logId,
                                                        subClass,
                                                        parentEvents,
                                                        this.Node.Machine.TimeZone,
                                                        primaryKS,
                                                        primaryDDL,
                                                        (TimeSpan?)duration,
                                                        logProperties,
                                                        sstableFilePaths,
                                                        ddlInstances,
                                                        assocatedNodes,
                                                        tokenRanges,
                                                        sessionId,
                                                        matchItem.Item5.Product);
                }
            }
            #endregion

            if (logEvent != null)
            {
                #region Log Event and Orphan processing
                if ((logEvent.Type & EventTypes.SessionItem) == EventTypes.SessionItem)
                {
                    if (orphanedSession && this._priorSessionLineTick.LogTick == logEvent.EventTimeLocal)
                    {
                        var checkSessionKey = logEvent.SessionTieOutId ?? matchItem.Item5.DetermineSessionKey(this.Cluster,
                                                                                                                this.Node,
                                                                                                                primaryKS,
                                                                                                                logProperties,
                                                                                                                logMessage);

                        if ((this._priorSessionLineTick.CurrentSession.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd
                                && checkSessionKey == this._priorSessionLineTick.CurrentSession.SessionTieOutId)
                        {
                            if((logEvent.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin)
                            {
                                if(this._priorSessionLineTick.CurrentSession.FixSessionEvent(logEvent))
                                {
                                    orphanedSession = false;
                                }
                            }
                            else if(logEvent.FixSessionEvent(this._priorSessionLineTick.CurrentSession))
                            {
                                orphanedSession = false;
                            }
                        }
                    }

                    if ((logEvent.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd)
                    {
                        this._priorSessionLineTick.LogTick = logEvent.EventTimeLocal;
                        this._priorSessionLineTick.CurrentSession = logEvent;
                    }
                }

                if (orphanedSession
                        && (logEvent.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd
                        && logEvent.SessionTieOutId != null)
                {
                    var beginEvent = this._openSessions.LastOrDefault(o => o.Key.StartsWith(logEvent.SessionTieOutId)
                                                                                && (logEvent.Keyspace == null
                                                                                        || (logEvent.TableViewIndex == null
                                                                                                && logEvent.Keyspace == o.Value.Keyspace)
                                                                                        || (logEvent.TableViewIndex != null
                                                                                                && logEvent.TableViewIndex == o.Value.TableViewIndex))).Value;

                    if (beginEvent != null && logEvent.FixSessionEvent(beginEvent))
                    {
                        orphanedSession = false;
                    }
                }

                if (orphanedSession)
                {
                    this._orphanedSessionEvents.Add(logEvent);
                }

                if(lateDDLresolution.HasValue)
                {
                    this._lateResolutionEvents.Add(new Tuple<LogCassandraEvent, LateDDLResolution>(logEvent, lateDDLresolution.Value));
                }
                else if((logEvent.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd)
                {
                    var updateDDLs = this._lateResolutionEvents.Where(i => i.Item1.Id == logEvent.Id);

                    if(updateDDLs.HasAtLeastOneElement())
                    {
                        var defaultKS = logEvent.Keyspace.Name;

                        foreach (var ddlItem in updateDDLs)
                        {
                            if (ddlItem.Item2.KSName == null || ddlItem.Item2.KSName == defaultKS)
                            {
                                var ddls = ddlItem.Item2.DDLNames.SelectMany(n => Cluster.TryGetTableIndexViewsbyString(n, this.Cluster, this.DataCenter, defaultKS)).ToList();
                                IDDLStmt ddl = null;

                                if (ddlItem.Item2.DDLSSTableName != null)
                                {
                                    ddl = Cluster.TryGetTableIndexViewbyString(ddlItem.Item2.DDLSSTableName, this.Cluster, this.DataCenter, defaultKS);
                                }
                                ddlItem.Item1.UpdateDDLItems(logEvent.Keyspace, ddl, ddls);
                            }
                        }
                        this._lateResolutionEvents.RemoveAll(i => i.Item1.Id == logEvent.Id);
                    }
                }

                #endregion

                #region Session Key and Lookup addition/removal

                var prevLogEvent = sessionInfo.UpdateSessionStatus(this._openSessions, this._lookupSessionLabels, logEvent);

                if(prevLogEvent != null
                    && !prevLogEvent.EventTimeEnd.HasValue
                    && !this._unsedOpenSessionEvents.Any(s => s.Id == prevLogEvent.Id))
                {
                    this._unsedOpenSessionEvents.Add(prevLogEvent);
                }

                #endregion
            }

            return logEvent;
        }

        private void DetermineProperties(ILogMessage logMessage,
                                            Dictionary<string, object> logProperties,
                                            out LateDDLResolution? lateDDLresolution,
                                            out string logId,
                                            out string subClass,                                            
                                            out IKeyspace primaryKS,
                                            out IDDLStmt primaryDDL,
                                            out UnitOfMeasure duration,
                                            out DateTime? eventDurationTime,
                                            out List<string> sstableFilePaths,
                                            out List<IDDLStmt> ddlInstances,
                                            out IEnumerable<INode> assocatedNodes,
                                            out IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges)
                                            //out string sessionId)
        {            
            var keyspaceName = StringHelpers.RemoveQuotes((string)logProperties.TryGetValue("KEYSPACE"));
            var ddlName = StringHelpers.RemoveQuotes((string)logProperties.TryGetValue("DDLITEMNAME") ?? (string)logProperties.TryGetValue("TABLEVIEWNAME"));
            var ddlSchemaId = (string)logProperties.TryGetValue("DDLSCHEMAID");
            var sstableFilePath = StringHelpers.RemoveQuotes((string)logProperties.TryGetValue("SSTABLEPATH"));
            var keyspaceNames = TurnPropertyIntoCollection(logProperties, "KEYSPACES");
            var ddlNames = TurnPropertyIntoCollection(logProperties, "DDLITEMNAMES");
            var solrDDLNames = TurnPropertyIntoCollection(logProperties, "SOLRINDEXNAME");            
            IEnumerable<IKeyspace> keyspaceInstances = keyspaceNames == null
                                                            ? null
                                                            : keyspaceNames
                                                                    .Select(k => this._dcKeyspaces.FirstOrDefault(ki => ki.Equals(k)))
                                                                    .Where(k => k != null)
                                                                    .DuplicatesRemoved(k => k.FullName);
            object objDuration = logProperties.TryGetValue("DURATION");
            duration = objDuration is UnitOfMeasure
                            ? (UnitOfMeasure)objDuration
                            : (objDuration?.IsNumber() ?? false
                                ? new UnitOfMeasure((dynamic)objDuration, UnitOfMeasure.Types.MS)
                                : (objDuration is string
                                    ? UnitOfMeasure.Create((string)objDuration)
                                    : UnitOfMeasure.NaNValue));
            eventDurationTime = logProperties.TryGetValue("EVENTDURATIONDATETIME") as DateTime?;

            logId = logProperties.TryGetValue("ID")?.ToString();
            subClass = logProperties.TryGetValue("SUBCLASS")?.ToString();
            sstableFilePaths = TurnPropertyIntoCollection(logProperties, "SSTABLEPATHS");
            primaryKS = null;
            primaryDDL = Cluster.TryGetTableIndexViewbyString(sstableFilePath ?? ddlName, this.Cluster, this.DataCenter, keyspaceName);
            ddlInstances = null;
            assocatedNodes = null;
            tokenRanges = null;            
            lateDDLresolution = null;

            #region Determine Keyspace/DDLs
            {
                IEnumerable<IDDLStmt> sstableDDLInstance = null;

                if (primaryDDL != null)
                {
                    primaryKS = primaryDDL.Keyspace;
                }
                else if (primaryKS == null && !string.IsNullOrEmpty(keyspaceName))
                {
                    primaryKS = this._dcKeyspaces.FirstOrDefault(ki => ki.Equals(keyspaceName));
                }

                if (!string.IsNullOrEmpty(sstableFilePath))
                {
                    if (sstableFilePaths == null)
                    {
                        sstableFilePaths = new List<string>() { sstableFilePath };
                    }
                    else
                    {
                        if (!sstableFilePaths.Contains(sstableFilePath))
                        {
                            sstableFilePaths.Add(sstableFilePath);
                        }
                    }
                }

                if (sstableFilePaths != null && sstableFilePaths.HasAtLeastOneElement())
                {
                    sstableFilePaths = sstableFilePaths.Select(p => StringHelpers.RemoveQuotes(p)).DuplicatesRemoved(p => p).ToList();

                    var ddlItems = sstableFilePaths.SelectMany(i => Cluster.TryGetTableIndexViewsbyString(i, this.Cluster, this.DataCenter))
                                                    .DuplicatesRemoved(d => d.FullName);

                    if (ddlItems.HasAtLeastOneElement())
                    {
                        if (ddlInstances == null || ddlInstances.IsEmpty())
                        {
                            ddlInstances = ddlItems.ToList();
                        }
                        else
                        {
                            ddlInstances.AddRange(ddlItems);
                        }

                        sstableDDLInstance = ddlItems;
                    }
                }

                if (!string.IsNullOrEmpty(ddlName))
                {
                    if (ddlNames == null || ddlNames.IsEmpty())
                    {
                        ddlNames = new List<string>() { ddlName };
                    }
                    else
                    {
                        ddlNames.Add(ddlName);
                    }
                }

                if (solrDDLNames != null && solrDDLNames.HasAtLeastOneElement())
                {
                    var ddlItems = solrDDLNames.Select(i => Cluster.TryGetSolrIndexbyString(i, this.Cluster, this.DataCenter))
                                                    .Where(i => i != null)
                                                    .DuplicatesRemoved(d => d.FullName);

                    if (ddlItems.HasAtLeastOneElement())
                    {
                        if (ddlInstances == null || ddlInstances.IsEmpty())
                        {
                            ddlInstances = ddlItems.ToList();
                        }
                        else
                        {
                            ddlInstances.AddRange(ddlItems);

                            ddlInstances = ddlInstances.DuplicatesRemoved(d => d.FullName).ToList();
                        }
                    }

                    if (ddlItems.Count() == 1
                            && (primaryDDL == null || primaryDDL.Equals(((ICQLIndex)ddlItems.First()).Table)))
                    {
                        primaryDDL = ddlItems.First();
                        primaryKS = primaryDDL.Keyspace;
                    }
                    else if (primaryDDL == null)
                    {
                       var commonTbl = ((ICQLIndex)ddlInstances.First()).Table;

                        if (ddlInstances.Select(i => ((ICQLIndex)i).Table).All(t => t.Equals(commonTbl)))
                        {
                            primaryDDL = commonTbl;
                            primaryKS = commonTbl.Keyspace;
                        }                        
                    }
                }

                if (ddlNames != null && ddlNames.HasAtLeastOneElement())
                {
                    var ddlItems = ddlNames.SelectMany(i => Cluster.TryGetTableIndexViewsbyString(i, this.Cluster, this.DataCenter))
                                                    .DuplicatesRemoved(d => d.FullName);

                    if (ddlItems.HasAtLeastOneElement())
                    {
                        if (ddlInstances == null || ddlInstances.IsEmpty())
                        {
                            ddlInstances = ddlItems.ToList();
                        }
                        else
                        {
                            ddlInstances.AddRange(ddlItems);

                            ddlInstances = ddlInstances.DuplicatesRemoved(d => d.FullName).ToList();
                        }
                    }
                }

                if (!string.IsNullOrEmpty(ddlSchemaId))
                {
                    var ddlItem = this.Cluster.Keyspaces.SelectMany(k => k.DDLs).FirstOrDefault(ddl => ddl is ICQLTable && ((ICQLTable)ddl).Id.ToString() == ddlSchemaId) as IDDLStmt;

                    if (ddlItem != null)
                    {
                        if (ddlInstances == null || ddlInstances.IsEmpty())
                        {
                            ddlInstances = new List<IDDLStmt>() { ddlItem };
                        }
                        else if (!ddlInstances.Exists(ddl => ddl.FullName == ddlItem.FullName))
                        {
                            ddlInstances.Add(ddlItem);
                        }
                    }
                }
                
                if (primaryKS == null
                        && ddlInstances != null
                        && ddlInstances.HasAtLeastOneElement())
                {
                    if (ddlInstances.Count() == 1)
                    {
                        primaryDDL = ddlInstances.First();
                        primaryKS = primaryDDL.Keyspace;
                    }
                    else
                    {
                        var possibleKS = ddlInstances.First().Keyspace;

                        if (ddlInstances.All(d => d.Keyspace.Equals(possibleKS)))
                        {
                            primaryKS = possibleKS;
                        }
                    }
                }

                if (primaryKS == null
                        && keyspaceInstances != null
                        && keyspaceInstances.HasAtLeastOneElement())
                {
                    var possibleKS = keyspaceInstances.First();

                    if (keyspaceInstances.All(d => d.Equals(possibleKS)))
                    {
                        primaryKS = possibleKS;
                    }
                }

                var ddlInstancesCnt = ddlInstances == null ? 0 : ddlInstances.Count();
                var ddlNamesCnt = (ddlNames == null ? 0 : ddlNames.Count())
                                    + (solrDDLNames == null ? 0 : solrDDLNames.Count)
                                    + (sstableDDLInstance == null ? 0 : sstableDDLInstance.Count());

                if (ddlInstancesCnt != ddlNamesCnt)
                {
                    if (primaryKS == null && ddlInstancesCnt > ddlNamesCnt)
                    {
                        var names = new List<string>();
                        if (ddlNames != null)
                        {
                            names.AddRange(ddlNames);
                        }
                        if (solrDDLNames != null)
                        {
                            names.AddRange(solrDDLNames);
                        }

                        lateDDLresolution = new LateDDLResolution()
                        {
                            KSName = keyspaceName,
                            DDLSSTableName = sstableFilePath ?? ddlName,
                            DDLNames = names
                        };
                        ddlInstances = null;
                    }
                    else
                    {
                        var localPrimaryKS = primaryKS;
                        var instanceNames = ddlInstances?.Select(d => localPrimaryKS == null ? d.Name : d.FullName);
                        var names = new List<string>();

                        if (ddlNames != null)
                        {
                            names.AddRange(primaryKS == null ? ddlNames : ddlNames.Select(s => s.Contains('.') ? s : localPrimaryKS.Name + '.' + s));
                        }
                        if (solrDDLNames != null)
                        {
                            names.AddRange(primaryKS == null ? solrDDLNames : solrDDLNames.Select(s => s.Contains('.') ? s : localPrimaryKS.Name + '.' + s));
                        }
                        if (sstableFilePaths != null)
                        {
                            names.AddRange(sstableDDLInstance.Select(d => localPrimaryKS == null ? d.Name : d.FullName));
                        }

                        var diffNames = ddlInstancesCnt > ddlNamesCnt
                                            ? instanceNames.Complement(names)
                                            : names.Complement(instanceNames ?? Enumerable.Empty<string>());

                        if (diffNames.IsEmpty())
                        {
                            diffNames = names;
                        }

                        Logger.Instance.ErrorFormat("MapperId<{0}>\t{1}\t{2}\tCasandra Log Event at {3:yyyy-MM-dd HH:mm:ss,fff} has mismatch between parsed C* objects from the log vs. DDL C* object instances. There are {4}. They are {{{5}}}. Log line is \"{6}\". DDLItems property for the log instance may contain invalid DDL instances.",
                                                    this.MapperId,
                                                    this.Node,
                                                    this.File.PathResolved,
                                                    logMessage.LogDateTime,
                                                    ddlInstancesCnt > ddlNamesCnt ? "multiple resolved C* object (DDL) instances" : "unresolved C* object names",
                                                    string.Join(", ", diffNames),
                                                    logMessage.Message);
                    }
                }
            }
            #endregion
            #region Assocated Nodes
            {
                var nodeItem = logProperties.TryGetValue("NODE");                
                var assocNodes = TurnPropertyIntoObjectCollection(logProperties, "NODES", true);

                if (nodeItem != null)
                {
                    if (nodeItem is IEnumerable<object>)
                    {
                        assocNodes.AddRange((IEnumerable<object>)nodeItem);
                    }
                    else
                    {
                        assocNodes.Add(nodeItem);
                    }
                }

                if(assocNodes.HasAtLeastOneElement())
                {
                    assocatedNodes = assocNodes.Select(n =>
                                    {
                                        if (n is System.Net.IPAddress)
                                            return nodeItem.ToString();
                                        else if (n is System.Net.IPEndPoint)
                                            return ((System.Net.IPEndPoint)n).Address.ToString();

                                        return n.ToString();
                                    })
                                    .Where(n => !this.Node.Equals(n))
                                    .Select(n => this.Cluster.TryGetNode(n))                                    
                                    .DuplicatesRemoved(n => n);
                }                
            }
            #endregion
            #region Tokens
            {
                var tokenRangeStrings = TurnPropertyIntoCollection(logProperties, "TOKENRANGE");
                var tokenStartStrings = TurnPropertyIntoCollection(logProperties, "TOKENSTART");
                var tokenEndStrings = TurnPropertyIntoCollection(logProperties, "TOKENEND");
                var tokenList = tokenRangeStrings != null || tokenStartStrings != null || tokenEndStrings != null ? new List<DSEInfo.TokenRangeInfo>() : null;

                if (tokenRangeStrings != null)
                {
                    tokenList.AddRange(tokenRangeStrings.Select(t => new DSEInfo.TokenRangeInfo(t)));
                }
                if (tokenStartStrings != null || tokenEndStrings != null)
                {
                    if (tokenStartStrings == null
                        || tokenEndStrings == null
                        || tokenStartStrings.Count() != tokenEndStrings.Count())
                    {
                        throw new ArgumentException(string.Format("Invalid Token Start/End pairs (Start: {0}, End: {1}) for CLogLineTypeParser RegEx Capture Groups.",
                                                                    tokenStartStrings == null ? null : string.Join(",", tokenStartStrings),
                                                                    tokenEndStrings == null ? null : string.Join(",", tokenEndStrings)));
                    }
                    for (int nIdx = 0; nIdx < tokenStartStrings.Count(); ++nIdx)
                    {
                        tokenList.Add(new DSEInfo.TokenRangeInfo(tokenStartStrings.ElementAt(nIdx), tokenEndStrings.ElementAt(nIdx)));
                    }
                }
                tokenRanges = tokenList;
            }
            #endregion

            if(Logger.Instance.IsDebugEnabled)
            {
                if(primaryDDL == null && (ddlName != null || sstableFilePath != null || ddlSchemaId != null))
                {
                    Logger.Instance.ErrorFormat("Log Event could not determine the primary DDL since DDLITEMNAME/TABLEVIEWNAME ({0}), SSTABLEPATH ({1}), or DDLSCHEMAID ({2}) was not found. File: {3}\r\nLog Message: {4}",
                                                    ddlName,
                                                    sstableFilePath,
                                                    ddlSchemaId,
                                                    this.File,
                                                    logMessage);
                    this.NbrErrors++;
                }

                if (primaryKS == null && keyspaceName != null)
                {
                    Logger.Instance.ErrorFormat("Log Event could not determine the primary Keyspace since KEYSPACE ({0}) was not found. File: {1}\r\nLog Message: {2}",
                                                    keyspaceName,
                                                    this.File,
                                                    logMessage);
                    this.NbrErrors++;
                }
            }
        }

        private static void UpdateMatchProperties(Regex matchRegEx, Match matchItem, Dictionary<string, object> logProperties, bool appendToPropertyValueOnDupKey = false)
        {
            string normalizedGroupName;
            string uomType;
            var groupNames = matchRegEx.GetGroupNames();
            bool processedKeyValues = false;
            bool processedKeyItemValues = false;

            foreach (var groupName in groupNames)
            {
                if (groupName != "0")
                {
                    if (groupName == "KEY" || groupName == "VALUE")
                    {
                        #region KEY and VALUE groups
                        if (!processedKeyValues)
                        {
                            var groupKeyInstance = matchItem.Groups["KEY"];
                            var groupValueInstance = matchItem.Groups["VALUE"];

                            if (groupKeyInstance.Success && groupValueInstance.Success)
                            {
                                var groupMaxLength = Math.Min(groupKeyInstance.Captures.Count, groupValueInstance.Captures.Count);

                                processedKeyValues = true;

                                for (int nIdx = 0; nIdx < groupMaxLength; ++nIdx)
                                {
                                    normalizedGroupName = groupKeyInstance.Captures[nIdx].Value;
                                    NormalizedGroupName(normalizedGroupName, logProperties, out uomType);

                                    logProperties.TryAddValue(normalizedGroupName,
                                                                StringHelpers.DetermineProperObjectFormat(groupValueInstance.Captures[nIdx].Value,
                                                                                                            false,
                                                                                                            false,
                                                                                                            true,
                                                                                                            uomType,
                                                                                                            true));
                                }
                            }
                        }
                        #endregion
                    }
                    else if (groupName.StartsWith("KEYITEM") || groupName.StartsWith("ITEM"))
                    {
                        #region KEYITEM and ITEM groups
                        if (!processedKeyItemValues)
                        {
                            processedKeyItemValues = true;

                            var keylistNames = groupNames.Where(g => g.StartsWith("KEYITEM"))
                                                    .Select(g => new { GroupName = g, MatchName = g.Substring(3) });
                            var itemlistNames = from gn in groupNames.Where(g => g.StartsWith("ITEM"))
                                                let gnMatchNamePos = gn.IndexOf('-')
                                                let gnItemKeyName = gnMatchNamePos < 0 ? gn : gn.Substring(0, gnMatchNamePos)
                                                let keyGroupByName = keylistNames.FirstOrDefault(k => gnMatchNamePos >= 0
                                                                                                        ? gnItemKeyName == k.MatchName
                                                                                                        : gnItemKeyName.StartsWith(k.MatchName))
                                                group gn by keyGroupByName into g
                                                select new { Key = g.Key, Items = g.Select(n => n) };

                            foreach (var groupItem in itemlistNames)
                            {
                                var captureItems = new List<CaptureCollection>();
                                var groupKeyInstance = matchItem.Groups[groupItem.Key.GroupName];

                                if (groupKeyInstance.Success)
                                {
                                    foreach (var itemName in groupItem.Items)
                                    {
                                        var groupValueInstance = matchItem.Groups[itemName];

                                        if (groupValueInstance.Success)
                                        {
                                            captureItems.Add(groupValueInstance.Captures);
                                        }
                                    }
                                }

                                if(captureItems.Count == 1)
                                {
                                    var itemList = new List<object>();
                                    var captureCollection = captureItems.First();

                                    for (int nIdx = 0; nIdx < captureCollection.Count; ++nIdx)
                                    {
                                        itemList.Add(StringHelpers.DetermineProperObjectFormat(captureCollection[nIdx].Value,
                                                                                                false,
                                                                                                false,
                                                                                                true,
                                                                                                null,
                                                                                                true));
                                    }

                                    normalizedGroupName = groupKeyInstance.Value;

                                    logProperties.TryAddValue(normalizedGroupName, itemList);
                                }
                                else if(captureItems.Count > 1)
                                {
                                    var keyGroupsList = groupKeyInstance.Captures.Count > 1 ? null : new List<List<List<object>>>();
                                    var groupMaxLength = captureItems.Min(c => c.Count);
                                    var keyList = new List<List<object>>();
                                    var breakItemValue = captureItems[0][0].Value;
                                    var breakCnt = 0;

                                    for (int nIdx = 0; nIdx < groupMaxLength; ++nIdx)
                                    {
                                        if (nIdx > 0 && breakItemValue == captureItems[0][nIdx].Value)
                                        {
                                            if (keyGroupsList == null)
                                            {
                                                if (!logProperties.TryAddValue(groupKeyInstance.Captures[breakCnt].Value, keyList))
                                                {
                                                    logProperties.Add(groupKeyInstance.Captures[breakCnt].Value + breakCnt.ToString(), keyList);
                                                }
                                            }
                                            else
                                            {
                                                keyGroupsList.Add(keyList);
                                            }
                                            keyList = new List<List<object>>();
                                            ++breakCnt;
                                        }

                                        var itemList = new List<object>();

                                        foreach (var itemCollection in captureItems)
                                        {
                                            itemList.Add(itemCollection[nIdx].Value);
                                        }


                                        keyList.Add(itemList);
                                    }

                                    if (keyGroupsList == null)
                                    {
                                        if (!logProperties.TryAddValue(groupKeyInstance.Captures[breakCnt].Value, keyList))
                                        {
                                            logProperties.Add(groupKeyInstance.Captures[breakCnt].Value + breakCnt.ToString(), keyList);
                                        }
                                    }
                                    else
                                    {
                                        logProperties.Add(groupKeyInstance.Value, keyGroupsList);
                                    }
                                }
                            }
                        }
                        #endregion
                    }
                    else
                    {
                        #region Single Value or common Groups
                        normalizedGroupName = NormalizedGroupName(groupName, logProperties, out uomType);

                        var groupInstance = matchItem.Groups[groupName];

                        if (groupInstance.Success)
                        {
                            if (groupInstance.Captures.Count <= 1)
                            {
                                if (appendToPropertyValueOnDupKey)
                                {
                                    var objValue = StringHelpers.DetermineProperObjectFormat(groupInstance.Value,
                                                                                                true,
                                                                                                false,
                                                                                                true,
                                                                                                uomType,
                                                                                                true);
                                    if(logProperties.ContainsKey(normalizedGroupName))
                                    {
                                        var currentValue = logProperties[normalizedGroupName];

                                        if(currentValue is IList<object>)
                                        {
                                            if(!((IList<object>) currentValue).Contains(objValue))
                                                ((IList<object>)currentValue).Add(objValue);
                                        }
                                        else if(currentValue is IEnumerable<object>)
                                        {
                                            if(!((IEnumerable<object>)currentValue).Contains(objValue))
                                                logProperties[normalizedGroupName] = new List<object>((IEnumerable<object>) currentValue) { objValue };
                                        }
                                        else if(!currentValue.Equals(objValue))
                                        {
                                            logProperties[normalizedGroupName] = new List<object>() { currentValue, objValue };
                                        }
                                    }
                                    else
                                    {
                                        logProperties.TryAddValue(normalizedGroupName, objValue);
                                    }
                                }
                                else
                                {
                                    logProperties.TryAddValue(normalizedGroupName,
                                                                StringHelpers.DetermineProperObjectFormat(groupInstance.Value,
                                                                                                            true,
                                                                                                            false,
                                                                                                            true,
                                                                                                            uomType,
                                                                                                            true));
                                }
                            }
                            else
                            {
                                var groupCaptures = new List<object>(groupInstance.Captures.Count);

                                foreach (Capture capture in groupInstance.Captures)
                                {
                                    groupCaptures.Add(StringHelpers.DetermineProperObjectFormat(capture.Value,
                                                                                                    true,
                                                                                                    false,
                                                                                                    true,
                                                                                                    uomType,
                                                                                                    true));
                                }

                                logProperties.TryAddValue(normalizedGroupName, groupCaptures);
                            }
                        }
                        #endregion
                    }
                }
            }
        }

        private static string NormalizedGroupName(string groupName, Dictionary<string, object> logProperties, out string uomType)
        {
            var groupNamePos = groupName.IndexOf('_');
            string normalizedGroupName;

            if (groupNamePos > 0
                    && groupName[groupNamePos + 1] == '_')
            {
                normalizedGroupName = groupName.Substring(0, groupNamePos);
                var lookupVar = groupName.Substring(groupNamePos + 2);
                object lookupValue;

                if (logProperties.TryGetValue(lookupVar, out lookupValue)
                        && lookupValue is UnitOfMeasure)
                {
                    uomType = ((UnitOfMeasure)lookupValue).UnitType.ToString();
                }
                else
                {
                    uomType = null;
                }
            }
            else if (groupNamePos > 0)
            {
                normalizedGroupName = groupName.Substring(0, groupNamePos);
                uomType = groupName.Substring(groupNamePos + 1);
            }
            else
            {
                normalizedGroupName = groupName;
                uomType = null;
            }

            return normalizedGroupName;
        }

        private static List<string> TurnPropertyIntoCollection(Dictionary<string, object> logProperties, string key)
        {
            var propValue = logProperties.TryGetValue(key);

            if(propValue != null)
            {
                if(propValue is string)
                {
                    return new List<string>() { (string)propValue };
                }
                else if(propValue is IEnumerable<object>)
                {
                    return ((IEnumerable<object>)propValue).Select(o => o.ToString()).ToList();
                }

                return new List<string>() { propValue.ToString() };
            }

            return null;
        }

        private static List<object> TurnPropertyIntoObjectCollection(Dictionary<string, object> logProperties, string key, bool returnEmptyList = false)
        {
            var propValue = logProperties.TryGetValue(key);

            if (propValue != null)
            {
                if (propValue is IEnumerable<object>)
                {
                    return ((IEnumerable<object>)propValue).ToList();
                }

                return new List<object>() { propValue };
            }

            return returnEmptyList ? new List<object>() : null;
        }

        static public readonly HashSet<string> LogLinesHash = new HashSet<string>();
        static public long NbrDuplicatedLogEventsTotal = 0;
        private readonly string _nodeStrId;

        private bool DuplicateLogEventFound(ILogMessage logMessage)
        {
            if (LibrarySettings.DetectDuplicatedLogEvents)
            {
               return DuplicateLogEventFound(this._nodeStrId
                                                + '|'
                                                + logMessage.FileName
                                                + '|'
                                                + logMessage.FileLine.ToString()
                                                + '|'
                                                + logMessage.LogDateTime.Ticks.ToString()
                                                + '|'
                                                + (logMessage.Message?.GetHashCode().ToString() ?? string.Empty));
            }

            return false;
        }

        private static bool DuplicateLogEventFound(string checkTag)
        {
            if (LogLinesHash.Contains(checkTag))
            {
                return true;
            }

            lock (LogLinesHash)
            {
                return !LogLinesHash.Add(checkTag);
            }
        }


    }
}
