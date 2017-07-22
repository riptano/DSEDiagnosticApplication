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

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class file_cassandra_log4net : DiagnosticFile
    {
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

        public static readonly CTS.List<LogCassandraEvent> OrphanedSessionEvents = new CTS.List<LogCassandraEvent>();

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
                this._eventList = fileInstance._logEvents;
                this.OrphanedSessionEvents = fileInstance._orphanedSessionEvents;
            }

            [JsonProperty(PropertyName="Events")]
            private readonly List<LogCassandraEvent> _eventList;
            public readonly IEnumerable<LogCassandraEvent> OrphanedSessionEvents;

            #region IResult
            [JsonConverter(typeof(DSEDiagnosticLibrary.IPathJsonConverter))]
            public IPath Path { get; private set; }
            [JsonIgnore]
            public Cluster Cluster { get { return this.DataCenter?.Cluster; } }
            [JsonIgnore]
            public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
            public INode Node { get; private set; }
            public int NbrItems { get { return this._eventList.Count; } }

            public IEnumerable<IParsed> Results { get { return this._eventList; } }

            #endregion
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

        public override uint ProcessFile()
        {
            this.CancellationToken.ThrowIfCancellationRequested();

            using (var logFileInstance = new ReadLogFile(this.File, LibrarySettings.Log4NetConversionPattern, this.Node))
            {
               System.Threading.Tasks.Task.Factory.StartNew(() =>
                                                                logFileInstance.ProcessLogFile().Wait(),
                                                            this.CancellationToken).Wait();

                var logMessages = logFileInstance.Log;
                this.NbrItemsParsed = unchecked((int)logFileInstance.LinesRead);

                Tuple<Regex, Match, Regex, Match, CLogLineTypeParser> matchItem;
                LogCassandraEvent logEvent;

                {
                    var firstMsgTime = logMessages.Messages.FirstOrDefault()?.LogDateTime;

                    if(firstMsgTime.HasValue)
                    {
                        var possibleOpenSessions = OrphanedSessionEvents.Where(o => (o.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin
                                                                                    && o.EventTimeLocal <= firstMsgTime.Value)
                                                                        .OrderBy(o => o.EventTimeLocal);

                        possibleOpenSessions.ForEach(o => this._openSessions[o.SessionTieOutId] = o);
                    }
                }

                foreach (var logMessage in logMessages.Messages)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    matchItem = CLogTypeParser.FindMatch(this._parser, logMessage);

                    if(matchItem == null)
                    {
                        if(logMessage.Level == LogLevels.Error)
                        {
                            logEvent = this.PorcessUnhandledError();

                            if(logEvent != null)
                                this._logEvents.Add(logEvent);
                        }
                        else if(logMessage.Level == LogLevels.Warn)
                        {
                            logEvent = this.PorcessUnhandledWarning();

                            if(logEvent != null)
                                this._logEvents.Add(logEvent);
                        }
                    }
                    else if(matchItem.Item5.IgnoreEvent)
                    {
                        continue;
                    }
                    else
                    {
                        logEvent = this.PorcessMatch(logMessage, matchItem);

                        if(logEvent != null)
                            this._logEvents.Add(logEvent);
                    }

                    if(logMessage.ExtraMessages.HasAtLeastOneElement())
                    {
                        this.PorcessException();
                    }
                }
            }

            this._orphanedSessionEvents.AddRange(this._unsedOpenSessionEvents.Where(s => !s.EventTimeEnd.HasValue).Select(s => { s.MarkAsOrphaned(); return s; }));
            this._orphanedSessionEvents.AddRange(this._openSessions.Where(s => !s.Value.EventTimeEnd.HasValue).Select(s => { s.Value.MarkAsOrphaned(); return s.Value; }));

            OrphanedSessionEvents.AddRange(this._orphanedSessionEvents);

            this._lookupSessionLabels.Clear();
            this._openSessions.Clear();
            this._unsedOpenSessionEvents.Clear();

            return (uint) this._logEvents.Count;
        }

        private LogCassandraEvent PorcessUnhandledError()
        {
            return null;
        }

        private LogCassandraEvent PorcessUnhandledWarning()
        {
            return null;
        }

        private LogCassandraEvent PorcessException()
        {
            return null;
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
            var logProperties = new Dictionary<string, object>();

            if(matchItem.Item2 != null)
            {
                UpdateMatchProperties(matchItem.Item1, matchItem.Item2, logProperties);
            }

            if (matchItem.Item4 != null)
            {
                UpdateMatchProperties(matchItem.Item3, matchItem.Item4, logProperties);
            }

            string logId = logProperties.TryGetValue("ID")?.ToString();
            object objDuration = logProperties.TryGetValue("DURATION");
            UnitOfMeasure duration = objDuration is UnitOfMeasure
                                        ? (UnitOfMeasure) objDuration
                                        : (objDuration?.IsNumber() ?? false
                                            ? new UnitOfMeasure((dynamic)objDuration, UnitOfMeasure.Types.MS)
                                            : (objDuration is string
                                                ? UnitOfMeasure.Create((string) objDuration)
                                                : null));
            DateTime? eventDurationTime = logProperties.TryGetValue("EVENTDURATIONDATETIME") as DateTime?;

            var keyspaceName = StringHelpers.RemoveQuotes((string) logProperties.TryGetValue("KEYSPACE"));
            var ddlName = StringHelpers.RemoveQuotes((string)logProperties.TryGetValue("DDLITEMNAME") ?? (string)logProperties.TryGetValue("TABLEVIEWNAME"));
            var sstableFilePath = StringHelpers.RemoveQuotes((string) logProperties.TryGetValue("SSTABLEPATH"));
            var keyspaceNames = TurnPropertyIntoCollection(logProperties, "KEYSPACES");
            var ddlNames = TurnPropertyIntoCollection(logProperties, "DDLITEMNAMES");
            var solrDDLNames = TurnPropertyIntoCollection(logProperties, "SOLRINDEXNAME");
            var sstableFilePaths = TurnPropertyIntoCollection(logProperties, "SSTABLEPATHS");
            EventClasses eventClass = matchItem.Item5.EventClass;

            IKeyspace primaryKS = null;
            IDDLStmt primaryDDL = Cluster.TryGetTableIndexViewbyString(sstableFilePath ?? ddlName, this.Cluster, this.DataCenter, keyspaceName);
            IEnumerable<IKeyspace> keyspaceInstances = keyspaceNames == null
                                                            ? null
                                                            : keyspaceNames
                                                                    .Select(k => this._dcKeyspaces.FirstOrDefault(ki => ki.Equals(k)))
                                                                    .Where(k => k != null)
                                                                    .DuplicatesRemoved(k => k.FullName);
            List<IDDLStmt> ddlInstances = null;
            IEnumerable<INode> assocatedNodes = null;
            IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges = null;
            List<LogCassandraEvent> parentEvents = null;
            CLogLineTypeParser.SessionInfo sessionInfo = new CLogLineTypeParser.SessionInfo();
            string sessionId = null;
            LogCassandraEvent sessionEvent = null;
            var eventType = matchItem.Item5.EventType;
            bool orphanedSession = false;
            LateDDLResolution? lateDDLresolution = null;

            #region Determine Keyspace/DDLs
            {
                IEnumerable<IDDLStmt> sstableDDLInstance = null;

                if(primaryDDL != null)
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

                if(!string.IsNullOrEmpty(ddlName))
                {
                    if(ddlNames == null || ddlNames.IsEmpty())
                    {
                        ddlNames = new List<string>() { ddlName };
                    }
                    else
                    {
                        ddlNames.Add(ddlName);
                    }
                }

                if(solrDDLNames != null && solrDDLNames.HasAtLeastOneElement())
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

                if(primaryKS == null
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

                if(primaryKS == null
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
                                    + (sstableDDLInstance == null ? 0 : sstableDDLInstance.Count());

                if (ddlInstancesCnt != ddlNamesCnt)
                {
                    if(primaryKS == null && ddlInstancesCnt > ddlNamesCnt)
                    {
                        lateDDLresolution = new LateDDLResolution()
                        {
                            KSName = keyspaceName,
                            DDLSSTableName = sstableFilePath ?? ddlName,
                            DDLNames = ddlNames
                        };
                        ddlInstances = null;
                    }
                    else
                    {
                        var instanceNames = ddlInstances?.Select(d => primaryKS == null ? d.Name : d.FullName);
                        var names = new List<string>();

                        if(ddlNames != null)
                        {
                            names.AddRange(primaryKS == null ? ddlNames : ddlNames.Select(s => s.Contains('.') ? s : primaryKS.Name + '.' + s));
                        }
                        if (sstableFilePaths != null)
                        {
                            names.AddRange(sstableDDLInstance.Select(d => primaryKS == null ? d.Name : d.FullName));
                        }

                        var diffNames = ddlInstancesCnt > ddlNamesCnt
                                            ? instanceNames.Complement(names)
                                            : names.Complement(instanceNames);

                        if(diffNames.IsEmpty())
                        {
                            diffNames = names;
                        }

                        Logger.Instance.ErrorFormat("{0}\t{1}\tCasandra Log Event at {2:yyyy-MM-dd HH:mm:ss,fff} has {3} DDL statements for {{{4}}} in regards to line \"{5}\". DDLItems property may contain invalid instances.",
                                                    this.Node,
                                                    this.File.PathResolved,
                                                    logMessage.LogDateTime,
                                                    ddlInstancesCnt > ddlNamesCnt ? "multiple resolutions" : "unresolved",
                                                    string.Join(",", diffNames),
                                                    logMessage.Message);
                    }
                }
            }
            #endregion
            #region Assocated Nodes
            {
                var nodeItem = logProperties.TryGetValue("NODE");
                var assocNode = nodeItem is string ? (string) nodeItem : nodeItem?.ToString();
                var assocNodes = TurnPropertyIntoCollection(logProperties, "NODES");

                if (string.IsNullOrEmpty(assocNode))
                {
                    if(assocNodes != null)
                    {
                        assocatedNodes = assocNodes.Select(n => this.Cluster.TryGetNode(n))
                                            .Where(n => n != null)
                                            .DuplicatesRemoved(n => n);
                    }
                }
                else
                {
                    if(assocNodes == null || assocNodes.IsEmpty())
                    {
                        assocatedNodes = new List<INode>() { this.Cluster.TryGetNode(assocNode) };
                    }
                    else
                    {
                        var assocNodesList = new List<string>(assocNodes);
                        assocNodesList.Add(assocNode);

                        assocatedNodes = assocNodesList.Select(n => this.Cluster.TryGetNode(n))
                                            .Where(n => n != null)
                                            .DuplicatesRemoved(n => n);
                    }
                }
            }
            #endregion
            #region Tokens
            {
                var tokenRangeStrings = TurnPropertyIntoCollection(logProperties, "TOKENRANGE");
                var tokenStartStrings = TurnPropertyIntoCollection(logProperties, "TOKENSTART");
                var tokenEndStrings = TurnPropertyIntoCollection(logProperties, "TOKENEND");
                var tokenList = tokenRangeStrings != null || tokenStartStrings != null || tokenEndStrings != null ? new List<DSEInfo.TokenRangeInfo>() : null;

                if(tokenRangeStrings != null)
                {
                    tokenList.AddRange(tokenRangeStrings.Select(t => new DSEInfo.TokenRangeInfo(t)));
                }
                if(tokenStartStrings != null || tokenEndStrings != null)
                {
                    if(tokenStartStrings == null
                        || tokenEndStrings == null
                        || tokenStartStrings.Count() != tokenEndStrings.Count())
                    {
                        throw new ArgumentException(string.Format("Invalid Token Start/End pairs (Start: {0}, End: {1}) for CLogLineTypeParser RegEx Capture Groups.",
                                                                    tokenStartStrings == null ? null : string.Join(",", tokenStartStrings),
                                                                    tokenEndStrings == null ? null : string.Join(",", tokenEndStrings)));
                    }
                    for(int nIdx = 0; nIdx < tokenStartStrings.Count(); ++nIdx)
                    {
                        tokenList.Add(new DSEInfo.TokenRangeInfo(tokenStartStrings.ElementAt(nIdx), tokenEndStrings.ElementAt(nIdx)));
                    }
                }
                tokenRanges = tokenList;
            }
            #endregion

            string subClass = matchItem.Item5.DetermineSubClass(this.Cluster,
                                                                            this.Node,
                                                                            primaryKS,
                                                                            logProperties,
                                                                            logMessage)
                                        ?? logProperties.TryGetValue("SUBCLASS")?.ToString();

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

                        orphanedSession = true;
                        eventClass |= EventClasses.Orphaned;
                    }
                }
                else
                {
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
                        parentEvents.AddRange(sessionEvent.ParentEvents.Cast<LogCassandraEvent>());
                    }

                    if ((sessionEvent.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin)
                    {
                        parentEvents.Add(sessionEvent);
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
                else if ((eventType & EventTypes.SessionItem) != EventTypes.SessionItem && duration != null)
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

                    ((LogCassandraEvent)logEvent.ParentEvents.LastOrDefault(p => ((LogCassandraEvent)p).SessionTieOutId == logEvent.SessionTieOutId))?.UpdateEndingTimeStamp(logEvent.EventTimeLocal);
                    ((LogCassandraEvent)logEvent.ParentEvents.FirstOrDefault())?.UpdateEndingTimeStamp(logEvent.EventTimeLocal);
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

        private static void UpdateMatchProperties(Regex matchRegEx, Match matchItem, Dictionary<string, object> logProperties)
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
                                logProperties.TryAddValue(normalizedGroupName,
                                                            StringHelpers.DetermineProperObjectFormat(groupInstance.Value,
                                                                                                        true,
                                                                                                        false,
                                                                                                        true,
                                                                                                        uomType,
                                                                                                        true));
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
    }
}
