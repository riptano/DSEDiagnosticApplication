using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using Common;
using DSEDiagnosticLog4NetParser;
using CTS = Common.Patterns.Collections.ThreadSafe;

namespace DSEDiagnosticFileParser
{
    public sealed class file_cassandra_log4net : DiagnosticFile
    {
        struct CurrentSessionLineTick
        {
            public DateTime LogTick;
            public LogCassandraEvent CurrentSession;
        }

        private CurrentSessionLineTick _priorSessionLineTick;
        private List<LogCassandraEvent> _sessionEvents = new List<LogCassandraEvent>();
        private Dictionary<string,LogCassandraEvent> _openSessions = new Dictionary<string, LogCassandraEvent>();
        private Dictionary<string, LogCassandraEvent> _lookupSessionLabels = new Dictionary<string, LogCassandraEvent>();
        private List<LogCassandraEvent> _orphanedSessionEvents = new List<LogCassandraEvent>();

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
        }

        public sealed class LogResults : IResult
        {
            private LogResults() { }
            internal LogResults(file_cassandra_log4net fileInstance)
            {
                this.Path = fileInstance.File;
                this.Node = fileInstance.Node;
                this._eventList = fileInstance._logEvents;
                this.OrphanedSessionEvents = fileInstance._orphanedSessionEvents;
                this.OpenSessionEvents = fileInstance._openSessions.Select(i => i.Value);
            }

            private readonly List<LogCassandraEvent> _eventList;

            public readonly IEnumerable<LogCassandraEvent> OrphanedSessionEvents;
            public readonly IEnumerable<LogCassandraEvent> OpenSessionEvents;

            #region IResult
            public IPath Path { get; private set; }
            public Cluster Cluster { get { return this.DataCenter?.Cluster; } }
            public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
            public INode Node { get; private set; }
            public int NbrItems { get { return this._eventList.Count; } }

            public IEnumerable<IParsed> Results { get { return this._eventList; } }

            #endregion
        }

        private readonly LogResults _result;

        public Cluster Cluster { get; private set; }
        public IDataCenter DataCenter { get; private set; }
        private IEnumerable<IKeyspace> _dcKeyspaces;

        public override IResult GetResult()
        {
            return this._result;
        }

        CLogLineTypeParser[] _parser = null;
        List<LogCassandraEvent> _logEvents = new List<LogCassandraEvent>();
        public static readonly CTS.List<LogCassandraEvent> OrphanedSessionEvents = new CTS.List<LogCassandraEvent>();

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

            this.CancellationToken.ThrowIfCancellationRequested();

            OrphanedSessionEvents.AddRange(this._orphanedSessionEvents);
            //OrphanedSessionEvents.AddRange(this._openSessions.Select(s => { s.Value.MarkAsOrphaned(); return s.Value; }));

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

            var keyspaceName = (string) logProperties.TryGetValue("KEYSPACE");
            var ddlName = (string)logProperties.TryGetValue("DDLITEMNAME");
            var sstableFilePath = (string) logProperties.TryGetValue("SSTABLEPATH");
            var keyspaceNames = TurnPropertyIntoCollection(logProperties, "KEYSPACES");
            var ddlNames = TurnPropertyIntoCollection(logProperties, "DDLITEMNAMES");
            var sstableFilePaths = TurnPropertyIntoCollection(logProperties, "SSTABLEPATHS")
                                    ?.DuplicatesRemoved(p => p);
            EventClasses eventClass = matchItem.Item5.EventClass;

            IKeyspace primaryKS = string.IsNullOrEmpty(keyspaceName)
                                    ? null
                                    : this._dcKeyspaces.FirstOrDefault(k => k.Equals(keyspaceName));
            IDDLStmt primaryDDL = null;
            IEnumerable<IKeyspace> keyspaceInstances = keyspaceNames == null
                                                            ? null
                                                            : keyspaceNames
                                                                    .Select(k => this._dcKeyspaces.FirstOrDefault(ki => ki.Equals(k)))
                                                                    .Where(k => k != null)
                                                                    .DuplicatesRemoved(k => k.FullName);
            IEnumerable<IDDLStmt> ddlInstances = null;
            IEnumerable<INode> assocatedNodes = null;
            IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges = null;
            IEnumerable<LogCassandraEvent> parentEvents = null; /* this._sessionEvents
                                .Where(e => (e.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin
                                                && (logMessage.LogDateTimewTZOffset >= e.EventTime
                                                        && (!e.EventTimeEnd.HasValue
                                                                || logMessage.LogDateTimewTZOffset <= e.EventTimeEnd.Value)))
                                    .ToList();*/
            CLogLineTypeParser.SessionInfo sessionInfo = new CLogLineTypeParser.SessionInfo();
            string sessionId = null;
            LogCassandraEvent sessionEvent = null;
            var eventType = matchItem.Item5.EventType;
            bool orphanedSession = false;

            #region Determine Keyspace/DDLs

            if (ddlNames != null && ddlNames.HasAtLeastOneElement())
            {
                if (!string.IsNullOrEmpty(ddlName))
                {
                    var ddlNameList = new List<string>(ddlNames);
                    ddlNameList.Add(ddlName);
                    ddlNames = ddlNameList;
                }

                if (primaryKS == null)
                {
                    ddlInstances = ddlNames.SelectMany(d => this.DataCenter.Keyspaces.Select(k => k.TryGetDDL(d)))
                                    .Where(d => d != null)
                                    .DuplicatesRemoved(d => d.FullName);
                }
                else
                {
                    ddlInstances = ddlNames.Select(d => primaryKS.TryGetDDL(d))
                                    .Where(d => d != null)
                                    .DuplicatesRemoved(d => d.FullName);
                }
            }

            if (!string.IsNullOrEmpty(ddlName))
            {
                if (primaryKS == null)
                {
                    var ddls = this.DataCenter.Keyspaces.Select(k => k.TryGetDDL(ddlName))
                                                                .Where(d => d != null)
                                                                .DuplicatesRemoved(d => d.FullName);

                    if(ddlInstances != null)
                    {
                        ddlInstances = ddlInstances.Append(ddls.ToArray()).DuplicatesRemoved(d => d.FullName);
                    }
                    else
                    {
                        ddlInstances = ddls;
                    }
                }
                else
                {
                    var ddlInstance = primaryKS.TryGetDDL(ddlName);

                    if (ddlInstance != null)
                    {
                        if (ddlInstances != null)
                        {
                            ddlInstances = ddlInstances.Append(ddlInstance).DuplicatesRemoved(d => d.FullName);
                        }
                        else
                        {
                            ddlInstances = new List<IDDLStmt>() { ddlInstance };
                        }
                    }
                }

                if (ddlInstances != null && ddlInstances.HasAtLeastOneElement())
                {
                    var possiblePrimaryDDLs = ddlInstances.Where(d => d.Name == ddlName && (primaryKS == null || d.Keyspace.Equals(primaryKS)));

                    if(possiblePrimaryDDLs.Count() == 1)
                    {
                        primaryDDL = possiblePrimaryDDLs.First();
                    }

                    if (primaryKS == null && primaryDDL != null)
                    {
                        primaryKS = primaryDDL.Keyspace;
                    }
                }
            }

            if (primaryDDL != null && primaryKS == null)
            {
                primaryKS = primaryDDL.Keyspace;
            }

            if (primaryKS == null)
            {
                if(ddlInstances != null && ddlInstances.HasAtLeastOneElement())
                {
                    var possibleKS = ddlInstances.First().Keyspace;

                    if(ddlInstances.All(d => d.Keyspace.Equals(possibleKS)))
                    {
                        primaryKS = possibleKS;
                    }
                }

                if(primaryKS == null && keyspaceInstances != null && keyspaceInstances.HasAtLeastOneElement())
                {
                    var fndKS = keyspaceInstances.First();

                    if(keyspaceInstances.All(k => k.Equals(fndKS)))
                    {
                        primaryKS = fndKS;
                    }
                }
            }

            if (!string.IsNullOrEmpty(sstableFilePath))
            {
                if(sstableFilePaths == null)
                {
                    sstableFilePaths = new List<string>() { StringHelpers.RemoveQuotes(sstableFilePath) };
                }
                else
                {
                    if (!sstableFilePaths.Contains(sstableFilePath = StringHelpers.RemoveQuotes(sstableFilePath)))
                    {
                        sstableFilePaths = new List<string>(sstableFilePaths);
                        ((List<string>)sstableFilePaths).Add(sstableFilePath);
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
            #region Determine primary Keyspace (if missing) and/or DDLs from SSTables
            if(sstableFilePaths != null && sstableFilePaths.HasAtLeastOneElement())
            {
                var ddlItems = sstableFilePaths.Select(i => Cluster.TryGetTableIndexViewbyString(i, this.Cluster.Name))
                                                .Where(i => i != null);
                if(ddlItems.HasAtLeastOneElement())
                {
                    if(ddlInstances == null || ddlInstances.IsEmpty())
                    {
                        ddlInstances = ddlItems.DuplicatesRemoved(d => d.FullName);
                    }
                    else
                    {
                        ddlInstances = ddlInstances.Append(ddlItems.ToArray()).DuplicatesRemoved(d => d.FullName);
                    }
                }
            }

            if (ddlInstances != null && ddlInstances.HasAtLeastOneElement())
            {
                if (ddlInstances.Count() == 1)
                {
                    if(primaryKS == null) primaryKS = ddlInstances.First().Keyspace;
                    if(primaryDDL == null) primaryDDL = ddlInstances.First();
                }
                else
                {
                    if (primaryKS == null)
                    {
                        var firstKS = ddlInstances.First()?.Keyspace.FullName;

                        if (ddlInstances.All(k => k.Keyspace.FullName == firstKS))
                        {
                            primaryKS = ddlInstances.First().Keyspace;
                        }
                    }
                }
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

                var eventInfo = this.GetLogEvent(sessionInfo);

                sessionEvent = eventInfo.Item1;
                sessionId = eventInfo.Item2;

                if((eventType & EventTypes.SessionBeginForcePriorEnd) == EventTypes.SessionBeginForcePriorEnd)
                {
                    eventType &= ~EventTypes.SessionBeginForcePriorEnd;
                    eventType |= EventTypes.SessionBegin;

                    if (sessionEvent != null &&
                            (((eventType & EventTypes.SessionBeginOrItem) != EventTypes.SessionBeginOrItem
                                    && (eventType & EventTypes.SessionReset) != EventTypes.SessionReset)
                                || ((sessionEvent.Class & ~EventClasses.StatusTypes) == (eventClass & ~EventClasses.StatusTypes)
                                        && sessionEvent.SubClass == subClass)))
                    {
                        var removeOpenSessions = this._openSessions.Where(kv => kv.Value.Id == sessionEvent.Id).Select(kv => kv.Key).ToArray();
                        removeOpenSessions.ForEach(s => this._openSessions.Remove(s));
                        sessionEvent = null;
                    }

                    eventType &= ~EventTypes.SessionReset;
                }

                if (sessionEvent == null)
                {
                    if((eventType & EventTypes.SessionIgnore) == EventTypes.SessionIgnore)
                    {
                        return null;
                    }

                    if ((eventType & EventTypes.SessionBeginOrItem) == EventTypes.SessionBeginOrItem)
                    {
                        eventType &= ~EventTypes.SessionBeginOrItem;
                        eventType |= EventTypes.SessionBegin;
                    }

                    if ((eventType & EventTypes.SessionBegin) != EventTypes.SessionBegin)
                    {
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

                    if (primaryKS == null) primaryKS = sessionEvent.Keyspace;
                    logId = sessionEvent.Id.ToString();

                    if(parentEvents == null)
                    {
                        parentEvents = new List<LogCassandraEvent>() { sessionEvent };
                    }
                    else
                    {
                        parentEvents = parentEvents.Append(sessionEvent);
                    }
                }
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
                                                        sessionId ?? sessionEvent?.SessionTieOutId,
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
                                                        sessionId ?? sessionEvent?.SessionTieOutId,
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
                                                        sessionId ?? sessionEvent?.SessionTieOutId,
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
                                                        sessionId ?? sessionEvent?.SessionTieOutId,
                                                        matchItem.Item5.Product);

                    sessionEvent.UpdateEndingTimeStamp(logMessage.LogDateTime);
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
                                                        sessionId ?? sessionEvent?.SessionTieOutId,
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

                        if (this._priorSessionLineTick.CurrentSession.Type == EventTypes.SessionEnd
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

                if (orphanedSession)
                {
                    this._orphanedSessionEvents.Add(logEvent);
                }
                #endregion

                #region Session Key and Lookup addition/removal

                this.DetermineSessionStatus(sessionInfo, logEvent);

                #endregion
            }

            return logEvent;
        }

        private Tuple<LogCassandraEvent, string> GetLogEvent(CLogLineTypeParser.SessionInfo logLineParserInfo)
        {
            string sessionKey = null;
            LogCassandraEvent logEvent = null;

            if(logLineParserInfo.SessionId != null
                    && (logLineParserInfo.SessionType == CLogLineTypeParser.SessionKeyTypes.Auto
                            || logLineParserInfo.SessionType == CLogLineTypeParser.SessionKeyTypes.Read))
            {
                logEvent = this._openSessions.TryGetValue(logLineParserInfo.SessionId);
                sessionKey = logEvent == null ? null : logLineParserInfo.SessionId;
            }

            if(logEvent == null)
            {
                switch (logLineParserInfo.LookupType)
                {
                    case CLogLineTypeParser.SessionLookupTypes.Session:
                        if (logLineParserInfo.SessionLookupRegEx == null)
                        {
                            if (logLineParserInfo.SessionLookup != null)
                            {
                                logEvent = this._openSessions.TryGetValue(logLineParserInfo.SessionLookup);
                            }
                        }
                        else
                        {
                            logEvent = this._openSessions.FirstOrDefault(kv => logLineParserInfo.IsLookupMatch(kv.Key)).Value;
                        }
                        break;
                    case CLogLineTypeParser.SessionLookupTypes.ReadLabel:
                    case CLogLineTypeParser.SessionLookupTypes.ReadRemoveLabel:
                        if (logLineParserInfo.SessionLookupRegEx == null)
                        {
                            if (logLineParserInfo.SessionLookup != null)
                            {
                                logEvent = this._lookupSessionLabels.TryGetValue(logLineParserInfo.SessionLookup);
                            }
                        }
                        else
                        {
                            logEvent = this._lookupSessionLabels.FirstOrDefault(kv => logLineParserInfo.IsLookupMatch(kv.Key)).Value;
                        }
                        break;
                    default:
                        break;
                }
                sessionKey = logEvent == null ? null : logLineParserInfo.SessionLookup;
            }

            return new Tuple<LogCassandraEvent, string>(logEvent, sessionKey);
        }

        private void DetermineSessionStatus(CLogLineTypeParser.SessionInfo logLineParserInfo,
                                            LogCassandraEvent logEvent,
                                            bool addSessionKey = false)
        {
            if (logEvent == null) return;

            if (logLineParserInfo.SessionId != null)
            {
                var sessionType = logLineParserInfo.SessionType;

                if(addSessionKey)
                {
                    sessionType = CLogLineTypeParser.SessionKeyTypes.Add;
                }
                else if (sessionType == CLogLineTypeParser.SessionKeyTypes.Auto)
                {
                    switch (logEvent.Type)
                    {
                        case EventTypes.SessionBegin:
                            sessionType = CLogLineTypeParser.SessionKeyTypes.Add;
                            break;
                        case EventTypes.SessionEnd:
                            sessionType = CLogLineTypeParser.SessionKeyTypes.Delete;
                            break;
                        default:
                            break;
                    }
                }

                switch (sessionType)
                {
                    case CLogLineTypeParser.SessionKeyTypes.Add:
                        this._openSessions[logLineParserInfo.SessionId] = logEvent;
                        break;
                    case CLogLineTypeParser.SessionKeyTypes.Delete:
                        this._openSessions.Remove(logLineParserInfo.SessionId);
                        break;
                    default:
                        break;
                }
            }

            if(logLineParserInfo.SessionLookupRegEx == null && logLineParserInfo.SessionLookup != null)
            {
                switch (logLineParserInfo.LookupType)
                {
                    case CLogLineTypeParser.SessionLookupTypes.DefineSession:
                        this._openSessions[logLineParserInfo.SessionLookup] = logEvent;
                        break;
                    case CLogLineTypeParser.SessionLookupTypes.DeleteSession:
                        this._openSessions.Remove(logLineParserInfo.SessionLookup);
                        break;
                    case CLogLineTypeParser.SessionLookupTypes.DefineLabel:
                        this._lookupSessionLabels[logLineParserInfo.SessionLookup] = logEvent;
                        break;
                    case CLogLineTypeParser.SessionLookupTypes.DeleteLabel:
                    case CLogLineTypeParser.SessionLookupTypes.ReadRemoveLabel:
                        this._lookupSessionLabels.Remove(logLineParserInfo.SessionLookup);
                        break;
                    default:
                        break;
                }
            }
            else if (logLineParserInfo.SessionLookupRegEx != null)
            {
                if (logLineParserInfo.LookupType == CLogLineTypeParser.SessionLookupTypes.DeleteLabel
                                || logLineParserInfo.LookupType == CLogLineTypeParser.SessionLookupTypes.ReadRemoveLabel)
                {
                    this._lookupSessionLabels.Where(kv => logLineParserInfo.IsLookupMatch(kv.Key))
                            .Select(kv => kv.Key)
                            .ForEach(k => this._lookupSessionLabels.Remove(k));
                }
                else if(logLineParserInfo.LookupType == CLogLineTypeParser.SessionLookupTypes.DeleteSession)
                {
                    this._openSessions.Where(kv => logLineParserInfo.IsLookupMatch(kv.Key))
                            .Select(kv => kv.Key)
                            .ForEach(k => this._openSessions.Remove(k));
                }
            }
        }

        private static void UpdateMatchProperties(Regex matchRegEx, Match matchItem, Dictionary<string, object> logProperties)
        {
            string normalizedGroupName;
            string uomType;
            int groupNamePos;

            foreach (var groupName in matchRegEx.GetGroupNames())
            {
                if (groupName != "0")
                {
                    groupNamePos = groupName.IndexOf('_');

                    if (groupNamePos > 0
                            && groupName[groupNamePos + 1] == '_')
                    {
                        normalizedGroupName = groupName.Substring(0, groupNamePos);
                        var lookupVar = groupName.Substring(groupNamePos + 2);
                        object lookupValue;

                        if(logProperties.TryGetValue(lookupVar, out lookupValue)
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

                    var groupInstance = matchItem.Groups[groupName];

                    if (groupInstance.Captures.Count <= 1)
                    {
                        logProperties.TryAddValue(normalizedGroupName,
                                                    StringHelpers.DetermineProperObjectFormat(matchItem.Groups[groupName].Value,
                                                                                                true,
                                                                                                false,
                                                                                                true,
                                                                                                uomType,
                                                                                                true));
                    }
                    else
                    {
                        var groupCaptures = new List<object>(groupInstance.Captures.Count);

                        foreach(Capture capture in groupInstance.Captures)
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
            }
        }

        private static IEnumerable<string> TurnPropertyIntoCollection(Dictionary<string, object> logProperties, string key)
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
                    return ((IEnumerable<object>)propValue).Select(o => o.ToString());
                }

                return new List<string>() { propValue.ToString() };
            }

            return null;
        }
    }
}
