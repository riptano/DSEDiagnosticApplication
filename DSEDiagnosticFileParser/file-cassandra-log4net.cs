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
        private List<Tuple<string, List<LogCassandraEvent>>> _openSessions = new List<Tuple<string, List<LogCassandraEvent>>>();
        private Dictionary<string, string> _sessionLogIds = new Dictionary<string, string>();

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
        }

        public Cluster Cluster { get; private set; }
        public IDataCenter DataCenter { get; private set; }
        private IEnumerable<IKeyspace> _dcKeyspaces;

        public override IResult GetResult()
        {
            throw new NotImplementedException();
        }

        CLogLineTypeParser[] _parser = null;
        List<LogCassandraEvent> _logEvents = new List<LogCassandraEvent>();
        public static readonly CTS.List<LogCassandraEvent> OrphanedSessionEvents = new CTS.List<LogCassandraEvent>();

        public override uint ProcessFile()
        {
            using (var logFileInstance = new ReadLogFile(this.File, LibrarySettings.Log4NetConversionPattern, this.Node))
            {
                System.Threading.Tasks.Task.Factory.StartNew(() =>
                                                                logFileInstance.ProcessLogFile().Wait()).Wait();

                var logMessages = logFileInstance.Log;

                Tuple<Regex, Match, Regex, Match, CLogLineTypeParser> matchItem;
                LogCassandraEvent logEvent;

                foreach (var logMessage in logMessages.Messages)
                {
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

            OrphanedSessionEvents.AddRange(this._openSessions.SelectMany(s => { s.Item2.ForEach(e => e.MarkAsOrphaned()); return s.Item2; }));

            return 0;
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
            var parentEvents = this._sessionEvents
                                .Where(e => e.Type == EventTypes.SessionBegin
                                                && (logMessage.LogDateTimewTZOffset >= e.EventTime
                                                        && (!e.EventTimeEnd.HasValue
                                                                || logMessage.LogDateTimewTZOffset <= e.EventTimeEnd.Value)))
                                    .ToList();
            string sessionKey = null;
            LogCassandraEvent sessionEvent = null;
            List<LogCassandraEvent> sessionEventList = null;
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
                    ddlInstances = this.DataCenter.Keyspaces.Select(k => k.TryGetDDL(ddlName))
                                        .Where(d => d != null)
                                        .DuplicatesRemoved(d => d.FullName);
                }
                else
                {
                    var ddlInstance = primaryKS.TryGetDDL(ddlName);

                    if(ddlInstance != null)
                        ddlInstances = new List<IDDLStmt>() { ddlInstance };
                }
            }

            if(!string.IsNullOrEmpty(ddlName))
            {
                primaryDDL = ddlInstances.FirstOrDefault(d => d.Name == ddlName);

                if(primaryKS == null && primaryDDL != null)
                {
                    primaryKS = primaryDDL.Keyspace;
                }
            }

            if(primaryKS == null)
            {
                if(keyspaceInstances != null && keyspaceInstances.HasAtLeastOneElement())
                {
                    var fndKS = keyspaceInstances.First();

                    if(keyspaceInstances.All(k => k.Equals(fndKS)))
                    {
                        primaryKS = fndKS;
                    }
                }
            }
            if(primaryDDL != null && primaryKS == null)
            {
                primaryKS = primaryDDL.Keyspace;
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
                if (primaryKS == null)
                {
                    var firstKS = ddlInstances.First()?.Keyspace.FullName;

                    if (ddlInstances.All(k => k.Keyspace.FullName == firstKS))
                    {
                        primaryKS = ddlInstances.First().Keyspace;
                    }
                }
                if (primaryDDL == null)
                {
                    var firstDDL = ddlInstances.First()?.FullName;

                    if (ddlInstances.All(k => k.FullName == firstDDL))
                    {
                        primaryDDL = ddlInstances.First();
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

            if ((eventType & EventTypes.SessionItem) != 0)
            {
                sessionKey = matchItem.Item5.DetermineSessionKey(this.Cluster,
                                                                    this.Node,
                                                                    primaryKS,
                                                                    logProperties,
                                                                    logMessage);

                if (sessionKey != null)
                {
                    sessionEventList = this._openSessions.Find(s => s.Item1 == sessionKey)?.Item2;

                    if (eventType == EventTypes.SessionBeginOrItem)
                    {
                        if (sessionEventList == null)
                        {
                            eventType = EventTypes.SessionBegin;
                        }
                        else
                        {
                            eventType = EventTypes.SessionItem;
                        }
                    }

                    if ((eventType & EventTypes.SessionBegin) != 0)
                    {
                        if (primaryKS == null && sessionEventList != null && sessionEventList.HasAtLeastOneElement())
                        {
                           primaryKS = sessionEventList.First().Keyspace;
                        }
                    }
                    else
                    {
                        sessionEvent = sessionEventList?.LastOrDefault();

                        if (sessionEvent != null)
                        {
                            if(primaryKS == null) primaryKS = sessionEvent.Keyspace;
                            logId = sessionEvent.Id.ToString();
                        }
                    }                    
                }

                if (sessionEvent == null
                        && sessionKey == null
                        && sessionEventList == null)
                {
                    orphanedSession = true;
                    eventClass |= EventClasses.Orphaned;
                }
            }

            //Session GUID
            {
                var sessionIdKey = matchItem.Item5.DetermineSessionIdKey(this.Cluster,
                                                                            this.Node,
                                                                            primaryKS,
                                                                            logProperties,
                                                                            logMessage);

                if(sessionIdKey != null)
                {
                    if((eventType & EventTypes.SessionId) != 0)
                    {
                        if (string.IsNullOrEmpty(logId)) logId = Guid.NewGuid().ToString();
                        if(this._sessionLogIds.ContainsKey(sessionIdKey))
                        {
                            this._sessionLogIds[sessionIdKey] = logId;
                        }
                        else
                        {
                            this._sessionLogIds.Add(sessionIdKey, logId);
                        }

                        eventType = EventTypes.SessionItemInfo;
                    }
                    else
                    {
                        this._sessionLogIds.TryGetValue(sessionIdKey, out logId);
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
                if ((eventType & EventTypes.SessionBegin) == 0 && eventDurationTime.HasValue)
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
                                                        sessionKey ?? sessionEvent?.SessionTieOutId,
                                                        matchItem.Item5.Product);
                }
                else if ((eventType & EventTypes.SessionBegin) == 0 && duration != null)
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
                                                        sessionKey ?? sessionEvent?.SessionTieOutId,
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
                                                        sessionKey ?? sessionEvent?.SessionTieOutId,
                                                        matchItem.Item5.Product);

                    if ((logEvent.Type & EventTypes.SessionBegin) != 0)
                    {
                        this.AddNewSessionToOpenSessions(logEvent, sessionEventList, sessionKey);
                    }
                }
            }
            else
            {
                if ((eventType & EventTypes.SessionEnd) != 0)
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
                                                        sessionKey ?? sessionEvent?.SessionTieOutId,
                                                        matchItem.Item5.Product);

                    sessionEvent.UpdateEndingTimeStamp(logMessage.LogDateTime);

                    this.RemoveSessionFromOpenSessions(logEvent, sessionEventList, sessionKey, sessionEvent);
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
                                                        sessionKey ?? sessionEvent?.SessionTieOutId,
                                                        matchItem.Item5.Product);
                }
            }
            #endregion

            if (logEvent != null)
            {
                if ((logEvent.Type & EventTypes.SessionItem) != 0)
                {
                    if(orphanedSession && this._priorSessionLineTick.LogTick == logEvent.EventTimeLocal)
                    {
                        var checkSessionKey = logEvent.SessionTieOutId ?? matchItem.Item5.DetermineSessionKey(this.Cluster,
                                                                                                                this.Node,
                                                                                                                primaryKS,
                                                                                                                logProperties,
                                                                                                                logMessage);

                        if (this._priorSessionLineTick.CurrentSession.Type == EventTypes.SessionEnd
                                && checkSessionKey == this._priorSessionLineTick.CurrentSession.SessionTieOutId)
                        {
                            if((logEvent.Type & EventTypes.SessionBegin) != 0)
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

                    if ((logEvent.Type & EventTypes.SessionEnd) != 0)
                    {
                        this._priorSessionLineTick.LogTick = logEvent.EventTimeLocal;
                        this._priorSessionLineTick.CurrentSession = logEvent;
                    }
                }

                if (orphanedSession)
                {
                    OrphanedSessionEvents.Add(logEvent);
                }
            }

            return logEvent;
        }

        private void AddNewSessionToOpenSessions(LogCassandraEvent logEvent,
                                                    List<LogCassandraEvent> sessionEventList,
                                                    string sessionKey)
        {
            this._sessionEvents.Add(logEvent);
            if (sessionEventList == null)
            {
                this._openSessions.Add(new Tuple<string, List<LogCassandraEvent>>(sessionKey, new List<LogCassandraEvent>() { logEvent }));
            }
            else
            {
                sessionEventList.Add(logEvent);
            }
        }

        private void RemoveSessionFromOpenSessions(LogCassandraEvent logEvent,
                                                        List<LogCassandraEvent> sessionEventList,
                                                        string sessionKey,
                                                        LogCassandraEvent sessionEvent)
        {
            if (sessionEventList == null || sessionEventList.Count < 2)
            {
                this._openSessions.RemoveAt(this._openSessions.FindIndex(s => s.Item1 == (sessionKey ?? sessionEvent.SessionTieOutId)));
            }
            else
            {
                sessionEventList.Remove(sessionEvent);
            }

            /*
            if (logEvent.Class == EventClasses.MemtableFlush && logEvent.TableViewIndex != null)
            {
                var compactionList = this._openSessions.LastOrDefault(c => c.Item2.HasAtLeastOneElement()
                                                                        && c.Item2.First().Class == EventClasses.Compaction
                                                                        && c.Item2.Any(i => i.TableViewIndex != null
                                                                                                && i.TableViewIndex.Equals(logEvent.TableViewIndex)));
                if (compactionList != null)
                {
                    var compaction = compactionList.Item2.LastOrDefault(c => sessionEvent.EventTime <= c.EventTime);

                    if (compaction != null)
                    {
                        compaction.UpdateEndingTimeStamp(logEvent.EventTimeLocal);

                        if (compactionList.Item2.Count < 2)
                        {
                            this._openSessions.RemoveAt(this._openSessions.FindIndex(s => s.Item1 == compaction.SessionTieOutId));
                        }
                        else
                        {
                            compactionList.Item2.Remove(compaction);
                        }
                    }
                }
            }*/
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

                    if (groupNamePos > 0)
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
