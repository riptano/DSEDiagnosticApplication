using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using Common;
using DSEDiagnosticLog4NetParser;

namespace DSEDiagnosticFileParser
{
    public sealed class file_cassandra_log4net : DiagnosticFile
    {
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

        private List<LogCassandraEvent> _sessionEvents = new List<LogCassandraEvent>();
        private List<Tuple<string,LogCassandraEvent>> _openSessions = new List<Tuple<string,LogCassandraEvent>>();

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

            string subClass = matchItem.Item5.SubClass ?? logProperties.TryGetValue("SUBCLASS")?.ToString();
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
            var sstableFilePath = logProperties.TryGetValue("SSTABLEPATH") as string;
            var keyspaceNames = TurnPropertyIntoCollection(logProperties, "KEYSPACES");
            var ddlNames = TurnPropertyIntoCollection(logProperties, "DDLITEMNAMES");
            var sstableFilePaths = TurnPropertyIntoCollection(logProperties, "SSTABLEPATHS");
            EventClasses eventClass = matchItem.Item5.EventClass;

            IKeyspace primaryKS = string.IsNullOrEmpty(keyspaceName)
                                    ? null
                                    : this._dcKeyspaces.FirstOrDefault(k => k.Equals(keyspaceName));
            IDDLStmt primaryDDL = null;
            IEnumerable<IKeyspace> keyspaceInstances = keyspaceNames == null
                                                            ? null
                                                            : keyspaceNames
                                                                    .Select(k => this._dcKeyspaces.FirstOrDefault(ki => ki.Equals(k)))
                                                                    .Where(k => k != null);
            IEnumerable<IDDLStmt> ddlInstances = null;
            IEnumerable<INode> assocatedNodes = null;
            IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges = null;
            var parentEvents = this._sessionEvents
                                .Where(e => e.Type == EventTypes.SessionBegin
                                                && (logMessage.LogDateTimewTZOffset >= e.EventTime
                                                        && (!e.EventTimeEnd.HasValue
                                                                || logMessage.LogDateTimewTZOffset <= e.EventTimeEnd.Value)));
            string sessionKey = null;
            LogCassandraEvent sessionEvent = null;
            var eventType = matchItem.Item5.EventType;

            #region Session Instance
            if(eventType == EventTypes.SessionBeginOrItem)
            {
                sessionEvent = matchItem.Item5.DetermineOpenSession(this.Cluster,
                                                                    this.Node,
                                                                    primaryKS,
                                                                    logProperties,
                                                                    logMessage,
                                                                    this._sessionEvents,
                                                                    this._openSessions);

                if(sessionEvent == null)
                {
                    eventType = EventTypes.SessionBegin;
                }
                else
                {
                    eventType = EventTypes.SessionItem;
                }
            }

            if (eventType == EventTypes.SessionBegin)
            {
                sessionKey = matchItem.Item5.DetermineSessionKey(this.Cluster,
                                                                    this.Node,
                                                                    primaryKS,
                                                                    logProperties,
                                                                    logMessage,
                                                                    this._sessionEvents,
                                                                    this._openSessions);
            }
            else if(sessionEvent == null && (eventType & EventTypes.SessionItem) != 0)
            {
                sessionEvent = matchItem.Item5.DetermineOpenSession(this.Cluster,
                                                                    this.Node,
                                                                    primaryKS,
                                                                    logProperties,
                                                                    logMessage,
                                                                    this._sessionEvents,
                                                                    this._openSessions);
            }
            #endregion
            #region Determine Keyspace/DDLs
            if(primaryKS == null)
            {
                if(keyspaceInstances == null || keyspaceInstances.IsEmpty())
                {
                    if(sessionEvent != null)
                    {
                        primaryKS = sessionEvent.Keyspace;
                    }
                }
                else
                {
                    primaryKS = keyspaceInstances.First();
                }
            }

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
                    ddlInstances = ddlNames.SelectMany(d => this.DataCenter.Keyspaces.Select(k => k.TryGetDDL(d))).Where(d => d != null);
                }
                else
                {
                    ddlInstances = ddlNames.Select(d => primaryKS.TryGetDDL(d)).Where(d => d != null);
                }
            }
            else if (!string.IsNullOrEmpty(ddlName))
            {
                if (primaryKS == null)
                {
                    ddlInstances = this.DataCenter.Keyspaces.Select(k => k.TryGetDDL(ddlName)).Where(d => d != null);
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
            }

            if (!string.IsNullOrEmpty(sstableFilePath))
            {
                if(sstableFilePaths == null)
                {
                    sstableFilePaths = new List<string>() { StringHelpers.RemoveQuotes(sstableFilePath) };
                }
                else
                {
                    sstableFilePaths = new List<string>(sstableFilePaths);
                    ((List<string>)sstableFilePaths).Add(StringHelpers.RemoveQuotes(sstableFilePath));
                }
            }
            #endregion
            #region Assocated Nodes
            {
                var assocNode = logProperties.TryGetValue("NODE") as string;
                var assocNodes = TurnPropertyIntoCollection(logProperties, "NODES");

                if (string.IsNullOrEmpty(assocNode))
                {
                    if(assocNodes != null)
                    {
                        assocatedNodes = assocNodes.Select(n => this.Cluster.TryGetNode(n)).Where(n => n != null);
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

                        assocatedNodes = assocNodesList.Select(n => this.Cluster.TryGetNode(n)).Where(n => n != null);
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
                var ddlFromSSTables = sstableFilePaths.Select(f => StringHelpers.ParseSSTableFileIntoKSTableNames(f))
                                                        .Where(i => i != null);
                var ddlItems = ddlFromSSTables.Select(i => Cluster.TryGetTableIndexViewbyString(i.Item1, this.Cluster.Name, i.Item2))
                                                .Where(i => i != null);
                if(ddlName.HasAtLeastOneElement())
                {
                    if(ddlInstances == null || ddlInstances.IsEmpty())
                    {
                        ddlInstances = ddlItems;
                    }
                    else
                    {
                        ddlInstances = ddlInstances.Append(ddlItems.ToArray());
                    }
                }

                if(primaryKS == null && ddlInstances != null)
                {
                    var firstKS = ddlInstances.FirstOrDefault()?.Keyspace.FullName;

                    if(firstKS != null)
                    {
                        if(ddlInstances.All(k => k.FullName == firstKS))
                        {
                            primaryKS = ddlInstances.First().Keyspace;
                        }
                    }
                }
            }
            #endregion
            LogCassandraEvent logEvent = null;
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

            if(sessionEvent == null)
            {
                if (eventType == EventTypes.SessionItem)
                {
                    eventType = EventTypes.SessionBegin;
                }

                if (eventType != EventTypes.SessionBegin && eventDurationTime.HasValue)
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
                                                        parentEvents.Select(i => i.Id),
                                                        this.Node.Machine.TimeZone,
                                                        primaryKS,
                                                        primaryDDL,
                                                        logProperties,
                                                        sstableFilePaths?.DuplicatesRemoved(s => s),
                                                        ddlInstances?.DuplicatesRemoved(d => d.FullName),
                                                        assocatedNodes?.DuplicatesRemoved(s => s.Id),
                                                        tokenRanges?.DuplicatesRemoved(t => t),
                                                        matchItem.Item5.Product);
                }
                else if (eventType != EventTypes.SessionBegin && duration != null)
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
                                                        parentEvents.Select(i => i.Id),
                                                        this.Node.Machine.TimeZone,
                                                        primaryKS,
                                                        primaryDDL,
                                                        logProperties,
                                                        sstableFilePaths?.DuplicatesRemoved(s => s),
                                                        ddlInstances?.DuplicatesRemoved(d => d.FullName),
                                                        assocatedNodes?.DuplicatesRemoved(s => s.Id),
                                                        tokenRanges?.DuplicatesRemoved(t => t),
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
                                                        parentEvents.Select(i => i.Id),
                                                        this.Node.Machine.TimeZone,
                                                        primaryKS,
                                                        primaryDDL,
                                                        (TimeSpan?)duration,
                                                        logProperties,
                                                        sstableFilePaths?.DuplicatesRemoved(s => s),
                                                        ddlInstances?.DuplicatesRemoved(d => d.FullName),
                                                        assocatedNodes?.DuplicatesRemoved(s => s.Id),
                                                        tokenRanges?.DuplicatesRemoved(t => t),
                                                        matchItem.Item5.Product);

                    if(logEvent.Type == EventTypes.SessionBegin)
                    {
                        this._sessionEvents.Add(logEvent);
                        this._openSessions.Add(new Tuple<string,LogCassandraEvent>(sessionKey, logEvent));
                    }
                }
            }
            else
            {
                if (eventType == EventTypes.SessionEnd)
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
                                                        parentEvents.Select(i => i.Id),
                                                        this.Node.Machine.TimeZone,
                                                        primaryKS,
                                                        primaryDDL,
                                                        logProperties,
                                                        sstableFilePaths?.DuplicatesRemoved(s => s),
                                                        ddlInstances?.DuplicatesRemoved(d => d.FullName),
                                                        assocatedNodes?.DuplicatesRemoved(s => s.Id),
                                                        tokenRanges?.DuplicatesRemoved(t => t),
                                                        matchItem.Item5.Product);

                    sessionEvent.UpdateEndingTimeStamp(logMessage.LogDateTime);
                    this._openSessions.RemoveAll(s => s.Item1 == sessionKey);
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
                                                        parentEvents.Select(i => i.Id),
                                                        this.Node.Machine.TimeZone,
                                                        primaryKS,
                                                        primaryDDL,
                                                        (TimeSpan?)duration,
                                                        logProperties,
                                                        sstableFilePaths?.DuplicatesRemoved(s => s),
                                                        ddlInstances?.DuplicatesRemoved(d => d.FullName),
                                                        assocatedNodes?.DuplicatesRemoved(s => s.Id),
                                                        tokenRanges?.DuplicatesRemoved(t => t),
                                                        matchItem.Item5.Product);
                }
            }

            return logEvent;
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

                    if (groupInstance.Captures.Count == 0)
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

                        foreach(Group capture in groupInstance.Captures)
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

                return propValue as IEnumerable<string>;
            }

            return null;
        }
    }
}
