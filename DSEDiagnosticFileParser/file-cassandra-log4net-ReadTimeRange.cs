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
    public sealed class file_cassandra_log4net_ReadTimeRange : DiagnosticFile
    {
        public DateTimeOffsetRange LogRestrictedTimeRange
        {
            get;
            set;
        } = LibrarySettings.LogRestrictedTimeRange;

        public bool IgnoreParsingErrors
        {
            get;
            set;
        } = LibrarySettings.LogIgnoreParsingErrors;

        public file_cassandra_log4net_ReadTimeRange(CatagoryTypes catagory,
                                                        IDirectoryPath diagnosticDirectory,
                                                        IFilePath file,
                                                        INode node,
                                                        string defaultClusterName,
                                                        string defaultDCName,
                                                        Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
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

            this._result = new LogResults(this);

            Logger.Instance.DebugFormat("Loaded class \"{0}\"{{File{{{1}}}}}",
                                            this.GetType().Name,
                                            this.ShortFilePath);
        }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class LogFileInfoParsed : IParsed
        {
            internal LogFileInfoParsed(file_cassandra_log4net_ReadTimeRange fileInstance, LogFileInfo logFileInfo)
            {
                this.Path = fileInstance.File;
                this.Node = fileInstance.Node;
                this.Cluster = fileInstance.Cluster;
                this.DataCenter = fileInstance.DataCenter;
                this.Node = fileInstance.Node;
                this.LogFileInfo = logFileInfo;
            }

            public SourceTypes Source { get { return SourceTypes.CassandraLog; } }
            public IPath Path { get; }
            public Cluster Cluster { get; }
            public IDataCenter DataCenter { get; }
            public INode Node { get; }
            public int Items { get { return 1; } }
            public uint LineNbr { get { return 0; } }

            public LogFileInfo LogFileInfo { get; }

            public override string ToString()
            {
                return string.Format("LogFileInfoParsed{{Source={0}, Path=\"{1}\", Cluster=\"{2}\", DataCenter=\"{3}\", Node=\"{4}\", Items={5}, LineNbr={6}, {7}",
                                        this.Source,
                                        this.Path.PathResolved,
                                        this.Cluster.Name,
                                        this.DataCenter.Name,
                                        this.Node.Id.NodeName(),
                                        this.Items,
                                        this.LineNbr,
                                        this.LogFileInfo);
            }
        }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class LogResults : IResult
        {
            private LogResults() { }
            internal LogResults(file_cassandra_log4net_ReadTimeRange fileInstance)
            {
                this.Path = fileInstance.File;
                this.Node = fileInstance.Node;
            }

            #region IResult
            [JsonConverter(typeof(DSEDiagnosticLibrary.IPathJsonConverter))]
            public IPath Path { get; }
            [JsonIgnore]
            public Cluster Cluster { get { return this.DataCenter?.Cluster; } }
            [JsonIgnore]
            public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
            public INode Node { get; }
            public int NbrItems { get { return this._eventList.Count; } }

            [JsonIgnore]
            public IEnumerable<IParsed> Results { get { return this._eventList; } }

            #endregion

            [JsonProperty(PropertyName = "Events")]
            private readonly List<LogFileInfoParsed> _eventList = new List<LogFileInfoParsed>();

            internal void AddLogFileInfo(LogFileInfoParsed logItem)
            {
                this._eventList.Add(logItem);
            }
        }

        [JsonProperty(PropertyName = "Results")]
        private readonly LogResults _result;

        public Cluster Cluster { get; private set; }
        public IDataCenter DataCenter { get; private set; }

        public override IResult GetResult()
        {
            return this._result;
        }

        public static Tuple<LogFileInfo,int> DeteremineLogFileInfo(IFilePath logFile, INode node, System.Threading.CancellationToken cancellationToken, DateTimeOffsetRange logTimeRange, bool? isDebugFile, bool ignoreParsingErrors)
        {
            LogFileInfo logFileInfo = null;
            int nbrItemsParsed = 0;

            using (var logFileInstance = new ReadLogFile(logFile, LibrarySettings.Log4NetConversionPattern, cancellationToken, node, logTimeRange, ignoreParsingErrors))
            {
                using (var logMessages = logFileInstance.ReadLogFileTimeRange())
                {
                    nbrItemsParsed = logMessages.CompletionStatus == LogCompletionStatus.ReadLogTimeRange ? 2 : 0;

                    if (nbrItemsParsed > 0)
                    {
                        logFileInfo = new LogFileInfo(logFile,
                                                        logMessages.LogFileTimeRange,
                                                        0,
                                                        null,
                                                        logMessages.LogTimeRange,
                                                        null,
                                                        isDebugFile);
                    }
                }
            }

            return new Tuple<LogFileInfo, int>(logFileInfo, nbrItemsParsed);
        }

        public override uint ProcessFile()
        {
            this.CancellationToken.ThrowIfCancellationRequested();
            var logFileInfoResult = DeteremineLogFileInfo(this.File, this.Node, this.CancellationToken, this.LogRestrictedTimeRange, null, this.IgnoreParsingErrors);
            LogFileInfo logFileInfo = logFileInfoResult.Item1;

            this.NbrItemsParsed = logFileInfoResult.Item2;

            if (logFileInfo != null)
            {
                var fndOverlapppingLogs = this.Node.LogFiles.Where(l => l.LogDateRange.IsBetween(logFileInfo.LogDateRange));

                if(fndOverlapppingLogs.HasAtLeastOneElement())
                {
                    Logger.Instance.WarnFormat("MapperId<{0}>\t{1}\t{2}\tDetected overlapping of logs for Date Range {3} with logs {{{4}}} ",
                                                    this.MapperId,
                                                    this.Node,
                                                    this.ShortFilePath,
                                                    logFileInfo.LogDateRange,
                                                    string.Join(", ", fndOverlapppingLogs));
                    ++this.NbrWarnings;
                }

                this.Node.AssociateItem(logFileInfo);
                this._result.AddLogFileInfo(new LogFileInfoParsed(this, logFileInfo));
            }

            return (uint) this._result.NbrItems;
        }
    }
}
