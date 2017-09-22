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
                                            this.File);
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

        public override uint ProcessFile()
        {
            this.CancellationToken.ThrowIfCancellationRequested();
            LogFileInfo logFileInfo = null;

            using (var logFileInstance = new ReadLogFile(this.File, LibrarySettings.Log4NetConversionPattern, this.CancellationToken, this.Node, file_cassandra_log4net.LogTimeRange))
            {
                using (var logMessages = logFileInstance.ReadLogFileTimeRange())
                {
                    this.NbrItemsParsed = logMessages.CompletionStatus == LogCompletionStatus.ReadLogTimeRange ? 2 : 0;

                   if(this.NbrItemsParsed > 0)
                    {
                        logFileInfo = new LogFileInfo(this.File,
                                                        logMessages.LogTimeRange,
                                                        0);
                    }
                }
            }

            if (logFileInfo != null)
            {
                var fndOverlapppingLogs = this.Node.LogFiles.Where(l => l.LogDateRange.IsBetween(logFileInfo.LogDateRange));

                if(fndOverlapppingLogs.HasAtLeastOneElement())
                {
                    Logger.Instance.WarnFormat("MapperId<{0}>\t{1}\t{2}\tDetected overlapping of logs for Date Range {3} with logs {{{4}}} ",
                                                    this.MapperId,
                                                    this.Node,
                                                    this.File.PathResolved,
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
