﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class file_nodetool_tpstats : DiagnosticFile
    {
        public file_nodetool_tpstats(CatagoryTypes catagory,
                                        IDirectoryPath diagnosticDirectory,
                                        IFilePath file,
                                        INode node,
                                        string defaultClusterName,
                                        string defaultDCName,
                                        Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this._result = new StatResults(this);
        }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class StatResults : IResult
        {
            private StatResults() { }
            internal StatResults(file_nodetool_tpstats fileInstance)
            {
                this.Path = fileInstance.File;
                this.Node = fileInstance.Node;
                this._statsList = fileInstance._statsList;
            }

            [JsonProperty(PropertyName = "Stats")]
            private readonly List<AggregatedStats> _statsList;

            #region IResult
            [JsonConverter(typeof(DSEDiagnosticLibrary.IPathJsonConverter))]
            public IPath Path { get; private set; }
            [JsonIgnore]
            public Cluster Cluster { get { return this.DataCenter?.Cluster; } }
            [JsonIgnore]
            public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
            public INode Node { get; private set; }
            public int NbrItems { get { return this._statsList.Count; } }

            [JsonIgnore]
            public IEnumerable<IParsed> Results { get { return this._statsList; } }

            #endregion
        }

        [JsonProperty(PropertyName = "Results")]
        private readonly StatResults _result;

        public override IResult GetResult()
        {
            return this._result;
        }

        private readonly List<AggregatedStats> _statsList = new List<AggregatedStats>();

        public override uint ProcessFile()
        {
            var fileLines = this.File.ReadAllLines();

            /*
            Pool Name                    Active   Pending      Completed   Blocked  All time blocked
            MutationStage                     0         0       28117575         0                 0
            ReadStage                         0         0       18889496         0                 0
            RequestResponseStage              0         0       46855295         0                 0
            ReadRepairStage                   0         0          97643         0                 0
            CounterMutationStage              0         0            416         0                 0
            MiscStage                         0         0              0         0                 0
            HintedHandoff                     0         0             13         0                 0
            GossipStage                       0         0        1363103         0                 0
            CacheCleanupExecutor              0         0              0         0                 0
            InternalResponseStage             0         0             32         0                 0
            CommitLogArchiver                 0         0              0         0                 0
            ValidationExecutor                0         0          30883         0                 0
            MigrationStage                    0         0             13         0                 0
            CompactionExecutor                0         0         969285         0                 0
            AntiEntropyStage                  0         0          93111         0                 0
            PendingRangeCalculator            0         0             17         0                 0
            Sampler                           0         0              0         0                 0
            MemtableFlushWriter               0         0           9219         0                 0
            MemtablePostFlush                 0         0          44224         0                 0
            MemtableReclaimMemory             0         0           9233         0                 0
            Native-Transport-Requests         1         0       52957736         0              2282

            Message type           Dropped
            READ                         0
            RANGE_SLICE                  0
            _TRACE                       0
            MUTATION                     0
            COUNTER_MUTATION             0
            BINARY                       0
            REQUEST_RESPONSE             0
            PAGED_RANGE                  0
            READ_REPAIR                  0

            */

            string line;
            Match regExMatch;
            Group name;
            Group props;
            string propType = null;
            string[] poolProperties = null;
            bool droppedPropties = false;

            var statItem = new AggregatedStats(this.File,
                                                this.Node,
                                                SourceTypes.TPStats,
                                                EventTypes.AggregateDataTool,
                                                EventClasses.NodeStats);
            this._statsList.Add(statItem);

            foreach (var element in fileLines)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                line = element.Trim();

                if (line == string.Empty) continue;

                ++this.NbrItemsParsed;

                regExMatch = this.RegExParser.Match(line);

                if (regExMatch.Success)
                {
                    name = regExMatch.Groups["Name"];
                    props = regExMatch.Groups["Prop"];

                    if (name.Success && props.Success)
                    {
                        if (props.Captures.Count == 5)
                        {
                            if (poolProperties == null)
                            {
                                propType = name.Value.Trim();
                                poolProperties = new string[5];
                                for (int nIdx = 0; nIdx < 5; ++nIdx)
                                {
                                    poolProperties[nIdx] = props.Captures[nIdx].Value.Trim();
                                }
                                continue;
                            }
                            else
                            {
                                for (int nIdx = 0; nIdx < 5; ++nIdx)
                                {
                                    //Attribute Key: "Pool Name.MutationStage.Active"
                                    statItem.AssociateItem(propType + '.' + name.Value.Trim() + '.' + poolProperties[nIdx], long.Parse(props.Captures[nIdx].Value.Trim()));
                                }
                                continue;
                            }
                        }
                        else if(props.Captures.Count == 1)
                        {
                            if(droppedPropties)
                            {
                                //Attribute Key: "READ.Dropped"
                                statItem.AssociateItem(name.Value.Trim() + '.' + propType, long.Parse(props.Captures[0].Value.Trim()));
                            }
                            else
                            {
                                droppedPropties = true;
                                propType = props.Captures[0].Value.Trim();
                            }
                            continue;
                        }
                    }
                }
                Logger.Instance.ErrorFormat("FileMapper<{3}>\t{0}\t{1}\tInvalid Line \"{2}\" found in nodetool tpstats File.",
                                                this.Node,
                                                this.File,
                                                line,
                                                this.MapperId);
            }

            this.Processed = true;
            return (uint) this._statsList.Count;
        }
    }
}
