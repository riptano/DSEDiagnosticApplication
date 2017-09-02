using System;
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
    public sealed class file_nodetool_cfstats : DiagnosticFile
    {
        public file_nodetool_cfstats(CatagoryTypes catagory,
                                        IDirectoryPath diagnosticDirectory,
                                        IFilePath file,
                                        INode node,
                                        string defaultClusterName,
                                        string defaultDCName,
                                        Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this._result = new LogResults(this);
        }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class LogResults : IResult
        {
            private LogResults() { }
            internal LogResults(file_nodetool_cfstats fileInstance)
            {
                this.Path = fileInstance.File;
                this.Node = fileInstance.Node;
                this._statsList = fileInstance._statsList;
                this.UnDefinedCQLObjects = fileInstance._unknownDDLs;
            }

            [JsonProperty(PropertyName = "Stats")]
            private readonly List<AggregatedStats> _statsList;

            public IEnumerable<string> UnDefinedCQLObjects { get; }

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
        private readonly LogResults _result;

        public override IResult GetResult()
        {
            return this._result;
        }

        private readonly List<AggregatedStats> _statsList = new List<AggregatedStats>();
        private readonly List<string> _unknownDDLs = new List<string>();

        private static readonly Tuple<string,UnitOfMeasure.Types>[] UOMKeywords = new Tuple<string, UnitOfMeasure.Types>[]
                                                                                    { new Tuple<string,UnitOfMeasure.Types>("latency",UnitOfMeasure.Types.Time),
                                                                                        new Tuple<string,UnitOfMeasure.Types>("space", UnitOfMeasure.Types.Storage | UnitOfMeasure.Types.Byte),
                                                                                        new  Tuple<string,UnitOfMeasure.Types>("memory", UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.Byte),
                                                                                        new Tuple<string,UnitOfMeasure.Types>("size", UnitOfMeasure.Types.Storage | UnitOfMeasure.Types.Byte),
                                                                                        new Tuple<string,UnitOfMeasure.Types>("bytes", UnitOfMeasure.Types.Byte)
                                                                                    };

        public override uint ProcessFile()
        {
            var fileLines = this.File.ReadAllLines();

            /*
            Keyspace: OpsCenter
	            Read Count: 5393
	            Read Latency: 11.973125347672909 ms.
	            Write Count: 166153555
	            Write Latency: 0.029237674270646812 ms.
	            Pending Flushes: 0
		            Table: backup_reports
		            SSTable count: 2
		            Space used (live): 147176
		            Space used (total): 147176
		            Space used by snapshots (total): 0
		            Off heap memory used (total): 332
		            SSTable Compression Ratio: 0.1591472412408917
		            Number of keys (estimate): 6
		            Memtable cell count: 16
		            Memtable data size: 22327
		            Memtable off heap memory used: 0
		            Memtable switch count: 1
		            Local read count: 1
		            Local read latency: 0.174 ms
		            Local write count: 14
		            Local write latency: 0.068 ms
		            Pending flushes: 0
		            Bloom filter false positives: 0
		            Bloom filter false ratio: 0.00000
		            Bloom filter space used: 192
		            Bloom filter off heap memory used: 176
		            Index summary off heap memory used: 36
		            Compression metadata off heap memory used: 120
		            Compacted partition minimum bytes: 42511
		            Compacted partition maximum bytes: 545791
		            Compacted partition mean bytes: 209988
		            Average live cells per slice (last five minutes): 4.0
		            Maximum live cells per slice (last five minutes): 4.0
		            Average tombstones per slice (last five minutes): 0.0
		            Maximum tombstones per slice (last five minutes): 0.0

		            Table: bestpractice_results
		            SSTable count: 1
		            Space used (live): 154853
		            Space used (total): 154853
		            Space used by snapshots (total): 0
		            Off heap memory used (total): 512
		            SSTable Compression Ratio: 0.05753838456931239
		            Number of keys (estimate): 29
		            Memtable cell count: 60
		            Memtable data size: 39664
		            Memtable off heap memory used: 0
		            Memtable switch count: 29
		            Local read count: 0
		            Local read latency: NaN ms
		            Local write count: 1243
		            Local write latency: 0.031 ms
		            Pending flushes: 0
		            Bloom filter false positives: 0
		            Bloom filter false ratio: 0.00000
		            Bloom filter space used: 176
		            Bloom filter off heap memory used: 168
		            Index summary off heap memory used: 32
		            Compression metadata off heap memory used: 312
		            Compacted partition minimum bytes: 9888
		            Compacted partition maximum bytes: 654949
		            Compacted partition mean bytes: 112321
		            Average live cells per slice (last five minutes): 0.0
		            Maximum live cells per slice (last five minutes): 0.0
		            Average tombstones per slice (last five minutes): 0.0
		            Maximum tombstones per slice (last five minutes): 0.0

            ----------------
            Keyspace: <nextkeyspace>
            ...
            */

            string line;
            bool skipSection = false;
            IKeyspace currentKeyspace = null;
            IDDLStmt currentDDL = null;
            AggregatedStats statItem = null;
            string[] splitValue = null; //first is the key/property and second item is the value
            Tuple<string, UnitOfMeasure.Types> UOM = null;
            string[] propValueSplit = null;
            object propValue = null;

            foreach (var element in fileLines)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                ++this.NbrItemsParsed;
                line = element.Trim();

                if (string.Empty == line || '-' == line[0])
                {
                    continue;
                }

                splitValue = line.Split(':');

                if (splitValue[0] == "Keyspace")
                {
                    skipSection = false;
                    currentKeyspace = this.Node.DataCenter.TryGetKeyspace(splitValue[1].Trim());

                    if (currentKeyspace == null)
                    {
                        Logger.Instance.WarnFormat("MapperId<{0}>\t{1}\t{2}\tNodetool CFStats Keyspace \"{3}\" was not found and this complete section will be skipped.",
                                                    this.MapperId,
                                                    this.Node,
                                                    this.File.PathResolved,
                                                    splitValue[1].Trim());
                        this._unknownDDLs.Add(splitValue[1].Trim());
                        skipSection = true;
                        statItem = null;
                    }
                    else
                    {
                        statItem = new AggregatedStats(this.File,
                                                        this.Node,
                                                        SourceTypes.CFStats,
                                                        EventTypes.AggregateDataTool,
                                                        EventClasses.PerformanceStats,
                                                        currentKeyspace);
                        this._statsList.Add(statItem);
                    }
                    continue;
                }
                else if (skipSection && currentKeyspace == null)
                {
                    continue;
                }
                else if (splitValue[0] == "Table" || splitValue[0] == "Table (index)")
                {
                    skipSection = false;
                    currentDDL = currentKeyspace.TryGetDDL(splitValue[1].Trim());

                    if (currentDDL == null)
                    {
                        var tableName = currentKeyspace.Name + '.' + splitValue[1].Trim();
                        Logger.Instance.WarnFormat("MapperId<{0}>\t{1}\t{2}\tNodetool CFStats Table/Index/View \"{3}\" was not found and this complete section will be skipped.",
                                                    this.MapperId,
                                                    this.Node,
                                                    this.File.PathResolved,
                                                    tableName);
                        this._unknownDDLs.Add(tableName);
                        skipSection = true;
                        statItem = null;
                    }
                    else
                    {
                        statItem = new AggregatedStats(this.File,
                                                        this.Node,
                                                        SourceTypes.CFStats,
                                                        EventTypes.AggregateDataTool,
                                                        EventClasses.PerformanceStats,
                                                        currentDDL);
                        this._statsList.Add(statItem);
                    }
                    continue;
                }
                else if (skipSection)
                {
                    continue;
                }

                UOM = UOMKeywords.FirstOrDefault(u => splitValue[0].IndexOf(u.Item1, StringComparison.CurrentCultureIgnoreCase) >= 0);
                propValueSplit = splitValue[1].Trim().Split(' ');

                if (Common.StringFunctions.ParseIntoNumeric(propValueSplit[0], out propValue, true))
                {
                    if((dynamic) propValue < 0) unchecked { propValue = (ulong) ((dynamic) propValue); }

                    if (UOM != null || propValueSplit.Length > 1)
                    {
                        propValue = new UnitOfMeasure((decimal)((dynamic)propValue),
                                                        propValueSplit.Length > 1 ? propValueSplit[1].Trim() : null,
                                                        UOM?.Item2 ?? UnitOfMeasure.Types.Unknown);
                    }
                }
                else
                {
                    try
                    {
                        propValue = new UnitOfMeasure(splitValue[1].Trim(),
                                                        UOM?.Item2 ?? UnitOfMeasure.Types.Unknown);
                    }
                    catch (System.Exception ex)
                    {
                        var name = statItem.Keyspace.Name;

                        if(statItem.TableViewIndex != null)
                        {
                            name += '.' + statItem.TableViewIndex.Name;
                        }

                        Logger.Instance.WarnFormat("MapperId<{0}>\t{1}\t{2}\tNodetool CFStats Table/Index/View \"{3}\" had an invalid value of \"{4}\"{6} for property \"{5}\"",
                                                    this.MapperId,
                                                    this.Node,
                                                    this.File.PathResolved,
                                                    name,
                                                    splitValue[1],
                                                    splitValue[0],
                                                    UOM == null ? string.Empty : string.Format(" ({0})", UOM));
                        Logger.Instance.Error(ex);
                        continue;
                    }
                }

                statItem.AssociateItem(splitValue[0], propValue);
            }

            this.Processed = true;
            return (uint) this._statsList.Count;
        }
    }
}

