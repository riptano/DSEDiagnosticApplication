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
            this._result = new StatResults(this);           
        }

        public file_nodetool_cfstats(IFilePath file,
                                        string defaultClusterName,
                                        string defaultDCName,
                                        Version targetDSEVersion = null)
            : base(CatagoryTypes.CommandOutputFile, file, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this._result = new StatResults(this);
        }

        /// <summary>
        /// A collection of keyspace names and if a warning or error occurred for that keyspace it will be ignored.       
        /// </summary>
        [JsonIgnore]
        public IEnumerable<string> IgnoreWarningsErrosInKeySpaces
        {
            get;
            set;
        } = LibrarySettings.IgnoreWarningsErrosInKeySpaces;

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class StatResults : IResult
        {
            private StatResults() { }
            internal StatResults(file_nodetool_cfstats fileInstance)
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
        private readonly StatResults _result;

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
            bool skipSection = true;
            bool notFoound = false;
            IKeyspace currentKeyspace = null;            
            IDDLStmt currentDDL = null;
            AggregatedStats statItem = null;
            AggregatedStats statItemCurrentKeyspace = null;
            string[] splitValue = null; //first is the key/property and second item is the value                       
            string attribute = null;
            var initialNodeStorage = this.Node.DSE.StorageUsed;

            object DetermineAttribVale(string strValue)
            {
                object propValue = null;
                
                var attrValue = strValue.Trim();
                var attrValueUOM = UnitOfMeasure.ConvertToType(attrValue);

                if ((attrValueUOM & UnitOfMeasure.Types.NaN) != 0)
                {
                    propValue = new UnitOfMeasure(attrValueUOM);                    
                }
                else
                {                    
                    var UOM = UOMKeywords.FirstOrDefault(u => attribute.IndexOf(u.Item1, StringComparison.CurrentCultureIgnoreCase) >= 0);
                    var propValueSplit = attrValue.Split(' ', '%');

                    if (Common.StringFunctions.ParseIntoNumeric(propValueSplit[0], out object numValue, true))
                    {
                        if (UOM != null || attrValueUOM != UnitOfMeasure.Types.Unknown || propValueSplit.Length > 1)
                        {
                            if (propValueSplit.Length == 2 && propValueSplit[1].Any(c => c == '%')) numValue = ((decimal)(dynamic)numValue) / 100m;

                            propValue = UnitOfMeasure.Create(((dynamic)numValue),
                                                                (UOM?.Item2 ?? UnitOfMeasure.Types.Unknown) | attrValueUOM,
                                                                propValueSplit.Length > 1 ? propValueSplit[1].Trim() : null,
                                                                true);
                        }
                        else
                        {
                            if ((dynamic)numValue == -1)
                            {
                                propValue = null;
                            }
                            else
                            {
                                propValue = numValue;
                            }
                        }
                    }
                    else
                    {
                        propValue = attrValue;                        
                    }
                }

                return propValue;
            }

            this.Node.DSE.StorageUsed = UnitOfMeasure.NaNValue;
            
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
                attribute = splitValue[0].Trim();
                
                if (attribute == "Keyspace")
                {
                    var keyspaceNotInDC = false;
                    var ksName = splitValue[1].Trim();
                    skipSection = false;
                    notFoound = false;
                                        
                    currentKeyspace = this.Node.DataCenter?.TryGetKeyspace(ksName);

                    if(currentKeyspace == null)
                    {
                        //Try Master Cluster!
                        currentKeyspace = Cluster.MasterCluster
                                                .GetKeyspaces(ksName)
                                                .FirstOrDefault(k => k.EverywhereStrategy
                                                                        || k.LocalStrategy
                                                                        || this.Node.DataCenter == null
                                                                        || k.Replications.Any(r => r.DataCenter.Name == this.Node.DataCenter.Name));

                        if (currentKeyspace == null)
                        {
                            currentKeyspace = this.Node.Cluster.Keyspaces.FirstOrDefault(k => k.Name == ksName);

                            if (currentKeyspace != null)
                            {
                                if (this.IgnoreWarningsErrosInKeySpaces == null || !this.IgnoreWarningsErrosInKeySpaces.Any(i => i == ksName))
                                {
                                    Logger.Instance.WarnFormat("MapperId<{0}>\t{1}\t{2}\tNodetool CFStats Keyspace \"{3}\" was found but not in DC \"{4}\" (Keyspace DC(s) are {{{5}}}). This section will use this Keyspace and the AggregatedStats instance will be marked as in Error.",
                                                                this.MapperId,
                                                                this.Node,
                                                                this.File.PathResolved,
                                                                ksName,
                                                                this.Node.DataCenter?.Name ?? "<NoDC>",
                                                                string.Join(",", currentKeyspace.DataCenters.Select(dc => dc.Name)));
                                    ++this.NbrWarnings;
                                    notFoound = true;
                                    this._unknownDDLs.Add(string.Format("{0} (KS Not Fnd in DC {1})", ksName, this.Node.DataCenter.Name));
                                }
                                keyspaceNotInDC = true;
                            }
                        }
                        else
                        {
                            if (this.IgnoreWarningsErrosInKeySpaces == null || !this.IgnoreWarningsErrosInKeySpaces.Any(i => i == ksName))
                            {
                                Logger.Instance.InfoFormat("MapperId<{0}>\t{1}\t{2}\tNodetool CFStats Keyspace \"{3}\" was not found in DC \"{4}\" in Cluster \"{5}\" but was Found in the Master Cluster. Using the Master Cluster's instance.",
                                                            this.MapperId,
                                                            this.Node,
                                                            this.File.PathResolved,
                                                            ksName,
                                                            this.Node.DataCenter?.Name ?? "<NoDC>",
                                                            this.Node.Cluster.Name);
                            }
                        }
                    }

                    if (currentKeyspace == null)
                    {
                        if (this.IgnoreWarningsErrosInKeySpaces == null || !this.IgnoreWarningsErrosInKeySpaces.Any(i => i == ksName))
                        {
                            Logger.Instance.WarnFormat("MapperId<{0}>\t{1}\t{2}\tNodetool CFStats Keyspace \"{3}\" was not found in DC \"{4}\" and this complete section will be skipped.",
                                                    this.MapperId,
                                                    this.Node,
                                                    this.File.PathResolved,
                                                    ksName,
                                                    this.Node.DataCenter?.Name ?? "<NoDC>");
                            ++this.NbrWarnings;
                            notFoound = true;
                            this._unknownDDLs.Add(ksName);
                        }

                        skipSection = true;
                        statItem = null;
                        statItemCurrentKeyspace = null;

                        var errorStatItem = new AggregatedStats(this.File,
                                                                    this.Node,
                                                                    SourceTypes.CFStats,
                                                                    EventTypes.AggregateDataTool,
                                                                    EventClasses.Node | EventClasses.KeyspaceTableViewIndexStats);
                        this._statsList.Add(errorStatItem);

                        errorStatItem.AssociateItem(AggregatedStats.ErrorKSNotFnd,
                                                        new List<string>() { string.Format("Keyspace \"{0}\" not found in DC \"{1}\"",
                                                                                            ksName,
                                                                                            this.Node.DataCenter?.Name ?? "<NoDC>") });
                    }
                    else
                    {
                        statItem = new AggregatedStats(this.File,
                                                        this.Node,
                                                        SourceTypes.CFStats,
                                                        EventTypes.AggregateDataTool,
                                                        EventClasses.PerformanceStats | EventClasses.Node | EventClasses.KeyspaceTableViewIndexStats,
                                                        currentKeyspace);
                        this._statsList.Add(statItem);
                        statItemCurrentKeyspace = statItem;

                        if(keyspaceNotInDC)
                        {
                            statItem.AssociateItem(AggregatedStats.DCNotInKS, this.Node.DataCenter);
                        }
                    }
                    continue;
                }
                else if (skipSection && currentKeyspace == null)
                {
                    continue;
                }
                else if (attribute == "Table" || attribute == "Table (index)")
                {
                    var itemName = splitValue[1].Trim();
                                        
                    skipSection = false;
                    notFoound = false;

                    currentDDL = currentKeyspace.TryGetDDL(itemName);

                    //Fixes issue where CFStats returns a concatenated name (e.g., idp.sso_idp_client_ndxidp.sso_idp_client_ndx)
                    if (currentDDL == null && itemName.Length % 2 == 0)
                    {                        
                        currentDDL = currentKeyspace.TryGetDDL(itemName.Substring(0, itemName.Length / 2));
                    }

                    if (currentDDL == null)
                    {
                        var tableName = currentKeyspace.Name + '.' + itemName;

                        if (this.IgnoreWarningsErrosInKeySpaces == null || !this.IgnoreWarningsErrosInKeySpaces.Any(i => i == currentKeyspace.Name))
                        {
                            Logger.Instance.WarnFormat("MapperId<{0}>\t{1}\t{2}\tNodetool CFStats Table/Index/View \"{3}\" was not found in \"{4}\" and this complete section will be skipped.",
                                                        this.MapperId,
                                                        this.Node,
                                                        this.ShortFilePath,
                                                        tableName,
                                                        currentKeyspace.DataCenter?.ToString() ?? currentKeyspace.Cluster?.ToString());
                            ++this.NbrWarnings;
                            notFoound = true;
                            this._unknownDDLs.Add(tableName);
                        }

                        skipSection = true;
                        statItem = null;

                        if(statItemCurrentKeyspace == null)
                        {
                            var errorStatItem = new AggregatedStats(this.File,
                                                                    this.Node,
                                                                    SourceTypes.CFStats,
                                                                    EventTypes.AggregateDataTool,
                                                                    EventClasses.Node | EventClasses.KeyspaceTableViewIndexStats);
                            this._statsList.Add(errorStatItem);
                            notFoound = true;
                            errorStatItem.AssociateItem(AggregatedStats.ErrorCItemNotFnd,
                                                                        new List<string>() { string.Format("{1} \"{0}\" not found", tableName, attribute) });
                        }
                        else
                        {                           
                            if(statItemCurrentKeyspace.Data.TryGetValue(AggregatedStats.ErrorCItemNotFnd, out object existingValue))
                            {
                                ((List<string>)existingValue).Add(string.Format("{1} \"{0}\" not found", tableName, attribute));
                            }
                            else
                            {
                                statItemCurrentKeyspace.AssociateItem(AggregatedStats.ErrorCItemNotFnd,
                                                                        new List<string>() { string.Format("{1} \"{0}\" not found", tableName, attribute) });
                            }
                        }
                    }
                    else
                    {
                        statItem = new AggregatedStats(this.File,
                                                        this.Node,
                                                        SourceTypes.CFStats,
                                                        EventTypes.AggregateDataTool,
                                                        EventClasses.PerformanceStats | EventClasses.Node | EventClasses.KeyspaceTableViewIndexStats,
                                                        currentDDL);
                        this._statsList.Add(statItem);
                    }
                    continue;
                }
                else if (skipSection)
                {
                    if (!notFoound && currentKeyspace != null)
                    {
                        if (UnitOfMeasure.ConvertToValue(DetermineAttribVale(splitValue[1]), out dynamic decValue))
                        {                            
                            if (attribute == Properties.StatPropertyNames.Default.TotalStorage)
                            {                                
                                ((KeySpace)currentKeyspace).AddToStorage(decValue);
                                this.Node.DSE.StorageUsed = this.Node.DSE.StorageUsed.Add(decValue);
                            }
                            else if (attribute == Properties.StatPropertyNames.Default.LocalReadCount)
                            {
                                long count = (long)decValue;

                                ((KeySpace)currentKeyspace).AddToReadCount(count);
                                this.Node.DSE.ReadCount += count;
                            }
                            else if (attribute == Properties.StatPropertyNames.Default.LocalWriteCount)
                            {
                                long count = (long)decValue;
                               
                                ((KeySpace)currentKeyspace).AddToWriteCount((long)count);
                                this.Node.DSE.WriteCount += count;
                            }
                            else if (Properties.StatPropertyNames.Default.NbrPartitionKeys.ComparePropName(attribute))
                            {
                                long count = (long)decValue;
                                
                                ((KeySpace)currentKeyspace).AddToKeyCount(count);
                                this.Node.DSE.KeyCount += count;
                            }
                            else if (attribute == Properties.StatPropertyNames.Default.SSTableCount)
                            {
                                long count = (long)decValue;
                                
                                ((KeySpace)currentKeyspace).AddToSSTableCount(count);
                                this.Node.DSE.SSTableCount += count;
                            }
                        }

                    }
                    continue;
                }

                //determine attribute value
                var propValue = DetermineAttribVale(splitValue[1]);
                
                if(statItem.TableViewIndex != null && UnitOfMeasure.ConvertToValue(propValue, out dynamic numValue))
                {
                    if (attribute == Properties.Settings.Default.CFStatsDetectReadActivityAttr
                            || (attribute == Properties.Settings.Default.CFStatsDetectWriteActivityAttr && !(statItem.TableViewIndex is ICQLIndex)))
                    {

                        if ((decimal) numValue > 0m)
                        {
                            if(!statItem.TableViewIndex.IsActive.HasValue || !statItem.TableViewIndex.IsActive.Value)
                                statItem.TableViewIndex.SetAsActive(true);
                        }
                        else if(!statItem.TableViewIndex.IsActive.HasValue)
                        {
                            statItem.TableViewIndex.SetAsActive(false);
                        }
                    }
                
                    if (attribute == Properties.StatPropertyNames.Default.TotalStorage)
                    {                        
                        if (statItem.TableViewIndex is CQLTable cqlTbl)
                        {
                            cqlTbl.AddToStorage(numValue);
                        }
                        else if (statItem.TableViewIndex is CQLIndex cqlIdx)
                        {
                            cqlIdx.AddToStorage(numValue);
                        }

                        ((KeySpace)statItem.TableViewIndex.Keyspace).AddToStorage(numValue);
                        this.Node.DSE.StorageUsed = this.Node.DSE.StorageUsed.Add(numValue);                        
                    }
                    else if (attribute == Properties.StatPropertyNames.Default.LocalReadCount)
                    {
                        long count = (long)numValue;

                        if (statItem.TableViewIndex is CQLTable cqlTbl)
                        {
                            cqlTbl.AddToReadCount(count);
                        }
                        else if (statItem.TableViewIndex is CQLIndex cqlIdx)
                        {
                            cqlIdx.AddToReadCount(count);
                        }

                        ((KeySpace)statItem.TableViewIndex.Keyspace).AddToReadCount(count);
                        this.Node.DSE.ReadCount += count;
                    }
                    else if (attribute == Properties.StatPropertyNames.Default.LocalWriteCount)
                    {
                        long count = (long)numValue;

                        if (statItem.TableViewIndex is CQLTable cqlTbl)
                        {
                            cqlTbl.AddToWriteCount(count);
                        }

                        ((KeySpace)statItem.TableViewIndex.Keyspace).AddToWriteCount(count);
                        this.Node.DSE.WriteCount += count;
                    }
                   else if (Properties.StatPropertyNames.Default.NbrPartitionKeys.ComparePropName(attribute))
                    {
                        long count = (long)numValue;

                        if (statItem.TableViewIndex is CQLTable cqlTbl)
                        {
                            cqlTbl.AddToKeyCount(count);
                        }

                        ((KeySpace)statItem.TableViewIndex.Keyspace).AddToKeyCount(count);
                        this.Node.DSE.KeyCount += count;
                    }
                    else if (attribute == Properties.StatPropertyNames.Default.SSTableCount)
                    {
                        long count = (long) numValue;

                        if (statItem.TableViewIndex is CQLTable cqlTbl)
                        {
                            cqlTbl.AddToSSTableCount(count);
                        }

                        ((KeySpace)statItem.TableViewIndex.Keyspace).AddToSSTableCount(count);
                        this.Node.DSE.SSTableCount += count;
                    }                    
                }

                statItem.AssociateItem(attribute, propValue);
            }

            {
                var statitems = from item in this._statsList
                                 where item.TableViewIndex != null && item.TableViewIndex is ICQLTable
                                 let tblAttribs = (from a in item.Data
                                                   where a.Key == Properties.StatPropertyNames.Default.MaxTombstonesSlice
                                                           || a.Key == Properties.StatPropertyNames.Default.MaxLiveCellsReadSlice
                                                           || a.Key == Properties.StatPropertyNames.Default.LocalReadCount
                                                           || a.Key == Properties.StatPropertyNames.Default.LocalWriteCount
                                                           || a.Key == Properties.StatPropertyNames.Default.LCSLevels
                                                           || a.Key == Properties.StatPropertyNames.Default.TotalStorage
                                                   group a by a.Key into g
                                                   select new { Key = g.Key, Values = g.Select(i => i.Value).Where(i => i != null).ToArray() }).ToArray()
                                 let tombstones = tblAttribs.Where(i => i.Key == Properties.StatPropertyNames.Default.MaxTombstonesSlice)
                                                     .SelectMany(i => i.Values)
                                                     .DefaultIfEmpty().Sum(i => (decimal)((dynamic)i))
                                 let liveCells = tblAttribs.Where(i => i.Key == Properties.StatPropertyNames.Default.MaxLiveCellsReadSlice)
                                                     .SelectMany(i => i.Values)
                                                     .DefaultIfEmpty().Sum(i => (decimal)((dynamic)i))
                                 let readCells = tblAttribs.Where(i => i.Key == Properties.StatPropertyNames.Default.LocalReadCount)
                                                     .SelectMany(i => i.Values)
                                                     .DefaultIfEmpty().Sum(i => (decimal)((dynamic)i))
                                 let writtenCells = tblAttribs.Where(i => i.Key == Properties.StatPropertyNames.Default.LocalWriteCount)
                                                     .SelectMany(i => i.Values)
                                                     .DefaultIfEmpty().Sum(i => (decimal)((dynamic)i))
                                 let LCSLevel = tblAttribs.Where(i => i.Key == Properties.StatPropertyNames.Default.LCSLevels)
                                                     .Select(i => PercentLCSBackup(i.Values.Cast<string>()))
                                                     .FirstOrDefault()
                                 let LCSSplitLevels = tblAttribs.Where(i => i.Key == Properties.StatPropertyNames.Default.LCSLevels)
                                                         .SelectMany(i => i.Values)
                                                         .Where(i => ((string)i).Contains('/'))
                                 let totalReadsWrites = tblAttribs.Where(i => i.Key == Properties.StatPropertyNames.Default.LocalReadCount
                                                                                 || i.Key == Properties.StatPropertyNames.Default.LocalWriteCount)
                                                         .SelectMany(i => i.Values)
                                                     .DefaultIfEmpty().Sum(i => (decimal)((dynamic)i))
                                 let totalStorageMB = tblAttribs.Where(i => i.Key == Properties.StatPropertyNames.Default.TotalStorage)
                                                         .SelectMany(i => i.Values)
                                                     .DefaultIfEmpty().Sum(i => i is UnitOfMeasure
                                                                                ? ((UnitOfMeasure) i).ConvertSizeUOM(UnitOfMeasure.Types.MiB)
                                                                                : (decimal)((dynamic)i))
                                 select new
                                 {
                                     Stat = item,
                                     TombstoneLivePercent = tombstones == 0 && liveCells == 0
                                                                     ? 0
                                                                     : tombstones / (tombstones + liveCells),
                                     ReadPercent = totalReadsWrites == 0 ? 0 : readCells / totalReadsWrites,
                                     WritePercent = totalReadsWrites == 0 ? 0 : writtenCells / totalReadsWrites,
                                     LCSLevel = LCSLevel.ActualLevel == 0 ? 0m : (LCSLevel.IdealLevel == 0 ? 1m : (decimal)LCSLevel.ActualLevel / (decimal)LCSLevel.IdealLevel),
                                     LCSSplitLevels = string.Join(", ", LCSSplitLevels),
                                     LCSNbrSplits = LCSLevel.SplitLevels,
                                     StorageMB = totalStorageMB
                                 };
                var statItemArray = statitems.ToArray();

                foreach (var stat in statItemArray)
                {
                    stat.Stat.AssociateItem(Properties.StatPropertyNames.Default.TombstoneLiveCellRatio, stat.TombstoneLivePercent);
                    stat.Stat.AssociateItem(Properties.StatPropertyNames.Default.LocalReadPercent, stat.ReadPercent);
                    stat.Stat.AssociateItem(Properties.StatPropertyNames.Default.LocalWritePercent, stat.WritePercent);
                    stat.Stat.AssociateItem(Properties.StatPropertyNames.Default.LCSSplitLevels, stat.LCSSplitLevels);
                    stat.Stat.AssociateItem(Properties.StatPropertyNames.Default.LCSBackRatio, stat.LCSLevel);
                    stat.Stat.AssociateItem(Properties.StatPropertyNames.Default.LCSNbrSplitLevels, stat.LCSNbrSplits);
                }                               
            }

            if (this.Node.DSE.StorageUsed.NaN)
                this.Node.DSE.StorageUsed = initialNodeStorage;

            this.Processed = true;
            return (uint) this._statsList.Count;
        }

        struct LCSLevelSplit
        {
            public LCSLevelSplit(string level)
                : this()
            {
                long numLvl;
                var splitValue = level.Split('/');

                if (splitValue.Length > 1)
                {
                    if (long.TryParse(splitValue[0], out numLvl))
                        ActualLevel = numLvl;
                    if (long.TryParse(splitValue[1], out numLvl))
                        IdealLevel = numLvl;
                    SplitLevels = level.Count(c => c == '/');
                }
                else if (long.TryParse(splitValue[0], out numLvl))
                    IdealLevel = numLvl;
            }

            public long IdealLevel;
            public long ActualLevel;
            public int SplitLevels;
        }
        static LCSLevelSplit PercentLCSBackup(IEnumerable<string> lcsLevels)
        {
            var levels = from l in (lcsLevels.SelectMany(l => l.Trim(new char[] { '[', ']', ' ' }).Split(','))
                                        .Select(l => new LCSLevelSplit(l)))
                         group l by 1 into g
                         select new LCSLevelSplit() { ActualLevel = g.DefaultIfEmpty().Sum(a => a.ActualLevel),
                                                        IdealLevel = g.DefaultIfEmpty().Sum(i => i.IdealLevel),
                                                        SplitLevels = g.DefaultIfEmpty().Sum(i => i.SplitLevels) };

            return levels.FirstOrDefault();
        }
    }      
}

