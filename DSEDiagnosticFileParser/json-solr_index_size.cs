using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class json_solr_index_size : ProcessJsonFile
    {
        public json_solr_index_size(CatagoryTypes catagory,
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

        private readonly List<AggregatedStats> _statsList = new List<AggregatedStats>();
        private readonly List<string> _unknownDDLs = new List<string>();

        public override uint ProcessJSON(JObject jObject)
        {
            this.NbrItemsParsed = jObject.Count;
           
            if(this.NbrItemsParsed > 0)
            {
                var solrIndexSizeList = new List<Tuple<string, ulong>>();

                foreach(var jItem in jObject)
                {
                    solrIndexSizeList.Add(new Tuple<string, ulong>(jItem.Key, jItem.Value.Value<ulong>()));

                    var currentDDL = Cluster.TryGetTableIndexViewbyString(jItem.Key, this.Node.Cluster, this.Node.DataCenter);

                    if (currentDDL == null)
                    {                        
                        Logger.Instance.WarnFormat("MapperId<{0}>\t{1}\t{2}\tsolr Index \"{3}\" was not found in \"{4}\".",
                                                    this.MapperId,
                                                    this.Node,
                                                    this.ShortFilePath,
                                                    jItem.Key,
                                                    this.Node.DataCenter);
                        ++this.NbrWarnings;
                        this._unknownDDLs.Add(jItem.Key);                                               
                    }
                    else
                    {
                        var statItem = new AggregatedStats(this.File,
                                                            this.Node,
                                                            SourceTypes.JSON,
                                                            EventTypes.AggregateDataTool,
                                                            EventClasses.PerformanceStats | EventClasses.Node | EventClasses.KeyspaceTableViewIndexStats,
                                                            currentDDL,
                                                            DSEInfo.InstanceTypes.Search);
                        this._statsList.Add(statItem);

                        statItem.AssociateItem(Properties.Settings.Default.SolrIndexStorageSizeStatAttribute,
                                                UnitOfMeasure.Create(jItem.Value.Value<decimal>(),
                                                                        UnitOfMeasure.Types.Byte | UnitOfMeasure.Types.Storage));
                    }                   
                }                
            }

            this.Processed = true;
            return (uint) this._statsList.Count;
        }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class StatResults : IResult
        {
            private StatResults() { }
            internal StatResults(json_solr_index_size fileInstance)
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
    }


}
