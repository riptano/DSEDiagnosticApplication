using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticInsights
{
    public static partial class BuildCluster
    {

        public static string DefaultDirectory = @"./Insights/";

        public static IEnumerable<DSEDiagnosticLibrary.IDataCenter> BuildDCNodes(this Connection connection)
        {
            var dcList = new List<DSEDiagnosticLibrary.IDataCenter>();

            foreach (var node in connection.Nodes)
            {
                var esDoc = DseNode.BuilCurrentQueryNodeClientId(connection.ESClient, node.clientId, connection.AnalysisPeriod.Max).Documents.First();
                var aDC = DSEDiagnosticLibrary.Cluster.TryGetAddDataCenter(esDoc.data_center, connection.Cluster);

                if(!dcList.Contains(aDC))
                    dcList.Add(aDC);

                var aNode = DSEDiagnosticLibrary.Cluster.TryGetAddNode(esDoc.listen_address, aDC);

                aNode.DSE.InsightClientId = esDoc.clientId;
                aNode.DSE.PhysicalServerId = esDoc.server_id;
                aNode.DSE.HostId = esDoc.host_id;
                aNode.DSE.Rack = esDoc.rack;
                aNode.DSE.CaptureTimestamp = connection.AnalysisPeriod.Max;
                if (!string.IsNullOrEmpty(esDoc.dse_version)) aNode.DSE.Versions.DSE = Version.Parse(esDoc.dse_version);
                if (!string.IsNullOrEmpty(esDoc.schema_version)) aNode.DSE.Versions.Schema = Guid.Parse(esDoc.schema_version);
                if (!string.IsNullOrEmpty(esDoc.cassandra_version)) aNode.DSE.Versions.Cassandra = Version.Parse(esDoc.cassandra_version);

                if(esDoc.workloads != null)
                {                    
                    foreach (var workLoad in esDoc.workloads)
                    {
                        aNode.DSE.InstanceType |= (DSEDiagnosticLibrary.DSEInfo.InstanceTypes)Enum.Parse(typeof(DSEDiagnosticLibrary.DSEInfo.InstanceTypes), workLoad, true);
                    }
                }

                if (esDoc.graph) aNode.DSE.InstanceType |= DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Graph;
                aNode.DSE.Partitioner = esDoc.partitioner;

                var nodeConfigDoc = DSENodeConfiguration.BuilCurrentQueryNodeClientId(connection.ESClient, node.clientId, connection.AnalysisPeriod.Max)
                                        .Documents.FirstOrDefault();

                if(nodeConfigDoc != null)
                {
                    var jvmProps = nodeConfigDoc.Data.jvm_properties;
                    var dseConfig = nodeConfigDoc.Data.dse_config;
                    var cConfig = nodeConfigDoc.Data.cassandra_config;

                    if(!string.IsNullOrEmpty(nodeConfigDoc.Data.hostname))
                    {
                        aNode.Id.SetIPAddressOrHostName(nodeConfigDoc.Data.hostname, false);
                    }

                    if (string.IsNullOrEmpty(nodeConfigDoc.Data.os))
                    {
                        jvmProps.NullSafeSet<string>("os_name", c => aNode.Machine.OS = c);
                    }
                    else
                    {
                        aNode.Machine.OS = nodeConfigDoc.Data.os;
                    }

                    if(nodeConfigDoc.Data.jvm_memory_max > 0)
                    {
                        aNode.Machine.Java.HeapMemory.Maximum = DSEDiagnosticLibrary.UnitOfMeasure.Create((decimal)nodeConfigDoc.Data.jvm_memory_max,
                                                                                                            DSEDiagnosticLibrary.UnitOfMeasure.Types.Memory
                                                                                                                | DSEDiagnosticLibrary.UnitOfMeasure.Types.Byte);                        
                    }

                    jvmProps.NullSafeSet<string>("java_vendor", c => aNode.Machine.Java.Vendor = c);
                    jvmProps.NullSafeSet<int>("sun_arch_data_model", c => aNode.Machine.Java.Model = c, null);
                    jvmProps.NullSafeSet<string>("java_runtime_name", c => aNode.Machine.Java.RuntimeName = c);
                    jvmProps.NullSafeSet<string>("java_runtime_version", c => aNode.Machine.Java.Version = c);
                    jvmProps.NullSafeSet<string>("user_timezone", c => aNode.Machine.TimeZoneName = c);
                    jvmProps.EmptySafeSet<uint>("dse_system_cpu_cores", aNode.Machine.CPU.Cores, c => aNode.Machine.CPU.Cores = c);
                    jvmProps.NullSafeSet<string>("os_version", c => aNode.Machine.Kernel = c);
                    jvmProps.NullSafeSet<string>("os_arch", c => aNode.Machine.CPU.Architecture = c);
                    jvmProps.NullSafeSet<decimal>("dse_system_memory_in_mb",
                                                    c => aNode.Machine.Memory.PhysicalMemory
                                                            = DSEDiagnosticLibrary.UnitOfMeasure.Create(c,
                                                                                                        DSEDiagnosticLibrary.UnitOfMeasure.Types.Memory
                                                                                                            | DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB),
                                                    null);

                    aNode.DSE.Uptime = DSEDiagnosticLibrary.UnitOfMeasure.Create(connection.AnalysisPeriod.Max - nodeConfigDoc.Data.StartupTimeUTC,
                                                                                    DSEDiagnosticLibrary.UnitOfMeasure.Types.Time);

                    aNode.UpdateDSENodeToolDateRange();

                    uint nLine = 0;
                    var filePath = Common.Path.PathUtils.BuildFilePath(DefaultDirectory + "JVM_Properties");

                    foreach (var jItem in jvmProps)
                    {
                       // try
                        {
                            DSEDiagnosticLibrary.YamlConfigurationLine.AddConfiguration(filePath,
                                                                                            aNode,
                                                                                            ++nLine,
                                                                                            jItem.Key,
                                                                                            jItem.Value.ToString(),
                                                                                            DSEDiagnosticLibrary.ConfigTypes.Java,
                                                                                            DSEDiagnosticLibrary.SourceTypes.DSEInsights);
                        }
                       //catch (System.Exception ex)
                       // {
                       //     Logger.Instance.WarnFormat("FileMapper<{2}>\t{0}\t{1}\tJSON Key \"{3}\" at Line {6} throw an exception of \"{4}\" ({5}). Key Ignored",
                       //                                 this.Node.Id,
                       //                                 this.ShortFilePath,
                       //                                 this.MapperId,
                       //                                 jItem.Key,
                       //                                 ex.Message,
                       //                                 ex.GetType().Name,
                       //                                 nLine);
                       // }
                    }

                    //DSE Config
                    filePath = Common.Path.PathUtils.BuildFilePath(DefaultDirectory + "dse.yaml");
                    BuildYamlConfig(aNode, dseConfig, filePath, DSEDiagnosticLibrary.ConfigTypes.DSE);

                    //Cassandra Config
                    filePath = Common.Path.PathUtils.BuildFilePath(DefaultDirectory + "cassandra.yaml");
                    BuildYamlConfig(aNode, cConfig, filePath, DSEDiagnosticLibrary.ConfigTypes.Cassandra);
                }

            }

            return dcList;
        }

        public static IEnumerable<DSEDiagnosticLibrary.IKeyspace> BuildSchema(this Connection connection)
        {            
            var schemaDoc = Insight.BuildCurrentQuery<DSESchemaInformation>(connection.ESClient,
                                                                                    connection.InsightClusterId,
                                                                                    connection.AnalysisPeriod.Max)
                                            .Documents
                                            .First();

            //Keyspaces
            var ksList = connection.BuildKeyspace(schemaDoc);



            return ksList;
        }

        public static IEnumerable<DSEDiagnosticLibrary.IKeyspace> BuildKeyspace(this Connection connection, DSESchemaInformation schemaDoc)
        {
            var ksList = new List<DSEDiagnosticLibrary.IKeyspace>();

            var filePath = Common.Path.PathUtils.BuildFilePath(DefaultDirectory + "Keyspace.cql");
            uint itemNbr = 0;

            foreach (var ksItem in schemaDoc.Data.keyspaces)
            {
                var ksDict = (IReadOnlyDictionary<string, dynamic>)ksItem;
                var ksName = ksDict.TryGetValue<string>("keyspace_name");
                var ksInstance = DSEDiagnosticLibrary.KeySpace.TryGet(connection.Cluster, ksName).FirstOrDefault();

                if (ksInstance == null)
                {
                    var replicationDict = ksDict.TryGetValue<IReadOnlyDictionary<string, dynamic>>("replication");
                    var replClass = replicationDict.TryGetValue<string>("class");
                    var replications = new List<DSEDiagnosticLibrary.KeyspaceReplicationInfo>();
                    var replFactor = replicationDict.TryGetStructValue<int>("replication_factor");

                    if (replFactor.HasValue && replFactor.Value > 0)
                    {
                        var defaultDC = connection.Cluster.DataCenters.FirstOrDefault();

                        replications.Add(new DSEDiagnosticLibrary.KeyspaceReplicationInfo(replFactor.Value, defaultDC));
                    }

                    ksInstance = new DSEDiagnosticLibrary.KeySpace(filePath,
                                                                    ++itemNbr,
                                                                    ksName,
                                                                    replClass,
                                                                    replications,
                                                                    ksDict.TryGetValue<bool>("durable_writes"),
                                                                    ksName + '.' + replClass,
                                                                    null,
                                                                    connection.Cluster,
                                                                    false);
                    ksList.Add(ksInstance);
                }
            }

            return ksList;
        }

        public static void BuildUserDefinedTypes(this Connection connection, DSESchemaInformation schemaDoc)
        {            
            var filePath = Common.Path.PathUtils.BuildFilePath(DefaultDirectory + "UDT.cql");
            uint itemNbr = 0;

            foreach (IReadOnlyDictionary<string,dynamic> udtDict in schemaDoc.Data.types)
            {
                var keyspace = DSEDiagnosticLibrary.KeySpace.TryGet(connection.Cluster, udtDict.TryGetValue<string>("keyspace_name")).FirstOrDefault();
                var udtName = udtDict.TryGetValue<string>("type_name");
                var udtColNames = udtDict.TryGetValues<string>("field_names");
                var udtColTypes = udtDict.TryGetValues<string>("field_types");
            }            
        }


        private static void BuildYamlConfig(DSEDiagnosticLibrary.INode node,
                                            IReadOnlyDictionary<string,dynamic> yamlDir,
                                            IFilePath filePath,
                                            DSEDiagnosticLibrary.ConfigTypes configType,
                                            string parentKey = null,
                                            uint nLine = 0)
        {
            foreach (var cItem in yamlDir)
            {
                var keyName = parentKey == null ? cItem.Key : parentKey + '.' + cItem.Key;

                ++nLine;

                if (cItem.Value is IReadOnlyDictionary<string, dynamic> subDir)
                    BuildYamlConfig(node, subDir, filePath, configType, keyName, nLine);
                else if(cItem.Value != null)
                {
                    var strValue = cItem.Value.ToString();
                    IEnumerable<object> collectItem = null;

                    if (cItem.Value is object[] aItem) collectItem = aItem;
                    else if (cItem.Value is IReadOnlyCollection<object> rItem) collectItem = rItem;

                    if(collectItem != null)
                    {
                        if (collectItem.IsEmpty())
                            continue;

                        strValue = string.Join(",", collectItem.Where(i => i != null));

                        if (cItem.Key.ToLower() == "data_file_directories")
                        {
                            node.DSE.Locations.DataDirs = collectItem.Select(i => i.ToString());
                        }
                    }

                    SetNodeAttribuesFromConfig(node, cItem.Key, strValue);

                    DSEDiagnosticLibrary.YamlConfigurationLine.AddConfiguration(filePath,
                                                                                node,
                                                                                nLine,
                                                                                keyName,
                                                                                strValue,
                                                                                configType,
                                                                                DSEDiagnosticLibrary.SourceTypes.DSEInsights);
                }
            }
        }

        static void SetNodeAttribuesFromConfig(DSEDiagnosticLibrary.INode node, string key, string value)
        {
            switch (key.ToLower())
            {                
                case "num_tokens":
                    if (!node.DSE.NbrTokens.HasValue)
                    {
                        uint nbrTokens;
                        if (uint.TryParse(value, out nbrTokens))
                        {
                            node.DSE.NbrTokens = nbrTokens;
                            node.DSE.VNodesEnabled = nbrTokens > 1;
                        }
                    }
                    break;
                case "hints_directory":
                    node.DSE.Locations.HintsDir = value;
                    break;
                case "commitlog_directory":
                    node.DSE.Locations.CommitLogDir = value;
                    break;
                case "saved_caches_directory":
                    node.DSE.Locations.SavedCacheDir = value;
                    break;
                case "cassandra-conf":
                    node.DSE.Locations.CassandraYamlFile = value;
                    break;
                case "endpoint_snitch":
                    node.DSE.EndpointSnitch = value;
                    break;
                case "advanced_replication_options.enabled":
                    if (value.ToLower() == "true" || value == "1")
                    {
                        node.DSE.InstanceType |= DSEDiagnosticLibrary.DSEInfo.InstanceTypes.AdvancedReplication;
                    }
                    break;
                case "hadoop_enabled":
                    if (value == "1")
                    {
                        node.DSE.InstanceType |= DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Hadoop;
                    }
                    break;
                case "graph_enabled":
                    if (value == "1")
                    {
                        node.DSE.InstanceType |= DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Graph;
                    }
                    break;
                case "solr_enabled":
                    if (value == "1")
                    {
                        node.DSE.InstanceType |= DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Search;
                    }
                    break;
                case "spark_enabled":
                    if (value == "1")
                    {
                        node.DSE.InstanceType |= DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Analytics;
                    }
                    break;
                case "cfs_enabled":
                    if (value == "1")
                    {
                        node.DSE.InstanceType |= DSEDiagnosticLibrary.DSEInfo.InstanceTypes.CFS;
                    }
                    break;
                case "partitioner":
                    if(node.DSE.Partitioner == null)
                        node.DSE.Partitioner = value;
                    break;
                case "server_id":
                    node.DSE.PhysicalServerId = value;                                        
                    break;
                case "seeds":
                    if(!string.IsNullOrEmpty(value))
                    {
                        node.DSE.IsSeedNode = DSEDiagnosticLibrary.StringHelpers.RemoveQuotes(value, false)
                                                .Split(',')
                                                .Any(s => node.Id.Equals(s));
                    }
                    break;
                default:
                    break;
            }
        }

    }
}
