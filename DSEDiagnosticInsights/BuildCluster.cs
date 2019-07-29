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
                                            .FirstOrDefault();

            if (schemaDoc == null) return Enumerable.Empty<DSEDiagnosticLibrary.IKeyspace>();

            //Keyspaces
            var ksList = connection.BuildKeyspace(schemaDoc);

            connection.BuildUserDefinedTypes(schemaDoc);
            connection.BuildUserFunctions(schemaDoc);
            connection.BuildTablesViews(schemaDoc);
            connection.BuildIndexes(schemaDoc);

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
                    var replFactor = replicationDict.TryGetStructValue<ushort>("replication_factor");
                    var durableWrites = ksDict.TryGetValue<bool>("durable_writes");

                    var replications = new List<DSEDiagnosticLibrary.KeyspaceReplicationInfo>();
                    var ddlDCReplFactor = new List<Tuple<string, ushort>>();

                    if (replFactor.HasValue && replFactor.Value > 0)
                    {
                        var defaultDC = connection.Cluster.DataCenters.FirstOrDefault();

                        replications.Add(new DSEDiagnosticLibrary.KeyspaceReplicationInfo(replFactor.Value, defaultDC));
                        ddlDCReplFactor.Add(new Tuple<string, ushort>(null, replFactor.Value));
                    }
                    else
                    {
                        foreach (var item in replicationDict)
                        {
                            if(item.Key == "class" || item.Key == "replication_factor") continue;

                            var dcName = item.Key;
                            var dcInstance = connection.Cluster.TryGetDataCenter(dcName);
                            ushort replFactorValue;

                            if(item.Value is string strRepl && Common.StringFunctions.ParseIntoNumeric(strRepl, out object objRepl))
                            {
                                replFactorValue = (ushort)((dynamic)objRepl);
                            }
                            else
                            {
                                replFactorValue = (ushort)item.Value;
                            }

                            replications.Add(new DSEDiagnosticLibrary.KeyspaceReplicationInfo(replFactorValue, dcInstance));
                            ddlDCReplFactor.Add(new Tuple<string, ushort>(dcName, replFactorValue));
                        }
                    }

                    ksInstance = new DSEDiagnosticLibrary.KeySpace(filePath,
                                                                    ++itemNbr,
                                                                    ksName,
                                                                    replClass,
                                                                    replications,
                                                                    durableWrites,
                                                                    DSEDiagnosticCQLSchema.Parser.GenerateKeyspaceDDLString(ksName,
                                                                                                                            replClass,
                                                                                                                            ddlDCReplFactor,
                                                                                                                            durableWrites),
                                                                    null,
                                                                    connection.Cluster,
                                                                    false);

                    DSEDiagnosticCQLSchema.Parser.AddKeySpace(connection.Cluster, ksInstance, ksList);                    
                }
            }

            return ksList;
        }

        public static void BuildIndexes(this Connection connection, DSESchemaInformation schemaDoc)
        {
            var filePath = Common.Path.PathUtils.BuildFilePath(DefaultDirectory + "Index.cql");
            uint itemNbr = 0;
            var indexes = from idxDict in schemaDoc.Data.indexes.Cast<IReadOnlyDictionary<string, dynamic>>()
                                   let ksName = idxDict.TryGetValue<string>("keyspace_name")
                                   let tblName = idxDict.TryGetValue<string>("table_name")
                                   let idxName = idxDict.TryGetValue<string>("index_name")
                                   let idxKind = idxDict.TryGetValue<string>("kind")
                                   let options = idxDict.TryGetValue<IReadOnlyDictionary<string,object>>("options")
                                   let ksInstance = DSEDiagnosticLibrary.KeySpace.TryGet(connection.Cluster, ksName).FirstOrDefault()
                                   select new
                                   {
                                       keyspace = ksInstance,
                                       tblName,
                                       idxName,
                                       isCustom = idxKind == "CUSTOM",
                                       options,
                                       target = options.TryGetValue<string>("target"),
                                       idxclass = options.TryGetValue<string>("class_name")
                                   };

            foreach (var idxItem in indexes)
            {
                var colunms = new List<string>() { idxItem.target };
                DSEDiagnosticCQLSchema.Parser.ProcessDDLIndex(idxItem.keyspace,
                                                                idxItem.idxName,
                                                                idxItem.keyspace,
                                                                idxItem.tblName,
                                                                colunms,
                                                                idxItem.idxclass,
                                                                null,
                                                                idxItem.isCustom,
                                                                DSEDiagnosticCQLSchema.Parser.GenerateIndexDDLString(idxItem.idxName,
                                                                                                                        idxItem.idxclass,
                                                                                                                        colunms,
                                                                                                                        idxItem.tblName,
                                                                                                                        idxItem.options),
                                                                filePath,
                                                                null,
                                                                ++itemNbr,
                                                                out DSEDiagnosticLibrary.CQLIndex idxInstance,                                                                
                                                                withDict: idxItem.options
                                                                );
            }
        }

        public static void BuildUserDefinedTypes(this Connection connection, DSESchemaInformation schemaDoc)
        {            
            var filePath = Common.Path.PathUtils.BuildFilePath(DefaultDirectory + "UDT.cql");
            uint itemNbr = 0;
            var orderedTypes = from udtDict in schemaDoc.Data.types.Cast<IReadOnlyDictionary<string, dynamic>>()
                               let ksName = udtDict.TryGetValue<string>("keyspace_name")
                               let udtColNames = udtDict.TryGetValues<string>("field_names")
                               let udtColTypes = udtDict.TryGetValues<string>("field_types")
                               let cntSubTypes = udtColTypes.Count(i => i.Contains("<"))
                               orderby ksName ascending, cntSubTypes ascending
                               select new
                               {
                                   keyspace = DSEDiagnosticLibrary.KeySpace.TryGet(connection.Cluster, ksName).FirstOrDefault(),
                                   name = udtDict.TryGetValue<string>("type_name"),
                                   columns = udtColNames.Select(udtColTypes, (s1, s2) => s1 + ' ' + s2).ToList(),
                                   cntSubTypes
                               };
                               


            foreach (var udtItem in orderedTypes)
            {               
                DSEDiagnosticCQLSchema.Parser.ProcessDDLUDT(udtItem.keyspace,
                                                                udtItem.name,
                                                                udtItem.columns,
                                                                DSEDiagnosticCQLSchema.Parser.GenerateUDTDDLString(udtItem.keyspace.Name, udtItem.name, udtItem.columns),
                                                                filePath,
                                                                null,
                                                                ++itemNbr,
                                                                out DSEDiagnosticLibrary.CQLUserDefinedType udtInstance);               
            }       
        }

        public static void BuildUserFunctions(this Connection connection, DSESchemaInformation schemaDoc)
        {
            var filePath = Common.Path.PathUtils.BuildFilePath(DefaultDirectory + "Function.cql");
            uint itemNbr = 0;
            var orderedFunctions = from udtDict in schemaDoc.Data.functions.Cast<IReadOnlyDictionary<string, dynamic>>()
                                       let ksName = udtDict.TryGetValue<string>("keyspace_name")
                                       let udtColNames = udtDict.TryGetValues<string>("argument_names")
                                       let udtColTypes = udtDict.TryGetValues<string>("argument_types")
                                       let cntSubTypes = udtColTypes.Count(i => i.Contains("<"))
                                       orderby ksName ascending, cntSubTypes ascending
                                       select new
                                       {
                                           keyspace = DSEDiagnosticLibrary.KeySpace.TryGet(connection.Cluster, ksName).FirstOrDefault(),
                                           name = udtDict.TryGetValue<string>("function_name"),
                                           columns = udtColNames.Select(udtColTypes, (s1, s2) => s1 + ' ' + s2).ToList(),
                                           cntSubTypes
                                       };



            foreach (var funcItem in orderedFunctions)
            {
                DSEDiagnosticCQLSchema.Parser.ProcessDDLUDT(funcItem.keyspace,
                                                                funcItem.name,
                                                                funcItem.columns,
                                                                funcItem.name + ' ' + string.Join(",", funcItem.columns),
                                                                filePath,
                                                                null,
                                                                ++itemNbr,
                                                                out DSEDiagnosticLibrary.CQLUserDefinedType udtInstance);
            }
        }

        public static void BuildTablesViews(this Connection connection, DSESchemaInformation schemaDoc)
        {
            var filePath = Common.Path.PathUtils.BuildFilePath(DefaultDirectory + "Table.cql");
            uint itemNbr = 0;

            var tblDicts = schemaDoc.Data.tables.Cast<IReadOnlyDictionary<string, dynamic>>()
                            .Concat(schemaDoc.Data.views.Cast<IReadOnlyDictionary<string, dynamic>>())
                            .Select(d =>
                            {
                                var viewName = d.TryGetValue<string>("view_name");
                                return new
                                {
                                    ksName = d.TryGetValue<string>("keyspace_name"),
                                    tblName = viewName == null ? d.TryGetValue<string>("table_name") : viewName,
                                    baseTblName = d.TryGetValue<string>("base_table_name"),
                                    isView = viewName != null,
                                    dist = d
                                };
                            })
                            .ToArray();
            var tblCols = from colDict in schemaDoc.Data.columns.Cast<IReadOnlyDictionary<string, dynamic>>()
                          let ksName = colDict.TryGetValue<string>("keyspace_name")
                          let tblName = colDict.TryGetValue<string>("table_name") ?? colDict.TryGetValue<string>("view_name")
                          select new
                          {
                              ksName,
                              tblName,
                              colName = colDict.TryGetValue<string>("column_name"),
                              clusterOrder = colDict.TryGetValue<string>("clustering_order"),
                              kind = colDict.TryGetValue<string>("kind"),                              
                              pos = colDict.TryGetValue<int>("position"),
                              type = colDict.TryGetValue<string>("type"),
                              Props = colDict
                          };

            var groupedTblCols = from colItem in tblCols
                              group colItem by new { colItem.ksName, colItem.tblName } into grpTbl
                              orderby grpTbl.Key.ksName, grpTbl.Key.tblName
                              let tableDict = tblDicts.FirstOrDefault(d => d.ksName == grpTbl.Key.ksName && d.tblName == grpTbl.Key.tblName)
                              let ksInstance = DSEDiagnosticLibrary.KeySpace.TryGet(connection.Cluster, grpTbl.Key.ksName).FirstOrDefault()
                              select new
                              {
                                  grpTbl.Key.ksName,
                                  grpTbl.Key.tblName,
                                  Keyspace = ksInstance,
                                  BaseTable = tableDict.baseTblName == null ? null : ksInstance?.TryGetTable(tableDict.baseTblName),
                                  IsView = tableDict?.isView ?? false,
                                  tableDict,
                                  columns = (from col in grpTbl                                             
                                             select new DSEDiagnosticCQLSchema.Parser.ColumnDefination
                                             {
                                                 Name = col.colName,
                                                 Type = col.type,
                                                 Kind = col.kind == "partition_key"
                                                            ? DSEDiagnosticCQLSchema.Parser.ColumnDefination.Kinds.PartitionKey
                                                            : (col.kind == "clustering"
                                                                    ? DSEDiagnosticCQLSchema.Parser.ColumnDefination.Kinds.ClustingKey
                                                                    : DSEDiagnosticCQLSchema.Parser.ColumnDefination.Kinds.Regular),
                                                 KindPos = col.pos,
                                                 OrderBy = string.IsNullOrEmpty(col.clusterOrder) || col.clusterOrder == "none"
                                                                ? DSEDiagnosticCQLSchema.Parser.ColumnDefination.OrderByDirection.None
                                                                : (col.clusterOrder == "asc"
                                                                        ? DSEDiagnosticCQLSchema.Parser.ColumnDefination.OrderByDirection.Ascending
                                                                        : DSEDiagnosticCQLSchema.Parser.ColumnDefination.OrderByDirection.Descending)
                                             })
                              };


            foreach (var tblvwItem in groupedTblCols)
            {
                if (tblvwItem.IsView)
                {
                    DSEDiagnosticCQLSchema.Parser.ProcessDDLView(tblvwItem.Keyspace,
                                                                    tblvwItem.tblName,
                                                                    tblvwItem.BaseTable,
                                                                    tblvwItem.columns,
                                                                    tblvwItem.tableDict.dist,
                                                                    tblvwItem.tableDict.dist.TryGetValue<string>("where_clause"),
                                                                    filePath,
                                                                    ++itemNbr,
                                                                    null,
                                                                    out DSEDiagnosticLibrary.CQLMaterializedView view);                                                                    
                }
                else
                {
                    DSEDiagnosticCQLSchema.Parser.ProcessDDLTable(tblvwItem.Keyspace,
                                                                    tblvwItem.tblName,
                                                                    tblvwItem.columns,
                                                                    tblvwItem.tableDict.dist,
                                                                    filePath,
                                                                    ++itemNbr,
                                                                    null,
                                                                    out DSEDiagnosticLibrary.CQLTable table);
                }
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
