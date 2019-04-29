using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using Common;
using Nest;
using Nest.JsonNetSerializer;
using Nest.JsonNetSerializer.Converters;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using DSEDiagnosticInsightsES;

namespace DSEDiagnosticInsights
{
    [InsightESAttribute("dse-nodes", "clusterId", "update_ts")]
    public sealed class DseNode : IInsightClass
    {
        private static readonly ESQueryAttribute ESIQAttrib = (ESQueryAttribute)typeof(DseNode).GetCustomAttribute(typeof(ESQueryAttribute));

        public DseNode() { }

        public static Nest.ISearchResponse<DseNode> BuildQuery(Nest.IElasticClient elasticClient, Guid clusterId, int? nbrDocs = null)
        {
            return ESQuerySearch.BuildQuery<DseNode>(elasticClient, clusterId, nbrDocs, ESIQAttrib);
        }

        public static Nest.ISearchResponse<DseNode> BuildQuery(Nest.IElasticClient elasticClient, string clusterName, int? nbrDocs = null)
        {
            return elasticClient.Search<DseNode>(s => ESQuerySearch.BuildQuery<DseNode>(s.Index(ESIQAttrib.ESIndex), "cluster_name", ESIQAttrib.QueryTimestampFieldName, clusterName, nbrDocs));
        }

        public static Nest.ISearchResponse<DseNode> BuildQuery(Nest.IElasticClient elasticClient, int? nbrDocs = null)
        {
            return ESQuerySearch.BuildQuery<DseNode>(elasticClient, nbrDocs, ESIQAttrib);
        }

        public static Nest.ISearchResponse<DseNode> BuilCurrentQuery(Nest.IElasticClient elasticClient,
                                                                        Guid clusterId,
                                                                        DateTimeOffset? useAsCurrent = null,
                                                                        int? nbrDocs = 1)
        {
            return ESQuerySearch.BuildCurrentQuery<DseNode>(elasticClient, clusterId, useAsCurrent, nbrDocs, ESIQAttrib);
        }

        public static Nest.ISearchResponse<DseNode> BuilCurrentQuery(Nest.IElasticClient elasticClient,
                                                                        string clusterName,
                                                                        DateTimeOffset? useAsCurrent = null,
                                                                        int? nbrDocs = 1)
        {
            return elasticClient.Search<DseNode>(s => ESQuerySearch.BuildCurrentQuery<DseNode>(s.Index(ESIQAttrib.ESIndex),
                                                                                                    "cluster_name",
                                                                                                    ESIQAttrib.QueryTimestampFieldName,
                                                                                                    clusterName,
                                                                                                    useAsCurrent,
                                                                                                    nbrDocs));
        }


        public string bootstrap_status;
        public string cluster_name;
        public Guid clientId;
        public Guid clusterId;
        public string cql_version;
        public string data_center;
        public string dse_version;
        public bool graph;
        public Guid host_id;
        public string native_protocol_version;
        public string partitioner;
        public IEnumerable<Guid> peers;
        public string rack;
        public string cassandra_version;
        public string schema_version;
        public string server_id;
        public IEnumerable<string> tokens;
        public long update_ts;
        public string workload;
        public IEnumerable<string> workloads;
        public string native_transport_address;
        public int? storagePortSsl;
        public int? storagePort;
        public int? native_transport_port;
        public int? native_Transport_port_ssl;
        public string broadcast_address;
        public long? gossip_generation;
        public string key;
        public string listen_address;
        public string rpc_address;
    }
}
