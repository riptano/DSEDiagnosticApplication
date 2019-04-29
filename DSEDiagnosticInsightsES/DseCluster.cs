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
    [InsightESAttribute("dse-clusters", "clusterId.keyword", "createdTime")]
    public sealed class DseCluster : IInsightClass
    {

        private static readonly ESQueryAttribute ESIQAttrib = (ESQueryAttribute)typeof(DseCluster).GetCustomAttribute(typeof(ESQueryAttribute));

        public static Nest.ISearchResponse<DseCluster> BuildQuery(Nest.IElasticClient elasticClient, Guid clusterId, int? nbrDocs = null)
        {
            return ESQuerySearch.BuildQuery<DseCluster>(elasticClient, clusterId, nbrDocs, ESIQAttrib);
        }

        public static Nest.ISearchResponse<DseCluster> BuildQuery(Nest.IElasticClient elasticClient, int? nbrDocs = null)
        {
            return ESQuerySearch.BuildQuery<DseCluster>(elasticClient, nbrDocs, ESIQAttrib);
        }

        public static Nest.ISearchResponse<DseCluster> BuilCurrentQuery(Nest.IElasticClient elasticClient,
                                                                        Guid clusterId,
                                                                        DateTimeOffset? useAsCurrent = null,
                                                                        int? nbrDocs = 1)
        {
            return ESQuerySearch.BuildCurrentQuery<DseCluster>(elasticClient, clusterId, useAsCurrent, nbrDocs, ESIQAttrib);
        }

        [JsonProperty("clusterId")]
        public Guid clusterId;

        [JsonProperty("createdTime")]
        public long createdTime;

        [JsonIgnore]
        public DateTimeOffset CreationTimeUTC
        {
            get
            {
                return DateTimeOffset.FromUnixTimeMilliseconds(this.createdTime);
            }
        }

        [JsonProperty("nodes")]
        public IEnumerable<Node> nodes;

        [JsonProperty("keyspaces")]
        public IEnumerable<Keyspace> keyspaces;
    }


    public sealed class Node
    {

        [JsonProperty("clientId")]
        public Guid clientId;

        [JsonProperty("hostId")]
        public Guid? hostId;

        [JsonProperty("dataCenter")]
        public string dataCenter;

        [JsonProperty("rack")]
        public string rack;

        [JsonProperty("server")]
        public string server;

    }

    public sealed class Keyspace
    {

        [JsonProperty("name")]
        public string name;

        [JsonProperty("createdTime")]
        public long createdTime;

        [JsonIgnore]
        public DateTimeOffset CreationTimeUTC
        {
            get
            {
                return DateTimeOffset.FromUnixTimeMilliseconds(this.createdTime);
            }
        }

        [JsonProperty("tables")]
        public IEnumerable<Table> tables;
    }

    public sealed class Table
    {

        [JsonProperty("name")]
        public string name;

        [JsonProperty("createdTime")]
        public long createdTime;

        [JsonIgnore]
        public DateTimeOffset CreationTimeUTC
        {
            get
            {
                return DateTimeOffset.FromUnixTimeMilliseconds(this.createdTime);
            }
        }
    }
}
