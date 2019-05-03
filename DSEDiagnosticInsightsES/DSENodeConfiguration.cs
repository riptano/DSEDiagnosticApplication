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
    [InsightESAttribute("dse.insights.event.node_configuration", "dse-node-configs")]
    public sealed class DSENodeConfiguration : Insight<DSENodeConfiguration.Configuration>
    {

        public sealed class Configuration : IInsightPayload
        {
            [JsonConverter(typeof(PropDictSerializer))]
            public IReadOnlyDictionary<string, dynamic> dse_config;
            [JsonConverter(typeof(PropDictSerializer))]
            public IReadOnlyDictionary<string, dynamic> cassandra_config;
            public string jvm_version;
            public string jvm_vendor;
            public long jvm_memory_max;
            public IReadOnlyList<string> jvm_final_flags;
            public string jvm_classpath;
            [JsonConverter(typeof(PropDictSerializer))]
            public IReadOnlyDictionary<string, dynamic> jvm_properties;
            public long jvm_startup_time;

            [JsonIgnore]
            public DateTimeOffset StartupTimeUTC
            {
                get
                {
                    return DateTimeOffset.FromUnixTimeMilliseconds(this.jvm_startup_time);
                }
            }

            public string os;
            public string hostname;
            [JsonConverter(typeof(PropDictArraySerializer))]
            public IReadOnlyDictionary<string, dynamic>[] cpu_layout;
        }

        private static readonly InsightESAttribute ESAttrib = (InsightESAttribute)typeof(DSENodeConfiguration).GetCustomAttribute(typeof(InsightESAttribute));

        public static Nest.ISearchResponse<DSENodeConfiguration> BuilCurrentQueryNodeClientId(Nest.IElasticClient elasticClient,
                                                                                                Guid nodeClientId,
                                                                                                DateTimeOffset? useAsCurrent = null,
                                                                                                int? nbrDocs = 1)
        {           
            return elasticClient.Search<DSENodeConfiguration>(s => ESQuerySearch.BuildCurrentQuery<DSENodeConfiguration>(s.Index(ESAttrib.ESIndex),
                                                                                                                            "metadata.tags.clientId.keyword",
                                                                                                                            ESAttrib.QueryTimestampFieldName,
                                                                                                                            nodeClientId,
                                                                                                                            useAsCurrent,
                                                                                                                            nbrDocs));
        }
    }
}
