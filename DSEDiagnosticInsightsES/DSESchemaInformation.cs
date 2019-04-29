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
    [InsightESAttribute("dse.insights.events.schema_information", "dse.insights.events.schema_information-searchable-events")]
    public sealed class DSESchemaInformation : Insight<DSESchemaInformation.Schema>
    {
        public sealed class Schema : IInsightPayload
        {
            [JsonProperty("keyspaces")]
            [JsonConverter(typeof(PropDictArraySerializer))]
            public object[] keyspaces;

            [JsonProperty("tables")]
            [JsonConverter(typeof(PropDictArraySerializer))]
            public object[] tables;

            [JsonProperty("columns")]
            [JsonConverter(typeof(PropDictArraySerializer))]
            public object[] columns;

            [JsonProperty("dropped_columns")]
            [JsonConverter(typeof(PropDictArraySerializer))]
            public object[] dropped_columns;

            [JsonProperty("triggers")]
            [JsonConverter(typeof(PropDictArraySerializer))]
            public object[] triggers;

            [JsonProperty("views")]
            [JsonConverter(typeof(PropDictArraySerializer))]
            public object[] views;

            [JsonProperty("types")]
            [JsonConverter(typeof(PropDictArraySerializer))]
            public object[] types;

            [JsonProperty("functions")]
            [JsonConverter(typeof(PropDictArraySerializer))]
            public object[] functions;

            [JsonProperty("aggregates")]
            [JsonConverter(typeof(PropDictArraySerializer))]
            public object[] aggregates;

            [JsonProperty("indexes")]
            [JsonConverter(typeof(PropDictArraySerializer))]
            public object[] indexes;
        }
    }
}
