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
    [InsightESAttribute("dse.insights.event.dropped_messages", "dse.insights.event.dropped_messages-searchable-events")]
    public sealed class DroppedMessageInformation : Insight<DroppedMessageInformation.DroppedMessagInstance>
    {

        public sealed class DroppedMessagInstance : IInsightPayload
        {
            [JsonProperty("group_stats")]
            public DroppedMessageGroupInsight[] droppedMessageGroupStats;
        }

        public sealed class DroppedMessageGroupInsight
        {
            [JsonProperty("group_name")]
            public string Group;

            [JsonProperty("reporting_interval_seconds")]
            public int ReportingIntervalSeconds;

            [JsonProperty("num_internal")]
            public int InternalDropped;

            [JsonProperty("num_cross_node")]
            public int CrossNodeDropped;

            [JsonProperty("internal_latency_ms")]
            public long InternalLatencyMs;

            [JsonProperty("crossnode_latency_ms")]
            public long CrossNodeLatencyMs;
        }
    }
}
