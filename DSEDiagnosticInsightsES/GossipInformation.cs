using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Nest;
using Nest.JsonNetSerializer;
using Nest.JsonNetSerializer.Converters;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using DSEDiagnosticInsightsES;

namespace DSEDiagnosticInsights
{
    [InsightESAttribute("dse.insights.event.gossip_change", "dse.insights.event.gossip_change-searchable-events")]
    public sealed class GossipChangeInformation : Insight<GossipChangeInformation.GossipInstance>
    {

        public enum GossipEventType
        {
            JOINED,
            REMOVED,
            ALIVE,
            DEAD,
            RESTARTED
        }

        public sealed class GossipInstance : IInsightPayload
        {
            [JsonProperty("event_type")]
            public GossipEventType eventType;

            [JsonProperty("endpoint_state")]
            [JsonConverter(typeof(PropDictSerializer))]
            public IReadOnlyDictionary<string, dynamic> EndpointState;

            [JsonProperty("endpoint_address")]
            public string IPAddress;
        }
    }
}
