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
    [InsightESAttribute("dse.insights.event.gc_information", "dse.insights.event.gc_information-searchable-events")]
    public sealed class GCInformation : Insight<GCInformation.GCInstance>
    {

        public sealed class GCInstance : IInsightPayload
        {
            [JsonProperty("gc_name")]
            public string GCName;

            [JsonProperty("duration")]
            public long Duration;

            [JsonProperty("memory_usage_before")]
            public IReadOnlyDictionary<string, long> MemoryUsageBefore;

            [JsonProperty("memory_usage_after")]
            public IReadOnlyDictionary<string, long> MemoryUsageAfter;

            [JsonProperty("promoted_bytes")]
            public long PromotedBytes;

            [JsonProperty("young_gc_cpu_nanos")]
            public long YoungGcCpuNanos;

            [JsonProperty("old_gc_cpu_nanos")]
            public long OldGcCpuNanos;
        }
    }
}
