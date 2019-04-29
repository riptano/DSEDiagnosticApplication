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
    [InsightESAttribute("dse.insights.event.flush", "dse.insights.event.flush-searchable-events")]
    public sealed class FlushInformation : Insight<FlushInformation.FlushInstance>
    {

        public sealed class FlushInstance : IInsightPayload
        {
            [JsonProperty("keyspace")]
            public string Keyspace;

            [JsonProperty("table")]
            public string Table;

            [JsonProperty("flush_reason")]
            public string Reason;

            [JsonProperty("is_truncate")]
            public bool Truncate;

            [JsonProperty("memtable_memory_use")]
            public MemoryUsage MemtableMemoryUsage;

            [JsonProperty("memtable_live_data_size")]
            public long MemtableLiveDataSize;

            [JsonProperty("flush_duration_millis")]
            public long FlushDurationMillis;

            [JsonProperty("sstables_flushed")]
            public int SStablesFlushed;

            [JsonProperty("sstable_disk_size")]
            public long SStableBytesOnDisk;

            [JsonProperty("partitions_flushed")]
            public long PartitonsFlushed;
        }

        public sealed class MemoryUsage
        {
            [JsonProperty("ownershipRatioOnHeap")]
            public decimal OwnershipRatioOnHeap;

            [JsonProperty("ownershipRatioOffHeap")]
            public decimal OwnershipRatioOffHeap;

            [JsonProperty("ownsOnHeap")]
            public long OwnsOnHeap;

            [JsonProperty("ownsOffHeap")]
            public long OwnsOffHeap;
        }

    }
}
