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

    public sealed class SSTableCompactionInformation
    {
        [JsonProperty("filename")]
        public string Filename;

        [JsonProperty("level")]
        public int Level;

        [JsonProperty("total_rows")]
        public long TotalRows;

        [JsonProperty("generation")]
        public int Generation;

        [JsonProperty("version")]
        public string Version;

        [JsonProperty("size_bytes")]
        public long SizeBytes;

        [JsonProperty("strategy")]
        public string Strategy;

    }

    [InsightESAttribute("dse.insights.event.compaction_ended", "dse.insights.event.compaction_ended-searchable-events")]
    public class CompactionEndedInformation : Insight<CompactionEndedInformation.CompactionInstance>
    {

        public sealed class CompactionInstance : IInsightPayload
        {
            [JsonProperty("id")]
            public Guid Id;

            [JsonProperty("keyspace")]
            public string Keyspace;

            [JsonProperty("table")]
            public string Table;

            [JsonProperty("type")]
            public string Type;

            [JsonProperty("compacted_bytes")]
            public long CompactedBytes;

            [JsonProperty("total_bytes")]
            public long TotalBytes;

            [JsonProperty("is_stop_requested")]
            public bool IsStopRequested;

            [JsonProperty("total_source_cql_rows")]
            public long TotalSourceCQLRows;

            [JsonProperty("total_sstable_size_bytes")]
            public long TotalSSTableSizeBytes;

            [JsonProperty("sstables")]
            public SSTableCompactionInformation[] SSTables;
        }
    }

    [InsightESAttribute("dse.insights.event.compaction_started", "dse.insights.event.compaction_started-searchable-events")]
    public sealed class CompactionStartedInformation : Insight<CompactionEndedInformation.CompactionInstance>
    {
    }

    [InsightESAttribute("dse-compaction-events", "dse-compaction-events-searchable-events")]
    public sealed class DseCompaction : Insight
    {
        [JsonProperty("id")]
        public Guid Id;

        [JsonProperty("type")]
        public string Type;
        
        [JsonProperty("total_bytes")]
        public long TotalBytes;

        [JsonProperty("is_stop_requested")]
        public bool IsStopRequested;

        [JsonProperty("started_timestamp")]
        public long StartedTimestamp;

        [JsonIgnore]
        public DateTimeOffset StartedTimeUTC
        {
            get
            {
                return DateTimeOffset.FromUnixTimeMilliseconds(this.StartedTimestamp);
            }
        }

        [JsonProperty("started_total_sstable_size_bytes")]
        public long StartedTotalSstableSizeBytes;

        [JsonProperty("started_sstables")]
        public SSTableCompactionInformation[] StartedSsTables;
    
        [JsonProperty("ended_timestamp")]
        public long EndedTimestamp;

        [JsonIgnore]
        public DateTimeOffset EndedTimeUTC
        {
            get
            {
                return DateTimeOffset.FromUnixTimeMilliseconds(this.EndedTimestamp);
            }
        }

        [JsonProperty("ended_total_sstable_size_bytes")]
        public long EndedTotalSstableSizeBytes;

        [JsonProperty("ended_total_source_cql_rows")]
        public int EndedTotalSourceCqlRows;
        
        [JsonProperty("ended_sstables")]
        public SSTableCompactionInformation[] EndedSsTables;

        [JsonIgnore]
        public TimeSpan Duration
        {
            get
            {
                return this.EndedTimeUTC - this.StartedTimeUTC;
            }
        }

        public override object GetData()
        {
            throw new NotImplementedException();
        }
    }
}
