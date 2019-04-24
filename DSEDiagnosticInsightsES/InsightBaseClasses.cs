using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Common;
using Newtonsoft;
using Newtonsoft.Json;
using DSEDiagnosticInsightsES;

namespace DSEDiagnosticInsights
{

    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false)]
    public class InsightESAttribute : ESQueryAttribute
    {
        public InsightESAttribute(string insightName, bool createESIndex = true)
            : base(insightName,
                    DSEDiagnosticInsightsES.Properties.Settings.Default.ESInsightClusterIdField,
                    DSEDiagnosticInsightsES.Properties.Settings.Default.ESInsightTimestampField,
                    insightName,
                    null,
                    createESIndex)
        {
        }

        public InsightESAttribute(string insightName, string esIndexName, bool createESIndex = true)
            : base(insightName,
                    DSEDiagnosticInsightsES.Properties.Settings.Default.ESInsightClusterIdField,
                    DSEDiagnosticInsightsES.Properties.Settings.Default.ESInsightTimestampField,
                    esIndexName,
                    null,
                    createESIndex)
        {
        }

        public InsightESAttribute(string esIndexName,
                                    string queryFieldName,
                                    string queryTimestampFieldName,
                                    bool createESIndex = true,
                                    string insightName = null)
            : base(insightName ?? esIndexName,
                    queryFieldName,
                    queryTimestampFieldName,
                    esIndexName,
                    null,
                    createESIndex)
        {
        }
    }

    public interface IInsightPayload
    {
    }

    public interface IInsightClass
    {
    }

    public abstract class Insight : ESQuerySearch, IInsightClass
    {
        public static readonly string ESClusterIdTerm = DSEDiagnosticInsightsES.Properties.Settings.Default.ESInsightClusterIdField;

        public Insight() { }
        
        [JsonProperty("metadata")]
        public InsightMetadata MetaData;
       
        public abstract object GetData();
        
        public static Nest.ISearchResponse<T> BuildQuery<T>(Nest.IElasticClient elasticClient, Guid clusterId, int? nbrDocs = null)
            where T : Insight
        {
            return ESQuerySearch.BuildQuery<T>(elasticClient, clusterId, nbrDocs);
        }

        public static Nest.ISearchResponse<T> BuildQuery<T>(Nest.IElasticClient elasticClient, Guid clusterId, DateTimeOffset forTimeFrame, int? nbrDocs = null)
            where T : Insight
        {
            return ESQuerySearch.BuildQuery<T>(elasticClient, clusterId, forTimeFrame, nbrDocs);
        }

        public static Nest.ISearchResponse<T> BuildQuery<T>(Nest.IElasticClient elasticClient, Guid clusterId, DateTimeOffsetRange searchRange, int? nbrDocs = null)
            where T : Insight
        {
            return ESQuerySearch.BuildQuery<T>(elasticClient, clusterId, searchRange, nbrDocs);
        }

        public static Nest.ISearchResponse<T> BuildCurrentQuery<T>(Nest.IElasticClient elasticClient, Guid clusterId, DateTimeOffset? useAsCurrent = null, int? nbrDocs = 1)
             where T : Insight
        {
            return ESQuerySearch.BuildCurrentQuery<T>(elasticClient, clusterId, useAsCurrent, nbrDocs);
        }        
    }

    public abstract class Insight<P> : Insight
        where P : IInsightPayload, new()
    {        
        public Insight()          
        { }
        
        [JsonProperty("data")]
        public P Data;

        public override object GetData() { return this.Data; }        
    }

    public enum InsightType

    {
        EVENT,
        GAUGE,
        COUNTER,
        HISTOGRAM,
        TIMER,
        METER,
        LOG
    }

    public class InsightMetadata
    {
        public InsightMetadata() { }
        public InsightMetadata(string name,
                                long? timestamp = null,
                                IReadOnlyDictionary<string, dynamic> tags = null,
                                InsightType? insightType = null,
                                string insightMappingId = null)
        {
            this.name = name;
            this.timestamp = timestamp;
            this.tags = tags;
            this.insightType = insightType;
            this.insightMappingId = insightMappingId;
        }

        public string name;
        public long? timestamp;

        [JsonIgnore]
        public DateTimeOffset? CreationTimeUTC
        {
            get
            {
                return this.timestamp.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(this.timestamp.Value) : (DateTimeOffset?) null;
            }
        }

        [JsonConverter(typeof(PropDictSerializer))]
        public IReadOnlyDictionary<string, dynamic> tags;
        public InsightType? insightType;
        public string insightMappingId;
    }
    
}
