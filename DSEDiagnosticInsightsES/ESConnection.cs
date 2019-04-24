using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using Common;
using CPC = Common.Patterns.Collections;
using Nest;
using Nest.JsonNetSerializer;
using Elasticsearch.Net;
using System.Net.Security;

namespace DSEDiagnosticInsightsES
{
    public sealed class ESConnection
    {
        CPC.LockFree.Queue<string> _esNetworkTrace = new CPC.LockFree.Queue<string>();        
        
        private ESConnection()
        { }

        public ESConnection(Uri esNode)
        {
            System.Net.ServicePointManager.ServerCertificateValidationCallback += ServerCertificate;
            this.Node = esNode;
            this.ConnectionPool = new Elasticsearch.Net.SingleNodeConnectionPool(esNode);
        }

        public ESConnection(string esNode)
            : this(new Uri(esNode))
        {
        }

        public Uri Node { get; }
        public IConnectionPool ConnectionPool { get; }
        public IEnumerable<string> NetworkTrace
        {
            get
            {
                return this._esNetworkTrace;
            }

        }

        /// <summary>
        /// Gets or sets debugging mode.
        /// Setting this to null will result in the default debugging mode.
        /// </summary>
        public bool? EnableDebugging
        {
            get
            {
                return LibrarySettings.ESEnableDebugging;
            }
            set
            {
                if (value.HasValue)
                    LibrarySettings.ESEnableDebugging = value.Value;
                else
                    LibrarySettings.ESEnableDebugging = LibrarySettings.ESEnableDebuggingDefault;
            }
        }

        /// <summary>
        /// Callback only called in debug mode
        /// </summary>
        public RemoteCertificateValidationCallback ServerCertificateDebugCallback;
       
        public IElasticClient Create(bool forceDebugging = false)
        {
            ConnectionSettings settings = new ConnectionSettings(this.ConnectionPool, sourceSerializer: JsonNetSerializer.Default);

            if (forceDebugging || this.EnableDebugging.Value)
            {
                settings.EnableDebugMode(apiCallDetails =>
                            {
                                // log out the request and the request body, if one exists for the type of request
                                if (apiCallDetails.RequestBodyInBytes != null)
                                {
                                    this._esNetworkTrace.Enqueue($"{apiCallDetails.HttpMethod} {apiCallDetails.Uri} " +
                                                                $"{Encoding.UTF8.GetString(apiCallDetails.RequestBodyInBytes)}");
                                }
                                else
                                {
                                    this._esNetworkTrace.Enqueue($"{apiCallDetails.HttpMethod} {apiCallDetails.Uri}");
                                }

                                // log out the response and the response body, if one exists for the type of response
                                if (apiCallDetails.ResponseBodyInBytes != null)
                                {
                                    this._esNetworkTrace.Enqueue($"Status: {apiCallDetails.HttpStatusCode}" +
                                                                $"{Encoding.UTF8.GetString(apiCallDetails.ResponseBodyInBytes)}");
                                }
                                else
                                {
                                    this._esNetworkTrace.Enqueue($"Status: {apiCallDetails.HttpStatusCode}");
                                }
                            });
            }
            
            return new ElasticClient(settings);
        }

        bool ServerCertificate(object sender, X509Certificate cert, X509Chain chain, System.Net.Security.SslPolicyErrors errors)
        {
            if (this.EnableDebugging.Value)
            {
                var callBack = this.ServerCertificateDebugCallback;

                if(callBack != null)
                {
                    callBack(sender, cert, chain, errors);
                }

            }

            return true;
        }

    }

    public abstract class ESQuerySearch
    {
        public static Nest.SearchDescriptor<T> BuildQuery<T>(Nest.SearchDescriptor<T> searchDescriptor,
                                                                string queryField,
                                                                string timestampField,
                                                                Guid clusterId,
                                                                int? nbrDocs = null)
            where T : class
        {
            return searchDescriptor
                    .AllTypes()
                    .Size(nbrDocs)
                    .Query(q => q.Bool(c => c.Must(m => m.Term(queryField, clusterId))))
                    .Sort(s => s.Ascending(timestampField));
        }

        public static Nest.SearchDescriptor<T> BuildQuery<T>(Nest.SearchDescriptor<T> searchDescriptor,
                                                                string queryField,
                                                                string timestampField,
                                                                string clusterId,
                                                                int? nbrDocs = null)
            where T : class
        {
            return searchDescriptor
                    .AllTypes()
                    .Size(nbrDocs)
                    .Query(q => q.Bool(c => c.Must(m => m.Term(queryField, clusterId))))
                    .Sort(s => s.Ascending(timestampField));
        }

        public static Nest.SearchDescriptor<T> BuildQuery<T>(Nest.SearchDescriptor<T> searchDescriptor,
                                                                string timestampField,
                                                                int? nbrDocs = null)
            where T : class
        {
            return searchDescriptor
                    .AllTypes()
                    .Size(nbrDocs)
                    .Sort(s => s.Ascending(timestampField));
        }

        public static Nest.SearchDescriptor<T> BuildQuery<T>(Nest.SearchDescriptor<T> searchDescriptor,
                                                               string queryField,
                                                               string timestampField,
                                                               Guid clusterId,
                                                               DateTimeOffsetRange timeFrame,
                                                               int? nbrDocs = null)
           where T : class
        {
            return searchDescriptor
                    .AllTypes()
                    .Size(nbrDocs)
                    .Query(q => q.Bool(c => c.Must(m => m.Term(queryField, clusterId),
                                                    m => m.Range(r => r.Field(timestampField)
                                                                        .GreaterThanOrEquals(timeFrame.Min.ToUnixTimeMilliseconds())
                                                                        .LessThanOrEquals(timeFrame.Max.ToUnixTimeMilliseconds())))))
                    .Sort(s => s.Ascending(timestampField));
        }        

        public static Nest.SearchDescriptor<T> BuildCurrentQuery<T>(Nest.SearchDescriptor<T> searchDescriptor,
                                                                        string queryField,
                                                                        string timestampField,
                                                                        Guid clusterId,
                                                                        DateTimeOffset? useAsCurrent = null,
                                                                        int? nbrDocs = 1)
           where T : class
        {
            return searchDescriptor
                    .AllTypes()
                    .Size(nbrDocs)
                    .Query(q => q.Bool(c => c.Must(m => m.Term(queryField, clusterId),
                                                    m => m.Range(r => r.Field(timestampField)
                                                                        .LessThanOrEquals((useAsCurrent ?? DateTimeOffset.Now).ToUnixTimeMilliseconds())))))
                    .Sort(s => s.Descending(timestampField));
        }

        public static Nest.SearchDescriptor<T> BuildCurrentQuery<T>(Nest.SearchDescriptor<T> searchDescriptor,
                                                                        string queryField,
                                                                        string timestampField,
                                                                        string clusterId,
                                                                        DateTimeOffset? useAsCurrent = null,
                                                                        int? nbrDocs = 1)
           where T : class
        {
            return searchDescriptor
                    .AllTypes()
                    .Size(nbrDocs)
                    .Query(q => q.Bool(c => c.Must(m => m.Term(queryField, clusterId),
                                                    m => m.Range(r => r.Field(timestampField)
                                                                        .LessThanOrEquals((useAsCurrent ?? DateTimeOffset.Now).ToUnixTimeMilliseconds())))))
                    .Sort(s => s.Descending(timestampField));
        }

        public static Nest.ISearchResponse<T> BuildQuery<T>(Nest.IElasticClient elasticClient,
                                                                Guid clusterId,
                                                                int? nbrDocs = null,
                                                                ESQueryAttribute esqueryAttrib = null)
            where T : class
        {
            var attrib = esqueryAttrib ?? (ESQueryAttribute)typeof(T).GetCustomAttribute(typeof(ESQueryAttribute));

            return elasticClient.Search<T>(s => BuildQuery(s.Index(attrib.ESIndex),
                                                            attrib.QueryFieldName,
                                                            attrib.QueryTimestampFieldName,
                                                            clusterId,
                                                            nbrDocs));
        }

        public static Nest.ISearchResponse<T> BuildQuery<T>(Nest.IElasticClient elasticClient,
                                                                int? nbrDocs = null,
                                                                ESQueryAttribute esqueryAttrib = null)
            where T : class
        {
            var attrib = esqueryAttrib ?? (ESQueryAttribute)typeof(T).GetCustomAttribute(typeof(ESQueryAttribute));

            return elasticClient.Search<T>(s => BuildQuery(s.Index(attrib.ESIndex),
                                                            attrib.QueryTimestampFieldName,
                                                            nbrDocs));
        }

        public static Nest.ISearchResponse<T> BuildQuery<T>(Nest.IElasticClient elasticClient,
                                                            Guid clusterId,
                                                            DateTimeOffsetRange searchRange,
                                                            int? nbrDocs = null,
                                                            ESQueryAttribute esqueryAttrib = null)
            where T : class
        {
            var attrib = esqueryAttrib ?? (ESQueryAttribute)typeof(T).GetCustomAttribute(typeof(ESQueryAttribute));

            return elasticClient.Search<T>(s => BuildQuery(s.Index(attrib.ESIndex),
                                                            attrib.QueryFieldName,
                                                            attrib.QueryTimestampFieldName,
                                                            clusterId,
                                                            nbrDocs));
        }

        public static Nest.ISearchResponse<T> BuildQuery<T>(Nest.IElasticClient elasticClient,
                                                            Guid clusterId,
                                                            DateTimeOffset forTimeFrame,
                                                            int? nbrDocs = null,
                                                            ESQueryAttribute esqueryAttrib = null)
            where T : class
        {
            var attrib = esqueryAttrib ?? (ESQueryAttribute)typeof(T).GetCustomAttribute(typeof(ESQueryAttribute));

            return elasticClient.Search<T>(s => BuildQuery(s.Index(attrib.GetIndexForTimeFrame(forTimeFrame)),
                                                            attrib.QueryFieldName,
                                                            attrib.QueryTimestampFieldName,
                                                            clusterId,
                                                            nbrDocs));
        }

        public static Nest.ISearchResponse<T> BuildCurrentQuery<T>(Nest.IElasticClient elasticClient,
                                                                    Guid clusterId,
                                                                    DateTimeOffset? useAsCurrent = null,
                                                                    int? nbrDocs = 1,
                                                                    ESQueryAttribute esqueryAttrib = null)
             where T : class
        {
            var attrib = esqueryAttrib ?? (ESQueryAttribute)typeof(T).GetCustomAttribute(typeof(ESQueryAttribute));

            return elasticClient.Search<T>(s => BuildCurrentQuery(s.Index(attrib.ESIndex),
                                                                    attrib.QueryFieldName,
                                                                    attrib.QueryTimestampFieldName,
                                                                    clusterId,
                                                                    useAsCurrent,
                                                                    nbrDocs));
        }
    }
}
