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
    [InsightESAttribute("dse.insights.event.exception", "dse.insights.event.exception-searchable-events")]
    public sealed class ExceptionInformation : Insight<ExceptionInformation.Exception>
    {

        public sealed class Exception : IInsightPayload
        {
            [JsonProperty("exception_causes")]
            public ExceptionInstance[] ExceptionCauses;
        }

        public sealed class ExceptionInstance
        {
            [JsonProperty("exception_class")]
            public string ExceptionClass;
            [JsonProperty("stack_trace")]
            public ExceptionCause[] StackTrace;
        }

        public sealed class ExceptionCause
        {
            [JsonProperty("methodName")]
            public string MethodName;

            [JsonProperty("fileName")]
            public string FileName;

            [JsonProperty("lineNumber")]
            public int LineNumber;

            [JsonProperty("className")]
            public string ClassName;

            [JsonProperty("nativeMethod")]
            public bool NativeMethod;

        }

    }
}
