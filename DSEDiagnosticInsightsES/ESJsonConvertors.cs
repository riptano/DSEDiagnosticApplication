using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DSEDiagnosticInsightsES
{
    public class GenericSerializer<T> : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public IReadOnlyDictionary<string, dynamic> DetermineJObjectValue(Newtonsoft.Json.Linq.JObject jObject)
        {
            var jItems = jObject.ToObject<Dictionary<string, dynamic>>();
            var vDict = new Dictionary<string, dynamic>(jItems.Count);

            foreach (var element in jItems)
            {
                if (element.Value is Newtonsoft.Json.Linq.JObject jObj)
                {
                    vDict.Add(element.Key, this.DetermineJObjectValue(jObj));
                }
                else if (element.Value is Newtonsoft.Json.Linq.JArray jArray)
                {
                    vDict.Add(element.Key, DetermineJObjectValue(jArray));
                }
                else if (element.Value is Newtonsoft.Json.Linq.JToken jToken)
                {
                    vDict.Add(element.Key, DetermineJObjectValue(jToken));
                }
                else
                {
                    vDict.Add(element.Key, element.Value);
                }
            }
            return vDict;
        }

        public dynamic DetermineJObjectValue(Newtonsoft.Json.Linq.JToken jToken)
        {
            //if (jToken is Newtonsoft.Json.Linq.JObject jObject) return this.DetermineJObjectValue(jObject);
            if (jToken.Type == Newtonsoft.Json.Linq.JTokenType.Object) return this.DetermineJObjectValue(jToken.ToObject<Newtonsoft.Json.Linq.JObject>());

            //if (jToken is Newtonsoft.Json.Linq.JArray jArray) return this.DetermineJObjectValue(jArray);
            if (jToken.Type == Newtonsoft.Json.Linq.JTokenType.Array) return this.DetermineJObjectValue(jToken.ToObject<Newtonsoft.Json.Linq.JArray>());
            
            if (jToken.HasValues)
            {
                var jTokens = new dynamic[jToken.Count()];
                var nIdx = 0;

                foreach (var element in jToken)
                {
                    jTokens[nIdx++] = this.DetermineJObjectValue(element);
                }
                return jTokens;
            }

            if (jToken.Type == Newtonsoft.Json.Linq.JTokenType.Boolean) return jToken.ToObject<bool>();
            if (jToken.Type == Newtonsoft.Json.Linq.JTokenType.Date) return jToken.ToObject<DateTime>();
            if (jToken.Type == Newtonsoft.Json.Linq.JTokenType.Float) return jToken.ToObject<decimal>();
            if (jToken.Type == Newtonsoft.Json.Linq.JTokenType.Guid) return jToken.ToObject<Guid>();
            if (jToken.Type == Newtonsoft.Json.Linq.JTokenType.Integer) return jToken.ToObject<long>();
            if (jToken.Type == Newtonsoft.Json.Linq.JTokenType.TimeSpan) return jToken.ToObject<TimeSpan>();

            var sValue = jToken.ToObject<dynamic>();

            if (sValue is Newtonsoft.Json.Linq.JToken nToken)
            {
                return this.DetermineJObjectValue(nToken);
            }

            return sValue;
        }

        public dynamic[] DetermineJObjectValue(Newtonsoft.Json.Linq.JArray jArray)
        {
            var vArray = new dynamic[jArray.Count];

            for (int nIdx = 0; nIdx < jArray.Count; ++nIdx)
            {
                var jToken = jArray[nIdx];

                vArray[nIdx] = this.DetermineJObjectValue(jToken);
            }

            return vArray;
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
                return null;

            if (reader.TokenType == JsonToken.StartObject)
            {
                var jObject = Newtonsoft.Json.Linq.JObject.Load(reader);

                return this.DetermineJObjectValue(jObject);
            }

            if (reader.TokenType == JsonToken.StartArray)
            {
                var jArray = Newtonsoft.Json.Linq.JArray.Load(reader);

                return this.DetermineJObjectValue(jArray);
            }

            return serializer.Deserialize(reader, objectType);
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(T).IsAssignableFrom(objectType);
        }
    }

    /// <summary>
    /// Used to deserialized to an IReadOnlyDictionary&lt;string, dynamic&gt;
    /// </summary>
    public class PropDictSerializer : GenericSerializer<IReadOnlyDictionary<string, dynamic>>
    {
    }

    /// <summary>
    /// Used to deserialized to an IReadOnlyDictionary&lt;string, dynamic&gt;[] (array)
    /// </summary>
    public class PropDictArraySerializer : GenericSerializer<IReadOnlyDictionary<string, dynamic>[]>
    {
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var vObj = base.ReadJson(reader, objectType, existingValue, serializer);

            if (vObj == null) return null;

            return ((object[])vObj).Cast<IReadOnlyDictionary<string, dynamic>>().ToArray();
        }

    }


}
