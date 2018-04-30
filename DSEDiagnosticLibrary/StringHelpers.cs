using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Net;
using Common;

namespace DSEDiagnosticLibrary
{
    static public partial class StringHelpers
    {
        static public string RemoveQuotes(string item, bool checkbrackets = true)
        {
            if (string.IsNullOrEmpty(item)) return null;

            RemoveQuotes(item, out item, checkbrackets);
            return item;
        }

        static public bool RemoveQuotes(string item, out string newItem, bool checkbrackets = true)
        {
            if (item.Length > 2
                    && ((item[0] == '\'' && item.Last() == '\'')
                            || (item[0] == '"' && item.Last() == '"')
                            || (checkbrackets && item[0] == '[' && item.Last() == ']')))
            {
                newItem = item.Substring(1, item.Length - 2);
                return true;
            }

            newItem = item;
            return false;
        }

        public static Tuple<string, string> SplitTableName(string cqlTableName, string defaultKeySpaceName)
        {
            var nameparts = Common.StringFunctions.Split(cqlTableName,
                                                            new char[] { '.', '/' },
                                                            Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                            Common.StringFunctions.SplitBehaviorOptions.Default
                                                                | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

            if (nameparts.Count == 1)
            {
                return new Tuple<string, string>(defaultKeySpaceName, RemoveQuotes(nameparts[0]));
            }

            return new Tuple<string, string>(RemoveQuotes(nameparts[0]), RemoveQuotes(nameparts[1]));
        }

        public static Tuple<string, string> SplitTableName(this string cqlTableName)
        {
            if (cqlTableName[0] == '[' || cqlTableName[0] == '(')
            {
                cqlTableName = cqlTableName.Substring(1, cqlTableName.Length - 2);
            }

            return SplitTableName(cqlTableName.Replace('/', '.'), null);
        }

        public static string RemoveNamespace(this string className)
        {
            className = RemoveQuotes(className);

            if (className != null && !className.Contains('/'))
            {
                var lastPeriod = className.LastIndexOf('.');

                if (lastPeriod >= 0)
                {
                    return className.Substring(lastPeriod + 1);
                }
            }

            return className;
        }

        static public System.Net.IPAddress DetermineIPAddress(string possibleIPAddress)
        {
            System.Net.IPAddress ipAddress = null;
            System.Net.IPAddress.TryParse(possibleIPAddress, out ipAddress);
            return ipAddress;
        }

        private static Regex hostPortMatch = new Regex(@"^(?<ip>(?:\[[\da-fA-F:]+\])|(?:\d{1,3}\.){3}\d{1,3})(?::(?<port>\d+))$", System.Text.RegularExpressions.RegexOptions.Compiled);
        public static IPEndPoint ParseHostPort(string hostPort)
        {
            Match match = hostPortMatch.Match(hostPort);
            if (!match.Success)
                return null;

            return new IPEndPoint(IPAddress.Parse(match.Groups["ip"].Value), int.Parse(match.Groups["port"].Value));
        }

        static public string DetermineProperFormat(string strValue,
                                                    bool ignoreBraces = false,
                                                    bool removeNamespace = true,
                                                    bool tryParseIPAddress = true,
                                                    string convertToUOMBasedOnContext = null,
                                                    bool checkforDate = false)
        {
            var result = DetermineProperObjectFormat(strValue, ignoreBraces, removeNamespace, tryParseIPAddress, convertToUOMBasedOnContext, checkforDate);

            return result == null ? null : (result is string ? (string)result : result.ToString());
        }

        readonly static Regex BooleanRegEx = new Regex("^(true|false|enable[d]?|disable[d]?|y[es]?|n[o]?)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        readonly static Regex UOMRegEx = new Regex(@"^\-?(?:[0-9,]+|[0-9,]+\.[0-9]+|\.[0-9]+)\s*[a-zA-Z%,_/\ ]+$", RegexOptions.Compiled);

        //scores-name-1-08bdd2d2126e11e78866d792efac3044
        // null, scores-name-1, 08bdd2d2126e11e78866d792efac3044, null
        //scores-name-1
        // null, null, null, scores-name-1
        readonly static Regex SSTableCQLTableNameRegEx = new Regex(Properties.Settings.Default.SSTableColumnFamilyRegEx, RegexOptions.Compiled | RegexOptions.IgnoreCase);
        readonly static string[] DateTimeFormats = new string[] {  "yyyy-MM-dd HH:mm:ss,fff",
                                                                    "yyyy-MM-dd HH:mm:ss.fff",
                                                                    "yyyy-MM-dd HH:mm:ss,ff",
                                                                    "yyyy-MM-dd HH:mm:ss.ff",
                                                                    "yyyy-MM-dd HH:mm:ss,f",
                                                                    "yyyy-MM-dd HH:mm:ss.f",};

        static public object DetermineProperObjectFormat(string strValue,
                                                            bool ignoreBraces = false,
                                                            bool removeNamespace = true,
                                                            bool tryParseIPAddress = true,
                                                            string convertToUOMBasedOnContext = null,
                                                            bool checkforDate = false)
        {
            object item;

            if (string.IsNullOrEmpty(strValue))
            {
                return strValue;
            }

            strValue = strValue.Trim();

            if (strValue == string.Empty)
            {
                return strValue;
            }

            if (strValue == "null")
            {
                return null;
            }

            if(BooleanRegEx.IsMatch(strValue))
            {
                bool boolValue;

                if(Boolean.TryParse(strValue, out boolValue))
                {
                    return boolValue;
                }

                if(strValue.StartsWith("enable", StringComparison.OrdinalIgnoreCase)
                    || strValue.StartsWith("y", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }

                if (strValue.StartsWith("disable", StringComparison.OrdinalIgnoreCase)
                    || strValue.StartsWith("n", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            if (convertToUOMBasedOnContext == null && checkforDate)
            {
                TimeSpan ts;

                if (strValue.IndexOf(':') > 0 && TimeSpan.TryParse(strValue, out ts))
                {
                    return ts;
                }


                DateTime dt;

                if (DateTime.TryParse(strValue, out dt))
                {
                    return dt;
                }

                const DateTimeStyles style = DateTimeStyles.AllowWhiteSpaces;
                if (DateTime.TryParseExact(strValue, DateTimeFormats,
                                            CultureInfo.InvariantCulture, style, out dt))
                {
                    return dt;
                }
            }

            if (tryParseIPAddress)
            {
                var strIPAddress = strValue;

                if (strIPAddress[0] == '/')
                {
                    strIPAddress = strIPAddress.Substring(1);
                }

                var endPoint = StringHelpers.ParseHostPort(strIPAddress);

                if(endPoint == null)
                {
                    if (NodeIdentifier.IPAddressRegEx.IsMatch(strIPAddress))
                    {
                        System.Net.IPAddress ipAddress;
                        if (System.Net.IPAddress.TryParse(strIPAddress, out ipAddress))
                        {
                            return ipAddress;
                        }
                    }
                }
                else
                {
                    return endPoint;
                }
            }

            if (!ignoreBraces)
            {
                if (strValue[0] == '{' && strValue[strValue.Length - 1] == '}')
                {
                    strValue = strValue.Substring(1, strValue.Length - 2);                    
                }

                if (strValue[0] == '['
                    && strValue[strValue.Length - 1] == ']')
                {
                    strValue = strValue.Substring(1, strValue.Length - 2);

                    var splitItems = strValue.Split(',');

                    if (splitItems.Length > 1)
                    {
                        var fmtItems = splitItems.Select(i => DetermineProperFormat(i, ignoreBraces, removeNamespace, tryParseIPAddress, convertToUOMBasedOnContext, checkforDate)).Sort();
                        return "[" + string.Join(", ", fmtItems) + "]";
                    }
                }
            }

            if (RemoveQuotes(strValue, out strValue, false))
            {
                var splitItems = strValue.Split(',');

                if (splitItems.Length > 1)
                {
                    var fmtItems = splitItems.Select(i => DetermineProperFormat(i, ignoreBraces, removeNamespace, tryParseIPAddress, convertToUOMBasedOnContext, checkforDate)).Sort();
                    return string.Join(", ", fmtItems);
                }
            }

            if(UOMRegEx.IsMatch(strValue))
            {
                var uom = UnitOfMeasure.Create(strValue);

                if (!uom.NaN && uom.UnitType != UnitOfMeasure.Types.Unknown)
                {
                    return uom;
                }
            }

            if (strValue.IndexOfAny(new char[] { '/', '\\', ':' }) >= 0)
            {
                return strValue;
            }

            if (StringFunctions.ParseIntoNumeric(strValue, out item))
            {
                if (!string.IsNullOrEmpty(convertToUOMBasedOnContext))
                {
                    var uomType = UnitOfMeasure.ConvertToType(convertToUOMBasedOnContext);

                    if (uomType != UnitOfMeasure.Types.Unknown)
                    {
                        return UnitOfMeasure.Create((decimal)((dynamic)item), uomType);
                    }
                }

                return item;
            }

            if (!string.IsNullOrEmpty(convertToUOMBasedOnContext) && UnitOfMeasure.IsNaN(strValue, false))
            {
                return UnitOfMeasure.Create(strValue,
                                            UnitOfMeasure.ConvertToType(convertToUOMBasedOnContext));
            }

            if(removeNamespace)
            {
                if (strValue.EndsWith(".local", StringComparison.OrdinalIgnoreCase)) return strValue;

                var lastPos = strValue.LastIndexOf('.');

                if (lastPos > 0 && (strValue.Length - lastPos) <= 4) return strValue;

                return RemoveNamespace(strValue);
            }

            return strValue;
        }



        /// <summary>
        /// Parses a SSTable path file into the keyspace and table names plus the table&apos;s guid
        /// </summary>
        /// <param name="sstableFilePath"></param>
        /// <returns>
        /// null if the sstableFilePath is not a valid sstable path format
        /// a tuple where
        ///     Item1 -- keyspace name
        ///     Item2 -- table/secondary index name
        ///     Item3 -- table&apos;s guid id or string.empty
        ///     Item4 -- if secondary index this will be the associated table name, otherwise null.
        /// </returns>
        static public Tuple<string,string,string,string> ParseSSTableFileIntoKSTableNames(string sstableFilePath)
        {
            // "/mnt/dse/data1/homeKS/homebase_tasktracking_ops_l3-737682f0599311e6ad0fa12fb1b6cb6e/homeKS-homebase_tasktracking_ops_l3-tmp-ka-15175-Data.db"
            // "/var/lib/cassandra/data/cfs/inode-76298b94ca5f375cab5bb674eddd3d51/cfs-inode.cfs_parent_path-tmp-ka-71-Data.db"
            // "/data/cassandra/data/usprodda/digitalasset_3_0_2-42613c599c943db2b2805c31c2d36acb/usprodda-digitalasset_3_0_2.digitalasset_3_0_2_id-tmp-ka-308-Data.db"
            // "/mnt/cassandra/data/data/keyspace1/digitalasset_3_0_2-980fca722ea611e78ff92901fcc7f505/.index2_tags/mb-1-big-Data.db"
            // "/var/lib/cassandra/data/Keyspace1/digitalasset_3_0_2/Keyspace1-digitalasset_3_0_2-jb-1-Data.db"
            // "/var/lib/cassandra/data/Keyspace1/digitalasset_3_0_2/Keyspace1-digitalasset_3_0_2.digitalasset_3_0_2_id-jb-1-Data.db"
            // "/d3/data/system/batches-919a4bc57a333573b03e13fc3f68b465/mc-62-big-Data.db"
            //
            // C* 2.0 -- /mnt/dse/data1/<keyspace>/<column family>/<keyspace>-<column family>-<version>-<generation>-<component>.db
            // C* 2.0 -- /mnt/dse/data1/<keyspace>/<column family>/<keyspace>-<column family>.<SecondaryIndex>-<version>-<generation>-<component>.db
            //
            // C* 2.1 -- /mnt/dse/data1/<keyspace>/<column family>-<uuid>/<keyspace>-<column family>-[tmp marker]-<version>-<generation>-<component>.db
            // C* 2.1 -- /mnt/dse/data1/<keyspace>/<column family>-<uuid>/<keyspace>-<column family>.<SecondaryIndex>-[tmp marker]-<version>-<generation>-<component>.db
            //
            // C* 3.X -- /mnt/dse/data1/<keyspace>/<column family>-<uuid>/mb-1-big-Data.db
            // C* 3.X -- /mnt/dse/data1/<keyspace>/<column family>-<uuid>/.<secondary index>/mb-1-big-Data.db
            //
            // Where <version> can be 'jb', 'ka', 'ja', and 'ic' and [tmp marker] can be 'tmp'
            var sstableFilePathParts = sstableFilePath.Split('/');

            if (sstableFilePathParts.Length >= 3)
            {
                var idxOffset = sstableFilePathParts[sstableFilePathParts.Length - 2][0] == '.' ? 1 : 0; //used to offset C* 3.x secondary index format

                var ksName = sstableFilePathParts[sstableFilePathParts.Length - (3 + idxOffset)];
                var tblNameMatch = SSTableCQLTableNameRegEx.Match(sstableFilePathParts[sstableFilePathParts.Length - (2 + idxOffset)]);

                if (tblNameMatch.Success)
                {
                    var columnFamily = string.IsNullOrEmpty(tblNameMatch.Groups[1].Value)
                                            ? tblNameMatch.Groups[3].Value
                                            : tblNameMatch.Groups[1].Value;
                    var cfUUID = tblNameMatch.Groups[2].Value;
                    string secondTblName = null;

                    if (idxOffset == 1)
                    {
                        secondTblName = sstableFilePathParts[sstableFilePathParts.Length - 2].Substring(1);
                    }
                    else if (sstableFilePathParts.Last().Length > ksName.Length + columnFamily.Length + 1
                                && sstableFilePathParts.Last()[ksName.Length + columnFamily.Length + 1] == '.'
                                && !sstableFilePathParts.Last().StartsWith("mc-"))
                    {
                        var lastNode = sstableFilePathParts.Last();
                        int tmpPos = -1;
                        int startPos = ksName.Length + columnFamily.Length + 2;

                        foreach (var ssVersion in LibrarySettings.SSTableVersionMarkers)
                        {
                            tmpPos = lastNode.IndexOf('-' + ssVersion + '-');

                            if(tmpPos > 0)
                            {
                                break;
                            }
                        }

                        if (tmpPos > 0)
                        {
                            secondTblName = lastNode.Substring(startPos, tmpPos - startPos);
                        }
                        else
                        {
                            throw new ArgumentException(string.Format("Could not find the Secondary Index name in SSTable '{0}' within path node '{1}'",
                                                                        sstableFilePath,
                                                                        lastNode));
                        }
                    }

                    return new Tuple<string, string, string, string>(ksName,
                                                                        secondTblName ?? columnFamily,
                                                                        cfUUID,
                                                                        secondTblName == null ? null : columnFamily);
                }
            }

            return null;
        }

        /// <summary>
        /// Removes all inline comments (begins with /* and ends with */) within a string.
        /// </summary>
        /// <param name="strValue"></param>
        /// <param name="newValue">
        /// outputs the new string.
        /// </param>
        /// <param name="startIndex">
        /// if negative, the complete string is scanned.
        /// </param>
        /// <param name="removePartial">
        /// if true (default false) and if a string only has one side of the delimiter, that side comment is removed.
        /// </param>
        /// <returns>
        /// 0 -- no inline comments found and newString is the same as strValue.
        /// 1 -- a complete inline comment(s) found and removed or an ending delimiter found and removePartial is true.
        /// -1 -- an beginning inline comment found (no end delimiter found) and may have been removed depending on removePartial
        /// </returns>
        static public int RemoveInLineComment(string strValue, out string newValue, int startIndex = -1, bool removePartial = false)
        {
            int nResult = 0;
            newValue = strValue;

            var startComment = startIndex > 0 ? strValue.LastIndexOf(@"/*", startIndex) : strValue.LastIndexOf(@"/*");

            if (startComment >= 0)
            {
                var endComment = strValue.IndexOf(@"*/", startComment);

                if (endComment < 0)
                {
                    if (removePartial)
                    {
                        newValue = strValue.Substring(0, startComment);
                        RemoveInLineComment(newValue, out newValue, -1, removePartial);
                    }

                    return -1;
                }

                if (endComment + 2 < strValue.Length)
                {
                    newValue = (strValue.Substring(0, startComment) + " " + strValue.Substring(endComment + 2)).TrimEnd();
                }
                else
                {
                    newValue = strValue.Substring(0, startComment).TrimEnd();
                }

                nResult = RemoveInLineComment(newValue, out newValue, -1, removePartial) == -1 ? -1 : 1;
            }
            else if (removePartial)
            {
                var endComment = strValue.IndexOf(@"*/");

                if (endComment >= 0)
                {
                    newValue = endComment + 2 >= strValue.Length ? string.Empty : strValue.Substring(endComment + 2);
                    nResult = RemoveInLineComment(newValue, out newValue, -1, removePartial) == -1 ? -1 : 1;
                }
            }

            return nResult;
        }

        static public Common.Patterns.TimeZoneInfo.IZone FindTimeZone(string ianaName)
        {
            return string.IsNullOrEmpty(ianaName)
                            ? null
                            : Common.TimeZones.Find(ianaName, Common.Patterns.TimeZoneInfo.ZoneNameTypes.IANATZName)
                                ?? Common.TimeZones.Find(ianaName, Common.Patterns.TimeZoneInfo.ZoneNameTypes.FormattedName);            
        }

        static public string InternString(this string str)
        {
            return string.IsNullOrEmpty(str) ? str : string.Intern(str);
        }

        private enum MMElementType
        {
            JSON = 0,
            Long = 1,
            Decimal = 2,
            DDLStmt = 4,
            Keyspace = 5,
            Node = 6,
            DataCenter = 7,
            Cluster = 8,
            String = 9,
            UnitOfMeasure = 10,
            DateTime = 11,
            DateTimeOffset = 12,
            Timespan = 13,
            TokenRange = 14,
            IPAddress = 15,
            IPEndPoint = 16,
            List = 17
        }

        //private static Newtonsoft.Json.JsonConverter[] CustomJsonConverters = new Newtonsoft.Json.JsonConverter[] { new DSEDiagnosticLibrary.IPAddressJsonConverter(),
        //                                                                                                            new DSEDiagnosticLibrary.IPEndPointJsonConverter(),
        //                                                                                                            new DSEDiagnosticLibrary.IPathJsonConverter(),
        //                                                                                                            new DSEDiagnosticLibrary.DateTimeRangeJsonConverter() };

        static public void WriteMMElement(this Common.Patterns.Collections.MemoryMapper.WriteElement writeElement, object value)
        {
            //true if null or false is not null
            if (value == null)
            {
                writeElement.StoreValue(true);
                return;
            }
            writeElement.StoreValue(false);

            if (value.GetType().IsValueType)
            {
                if (value.IsNumber())
                {
                    if (value is decimal || value is Double || value is float)
                    {
                        writeElement.StoreValue((int)MMElementType.Decimal);
                        writeElement.StoreValue((decimal)((dynamic)value));
                    }
                    else
                    {
                        writeElement.StoreValue((int)MMElementType.Long);
                        writeElement.StoreValue((long)((dynamic)value));
                    }
                }
                else if(value is UnitOfMeasure)
                {
                    writeElement.StoreValue((int)MMElementType.UnitOfMeasure);
                    writeElement.WriteMMElement((UnitOfMeasure)value);
                }
                else if(value is DateTime)
                {
                    writeElement.StoreValue((int)MMElementType.DateTime);
                    writeElement.StoreValue((DateTime)value);
                }
                else if(value is DateTimeOffset)
                {
                    writeElement.StoreValue((int)MMElementType.DateTimeOffset);
                    writeElement.StoreValue((DateTimeOffset)value);
                }
                else if (value is TimeSpan)
                {
                    writeElement.StoreValue((int)MMElementType.Timespan);
                    writeElement.StoreValue((TimeSpan)value);
                }
                else
                {
                    writeElement.StoreValue((int)MMElementType.JSON);
                    writeElement.StoreValue(Newtonsoft.Json.JsonConvert.SerializeObject(value));
                }

                return;
            }

            if (value is string)
            {
                writeElement.StoreValue((int)MMElementType.String);
                writeElement.StoreValue((string)value);
            }
            else if (value is IDDLStmt)
            {
                writeElement.StoreValue((int)MMElementType.DDLStmt);
                writeElement.StoreValue(value.GetHashCode());
            }
            else if (value is IKeyspace)
            {
                writeElement.StoreValue((int)MMElementType.Keyspace);
                writeElement.StoreValue(value.GetHashCode());
            }
            else if (value is INode)
            {
                writeElement.StoreValue((int)MMElementType.Node);
                writeElement.StoreValue(value.GetHashCode());
            }
            else if (value is IDataCenter)
            {
                writeElement.StoreValue((int)MMElementType.DataCenter);
                writeElement.StoreValue(value.GetHashCode());
            }
            else if (value is Cluster)
            {
                writeElement.StoreValue((int)MMElementType.Cluster);
                writeElement.StoreValue(value.GetHashCode());
            }
            else if (value is TokenRangeInfo)
            {
                writeElement.StoreValue((int)MMElementType.TokenRange);
                writeElement.WriteMMElement((TokenRangeInfo)value);
            }
            else if (value is IPAddress)
            {
                writeElement.StoreValue((int)MMElementType.IPAddress);
                writeElement.WriteMMElement((IPAddress)value);
            }
            else if (value is IPEndPoint)
            {
                writeElement.StoreValue((int)MMElementType.IPEndPoint);
                writeElement.WriteMMElement((IPEndPoint)value);
            }
            else if (typeof(System.Collections.IEnumerable).IsAssignableFrom(value.GetType()))
            {
                writeElement.StoreValue((int)MMElementType.List);
                writeElement.StoreValue(((IEnumerable<object>)value).Count());

                foreach(var item in (IEnumerable<object>)value)
                {
                    WriteMMElement(writeElement, item);
                }

            }
            else
            {
                writeElement.StoreValue((int)MMElementType.JSON);
                writeElement.StoreValue(Newtonsoft.Json.JsonConvert.SerializeObject(value, JsonConverters.Settings));
            }
        }

        static public void WriteMMElement(this Common.Patterns.Collections.MemoryMapper.WriteElement writeElement, TokenRangeInfo value)
        {
            bool isLong = value is TokenRangeLong;

            writeElement.StoreValue(isLong);
            if(isLong)
            {
                writeElement.StoreValue(((TokenRangeLong)value).StartRangeNum);
                writeElement.StoreValue(((TokenRangeLong)value).EndRangeNum);
            }
            else
            {
                writeElement.StoreValue(value.StartRange.ToString());
                writeElement.StoreValue(value.EndRange.ToString());
            }
           
            writeElement.WriteMMElement(value.Load);
        }

        static public void WriteMMElement(this Common.Patterns.Collections.MemoryMapper.WriteElement writeElement, IPAddress value)
        {
            if(value == null)
            {
                writeElement.StoreValue((string) null);
            }
            else
            {
                writeElement.StoreValue(value.ToString());
            }
        }

        static public void WriteMMElement(this Common.Patterns.Collections.MemoryMapper.WriteElement writeElement, IPEndPoint value)
        {
            if (value == null)
            {
                writeElement.StoreValue((string)null);
                writeElement.StoreValue((int) 0);
            }
            else
            {
                writeElement.StoreValue(value.Address.ToString());
                writeElement.StoreValue(value.Port);
            }
        }

        static public void WriteMMElement(this Common.Patterns.Collections.MemoryMapper.WriteElement writeElement, UnitOfMeasure value)
        {
            writeElement.StoreValue(value.Value);
            writeElement.StoreValue((ulong)value.UnitType);
        }

        static public void WriteMMElement(this Common.Patterns.Collections.MemoryMapper.WriteElement writeElement, IEnumerable<TokenRangeInfo> value)
        {
            var count = value?.Count() ?? -1;

            writeElement.StoreValue(count);
            foreach (var tokenRange in value)
            {
                writeElement.WriteMMElement(tokenRange);
            }
        }

        static public object GetMMElement(this Common.Patterns.Collections.MemoryMapper.ReadElement readElement, Cluster cluster, IDataCenter dc, INode node, IKeyspace keyspace)
        {
            object value = null;

            if (readElement.GetBoolValue()) return null;

            if (dc == null)
            {
                if (node != null)
                    dc = node.DataCenter;
                else if (keyspace != null)
                    dc = keyspace.DataCenter;
            }
            if (cluster == null)
            {
                if (dc != null)
                    cluster = dc.Cluster;
            }

            MMElementType elementType = (MMElementType)readElement.GetIntValue();
            int hashCode;

            switch (elementType)
            {
                case MMElementType.JSON:
                    value = Newtonsoft.Json.JsonConvert.DeserializeObject(readElement.GetStringValue(), JsonConverters.Settings);
                    break;
                case MMElementType.Long:
                    value = readElement.GetLongValue();
                    break;
                case MMElementType.Decimal:
                    value = readElement.GetDecimalValue();
                    break;
                case MMElementType.UnitOfMeasure:
                    value = readElement.GetMMUOM();
                    break;
                case MMElementType.DateTime:
                    value = readElement.GetStructValue<DateTime>();
                    break;
                case MMElementType.DateTimeOffset:
                    value = readElement.GetStructValue<DateTimeOffset>();
                    break;
                case MMElementType.Timespan:
                    value = readElement.GetStructValue<TimeSpan>();
                    break;
                case MMElementType.DDLStmt:
                    hashCode = readElement.GetIntValue();
                    value = keyspace?.TryGetDDL(hashCode) ?? dc?.Keyspaces.Select(k => k.TryGetDDL(hashCode)).FirstOrDefault() ?? Cluster.TryGetDDLStmt(hashCode);
                    break;
                case MMElementType.Keyspace:
                    hashCode = readElement.GetIntValue();
                    value = dc?.TryGetKeyspace(hashCode) ?? Cluster.TryGetKeySpace(hashCode);
                    break;
                case MMElementType.Node:
                    hashCode = readElement.GetIntValue();
                    value = dc?.TryGetNode(hashCode) ?? Cluster.TryGetNode(hashCode);
                    break;
                case MMElementType.DataCenter:
                    hashCode = readElement.GetIntValue();
                    value = Cluster.TryGetDataCenter(hashCode);
                    break;
                case MMElementType.Cluster:
                    hashCode = readElement.GetIntValue();
                    value = Cluster.TryGetCluster(hashCode);
                    break;
                case MMElementType.String:
                    value = readElement.GetStringValue();
                    break;
                case MMElementType.TokenRange:
                    value = readElement.GetMMTokenRange();
                    break;
                case MMElementType.IPAddress:
                    value = readElement.GetMMIPAddress();
                    break;
                case MMElementType.IPEndPoint:
                    value = readElement.GetMMIPEndPoint();
                    break;
                case MMElementType.List:
                    var count = readElement.GetIntValue();
                    var itemLst = new List<object>(count);

                    for(int nIdx = 0; nIdx < count; nIdx++)
                    {
                        itemLst.Add(GetMMElement(readElement, cluster, dc, node, keyspace));
                    }
                    value = itemLst;
                    break;
                default:
                    throw new ArgumentException(string.Format("GetMMElement Failed with an unkown MMEllementType of {0}", elementType));
                    //break;
            }

            return value;
        }

        static public TokenRangeInfo GetMMTokenRange(this Common.Patterns.Collections.MemoryMapper.ReadElement readElement)
        {
            var isLong = readElement.GetBoolValue();

            if(isLong)
                return new TokenRangeLong(readElement.GetLongValue(),
                                            readElement.GetLongValue(),
                                            readElement.GetMMUOM());

            return TokenRangeInfo.CreateTokenRange(readElement.GetStringValue(),
                                                    readElement.GetStringValue(),
                                                    readElement.GetMMUOM());
        }

        static public IEnumerable<TokenRangeInfo> GetMMTokenRangeEnum(this Common.Patterns.Collections.MemoryMapper.ReadElement readView)
        {
            var count = readView.GetIntValue();

            if (count < 0) return null;
            if (count == 0) return Enumerable.Empty<TokenRangeInfo>();

            var tokenRanges = new List<TokenRangeInfo>(count);

            for(int nIdx = 1; nIdx <= count; nIdx++)
            {
                tokenRanges.Add(readView.GetMMTokenRange());
            }
            return tokenRanges;
        }

        static public IPAddress GetMMIPAddress(this Common.Patterns.Collections.MemoryMapper.ReadElement readView)
        {
            var ipAddressStr = readView.GetStringValue();

            return ipAddressStr == null ? null : IPAddress.Parse(ipAddressStr);
        }

        static public IPEndPoint GetMMIPEndPoint(this Common.Patterns.Collections.MemoryMapper.ReadElement readView)
        {
            var ipAddressStr = readView.GetStringValue();
            var port = readView.GetIntValue();

            return ipAddressStr == null ? null : new IPEndPoint(IPAddress.Parse(ipAddressStr), port);
        }

        static public UnitOfMeasure GetMMUOM(this Common.Patterns.Collections.MemoryMapper.ReadElement readView)
        {
            return new UnitOfMeasure(readView.GetDecimalValue(), (UnitOfMeasure.Types) readView.GetULongValue());
        }

        static public void SkipMMElement(this Common.Patterns.Collections.MemoryMapper.ReadElement readElement)
        {

            if (readElement.GetBoolValue()) return;

            MMElementType elementType = (MMElementType)readElement.GetIntValue();

            switch (elementType)
            {
                case MMElementType.Long:
                    readElement.SkipLong();
                    break;
                case MMElementType.Decimal:
                    readElement.SkipDecimal();
                    break;
                case MMElementType.UnitOfMeasure:
                    readElement.SkipMMUOM();
                    break;
                case MMElementType.DateTime:
                case MMElementType.DateTimeOffset:
                case MMElementType.Timespan:
                    readElement.SkipStruct();
                    break;
                case MMElementType.DDLStmt:
                case MMElementType.Keyspace:
                case MMElementType.Node:
                case MMElementType.DataCenter:
                case MMElementType.Cluster:
                    readElement.SkipInt();
                    break;
                case MMElementType.JSON:
                case MMElementType.String:
                case MMElementType.IPAddress:
                    readElement.SkipString();
                    break;
                case MMElementType.IPEndPoint:
                    readElement.SkipString();
                    readElement.SkipInt();
                    break;
                case MMElementType.TokenRange:
                    readElement.SkipMMTokenRange();
                    break;
                case MMElementType.List:
                    var count = readElement.GetIntValue();
                    for(int nIdx = 0; nIdx < count; nIdx++)
                    {
                        SkipMMElement(readElement);
                    }
                    break;
                default:
                    throw new ArgumentException(string.Format("SkipMMElement Failed with an unkown MMEllementType of {0}", elementType));
                    //break;
            }
        }

        static public void SkipMMTokenRange(this Common.Patterns.Collections.MemoryMapper.ReadElement readElement)
        {
            var isLong = readElement.GetBoolValue();

            if (isLong)
            {
                readElement.SkipLong();
                readElement.SkipLong();
            }
            else
            {
                readElement.SkipString();
                readElement.SkipString();
            }
            readElement.SkipMMUOM();
        }

        static public void SkipMMTokenRangeEnum(this Common.Patterns.Collections.MemoryMapper.ReadElement readElement)
        {
            var count = readElement.GetIntValue();

            if (count <= 0) return;

            for (int nIdx = 1; nIdx <= count; nIdx++)
            {
                readElement.SkipMMTokenRange();
            }
        }

        static public void SkipMMIpAddresse(this Common.Patterns.Collections.MemoryMapper.ReadElement readElement)
        {
            readElement.SkipString();
        }

        static public void SkipMMUOM(this Common.Patterns.Collections.MemoryMapper.ReadElement readElement)
        {
            readElement.SkipDecimal();
            readElement.SkipULong();
        }

        static public void WriteMMElement(this Common.Patterns.Collections.MemoryMapper.WriteElement writeElement, IEnumerable<KeyValuePair<string, object>> values)
        {
            var startPos = writeElement.CurrentMMPosition;
            var startSize = writeElement.CurrentElementSize;
            int nbrItems = 0;

            writeElement.Pad(sizeof(int) * 2);

            foreach (var keyvaluePair in values)
            {
                writeElement.StoreValue(keyvaluePair.Key);
                writeElement.WriteMMElement(keyvaluePair.Value);
                nbrItems++;
            }

            writeElement.MemoryMapper.Write(startPos, (int) (writeElement.CurrentElementSize - startSize));
            writeElement.MemoryMapper.Write(startPos + sizeof(int), nbrItems);
        }

        static public Dictionary<string, object> ReadMMElement(this Common.Patterns.Collections.MemoryMapper.ReadElement readElement, Cluster cluster, IDataCenter dc, INode node, IKeyspace keyspace)
        {
            var values = new Dictionary<string, object>();

            readElement.SkipInt(); //Skip the total size
            var nbrElements = readElement.GetIntValue();

            for(int nIdx = 1; nIdx <= nbrElements; nIdx++)
            {
                values.Add(readElement.GetStringValue(),
                           readElement.GetMMElement(cluster, dc, node, keyspace));
            }

            return values;
        }

        static public void SkipMMElementKVPStrObj(this Common.Patterns.Collections.MemoryMapper.ReadElement readElement)
        {
            var writtenSize = readElement.GetIntValue();

            readElement.SkipBasedOnSize(writtenSize - sizeof(int));
        }
    }
}
