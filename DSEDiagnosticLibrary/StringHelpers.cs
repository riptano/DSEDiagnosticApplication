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

            if (!className.Contains('/'))
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

        static public string DetermineProperFormat(string strValue, bool ignoreBraces = false, bool removeNamespace = true)
        {
            var result = DetermineProperObjectFormat(strValue, ignoreBraces, removeNamespace, false, null);

            return result == null ? null : (result is string ? (string)result : result.ToString());
        }

        readonly static Regex BooleanRegEx = new Regex("^(true|false|enable[d]?|disable[d]?|y[es]?|n[o]?)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        readonly static Regex UOMRegEx = new Regex(@"^\-?(?:[0-9,]+|[0-9,]+\.[0-9]+|\.[0-9]+)\s*[a-zA-Z%,_/\ ]+$", RegexOptions.Compiled);

        //scores-name-1-08bdd2d2126e11e78866d792efac3044
        // null, scores-name-1, 08bdd2d2126e11e78866d792efac3044
        readonly static Regex SSTableCQLTableNameRegEx = new Regex(@"^([a-z0-9\-_$%+=@!?<>^*&]+)\-([0-9a-f]{32})$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        readonly static string[] DateTimeFormats = new string[] { "yyyy-MM-dd HH:mm:ss,fff",
                                                                   "yyyy-MM-dd HH:mm:ss.fff"};

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

            if (checkforDate)
            {
                TimeSpan ts;

                if (TimeSpan.TryParse(strValue, out ts))
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
                if (strValue[0] == '{')
                {
                    strValue = strValue.Substring(1);
                }
                if (strValue[strValue.Length - 1] == '}')
                {
                    strValue = strValue.Substring(0, strValue.Length - 1);
                }


                if (strValue[0] == '['
                    && (strValue[strValue.Length - 1] == ']'))
                {
                    strValue = strValue.Substring(1, strValue.Length - 2);

                    var splitItems = strValue.Split(',');

                    if (splitItems.Length > 1)
                    {
                        var fmtItems = splitItems.Select(i => DetermineProperFormat(i, ignoreBraces, removeNamespace)).Sort();
                        return "[" + string.Join(", ", fmtItems) + "]";
                    }
                }
            }

            if (RemoveQuotes(strValue, out strValue, false))
            {
                var splitItems = strValue.Split(',');

                if (splitItems.Length > 1)
                {
                    var fmtItems = splitItems.Select(i => DetermineProperFormat(i, ignoreBraces, removeNamespace)).Sort();
                    return string.Join(", ", fmtItems);
                }
            }

            if(UOMRegEx.IsMatch(strValue))
            {                
                var uom = UnitOfMeasure.Create(strValue);

                if (uom != null && uom.UnitType != UnitOfMeasure.Types.Unknown)
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

            return removeNamespace ? RemoveNamespace(strValue) : strValue;
        }



        /// <summary>
        /// Parses a SSTable path file into the keyspace and table names plus the table&apos;s guid
        /// </summary>
        /// <param name="sstableFilePath"></param>
        /// <returns>
        /// null if the sstableFilePath is not a vlaid sstable path format
        /// a tuple where
        ///     Item1 -- keyspace name
        ///     Item2 -- table name
        ///     Item3 -- table&apos;s guid id
        /// </returns>
        static public Tuple<string,string,string> ParseSSTableFileIntoKSTableNames(string sstableFilePath)
        {
            // "/mnt/dse/data1/homeKS/homebase_tasktracking_ops_l3-737682f0599311e6ad0fa12fb1b6cb6e/homeKS-homebase_tasktracking_ops_l3-tmp-ka-15175-Data.db"
            // "/var/lib/cassandra/data/cfs/inode-76298b94ca5f375cab5bb674eddd3d51/cfs-inode.cfs_parent_path-tmp-ka-71-Data.db"
            // C 2.1 -- <keyspace>-<column family>-[tmp marker]-<version>-<generation>-<component>.db
            // C* 3.X -- /mnt/cassandra/data/data/keyspace1/scores-08bdd2d2126e11e78866d792efac3044/mb-1-big-Data.db
            //
            var sstableFilePathParts = sstableFilePath.Split('/');

            if (sstableFilePathParts.Length >= 3)
            {
                var ksName = sstableFilePathParts[sstableFilePathParts.Length - 3];
                var tblNameMatch = SSTableCQLTableNameRegEx.Match(sstableFilePathParts[sstableFilePathParts.Length - 2]);

                if (tblNameMatch.Success)
                {
                    return new Tuple<string, string, string>(ksName, tblNameMatch.Groups[1].Value, tblNameMatch.Groups[2].Value);
                }
            }

            return null;
        }
    }
}
