using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Newtonsoft.Json.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Net;

namespace DSEDiagnosticFileParser
{
    internal static class MiscHelpers
    {
        static MiscHelpers()
        {
            if (LibrarySettings.SevenZipCOMFilePath == null)
            {
                Logger.Instance.WarnFormat("7Zip COM DLL was not found in \"{0}\". Extracting Files will fail.", Properties.Settings.Default.SevenZipCOMFilePath);
            }
            else if (LibrarySettings.SevenZipCOMFilePath.Exist())
            {
                SevenZip.SevenZipExtractor.SetLibraryPath(LibrarySettings.SevenZipCOMFilePath.PathResolved);
                Logger.Instance.InfoFormat("7Zip COM DLL set to \"{0}\".", LibrarySettings.SevenZipCOMFilePath.PathResolved);
            }
            else
            {
                Logger.Instance.WarnFormat("7Zip COM DLL does not exist for \"{0}\". Extracting Files will fail.", LibrarySettings.SevenZipCOMFilePath.PathResolved);
                LibrarySettings.SevenZipCOMFilePath = null;
            }
        }

        static public string RemoveQuotes(string item, bool checkbrackets = true)
        {
            RemoveQuotes(item, out item, checkbrackets);
            return item;
        }

        static public bool RemoveQuotes(string item, out string newItem, bool checkbrackets = true)
        {
            if (item.Length > 2
                    && ((item[0] == '\'' && item[item.Length - 1] == '\'')
                            || (item[0] == '"' && item[item.Length - 1] == '"')
                            || (checkbrackets && item[0] == '[' && item[item.Length - 1] == ']')))
            {
                newItem = item.Substring(1, item.Length - 2);
                return true;
            }

            newItem = item;
            return false;
        }

        static Tuple<string, string> SplitTableName(string cqlTableName, string defaultKeySpaceName)
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

        static Tuple<string, string> SplitTableName(this string cqlTableName)
        {
            if (cqlTableName[0] == '[' || cqlTableName[0] == '(')
            {
                cqlTableName = cqlTableName.Substring(1, cqlTableName.Length - 2);
            }

            return SplitTableName(cqlTableName.Replace('/', '.'), null);
        }

        static string RemoveNamespace(this string className)
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

        static bool IsIPv4(string value)
        {
            var quads = value.Split('.');

            // if we do not have 4 quads, return false
            if (!(quads.Length == 4)) return false;

            var portPos = quads[3].IndexOf(':');

            if (portPos > 0)
            {
                quads[3] = quads[3].Substring(0, portPos);
            }
            // for each quad
            foreach (var quad in quads)
            {
                int q;
                // if parse fails
                // or length of parsed int != length of quad string (i.e.; '1' vs '001')
                // or parsed int < 0
                // or parsed int > 255
                // return false
                if (!Int32.TryParse(quad, out q)
                    || !q.ToString().Length.Equals(quad.Length)
                    || q < 0
                    || q > 255)
                { return false; }

            }

            return true;
        }

        static bool IPAddressStr(string ipAddress, out string formattedAddress)
        {
            if (!string.IsNullOrEmpty(ipAddress))
            {
                if (ipAddress[0] == '/')
                {
                    ipAddress = ipAddress.Substring(1);
                }

                if (IsIPv4(ipAddress))
                {
                    var portPos = ipAddress.IndexOf(':');
                    string port = null;
                    IPAddress objIP;

                    if (portPos > 0)
                    {
                        port = ipAddress.Substring(portPos);
                        ipAddress = ipAddress.Substring(0, portPos);
                    }

                    if (IPAddress.TryParse(ipAddress, out objIP))
                    {
                        if (port == null)
                        {
                            formattedAddress = objIP.ToString();
                        }
                        else
                        {
                            formattedAddress = objIP.ToString() + port;
                        }
                        return true;
                    }
                }
            }

            formattedAddress = ipAddress;
            return false;
        }

        static string DetermineProperFormat(string strValue, bool ignoreBraces = false, bool removeNamespace = true)
        {
            var result = DetermineProperObjectFormat(strValue, ignoreBraces, removeNamespace);

            return result == null ? null : (result is string ? (string)result : result.ToString());
        }

        static object DetermineProperObjectFormat(string strValue, bool ignoreBraces = false, bool removeNamespace = true)
        {
            string strValueA;
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

            if (IPAddressStr(strValue, out strValueA))
            {
                return strValueA;
            }

            if (strValue.IndexOfAny(new char[] { '/', '\\', ':' }) >= 0)
            {
                return strValue;
            }

            if (StringFunctions.ParseIntoNumeric(strValue, out item))
            {
                return item;
            }

            return removeNamespace ? RemoveNamespace(strValue) : strValue;
        }

        public static V TryGetValue<K, V>(this Dictionary<K, V> collection, K key)
            where V : class
        {
            V getValue;

            if (collection != null && collection.TryGetValue(key, out getValue))
            {
                return getValue;
            }

            return default(V);
        }

        public static T ParseEnumString<T>(this string enumValue)
            where T : struct
        {
            T value;
            if(Enum.TryParse<T>(enumValue, out value))
            {
                return value;
            }

            return default(T);
        }

        public static void ParseEnumString<T>(this string enumValue, ref T enumSetValue)
            where T : struct
        {
            T value;
            if (Enum.TryParse<T>(enumValue, out value))
            {
                enumSetValue = value;
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="extractedFolder"></param>
        /// <param name="forceExtraction"></param>
        /// <param name="extractOnlyToThisFolder"></param>
        /// <returns></returns>
        public static bool UnZipFileToFolder(this IFilePath filePath, out IDirectoryPath extractedFolder,
                                                bool forceExtraction = false,
                                                bool extractToParentFolder = false,
                                                bool allowRecursiveUnZipping = true)
        {
            extractedFolder = filePath.ParentDirectoryPath;

            if (filePath.Exist()
                    && (forceExtraction || LibrarySettings.ExtractFilesWithExtensions.Contains(filePath.FileExtension)))
            {
                var newExtractedFolder = extractToParentFolder ? extractedFolder : (IDirectoryPath) extractedFolder.MakeChild(filePath.FileNameWithoutExtension);

                if (!newExtractedFolder.Exist())
                {
                    if(LibrarySettings.SevenZipCOMFilePath == null)
                    {
                        throw new System.IO.FileLoadException(string.Format("7Zip COM DLL is missing, cannot Extract file \"{0}\"", filePath.PathResolved),
                                                                    Properties.Settings.Default.SevenZipCOMFilePath);
                    }

                    List<IPath> extractedFiles = null;

                    using (var extractor = new SevenZip.SevenZipExtractor(filePath.PathResolved))
                    {
                        //extr.Extracting += new EventHandler<ProgressEventArgs>(extr_Extracting);
                        //extr.FileExtractionStarted += new EventHandler<FileInfoEventArgs>(extr_FileExtractionStarted);
                        //extr.FileExists += new EventHandler<FileOverwriteEventArgs>(extr_FileExists);
                        //extr.ExtractionFinished += new EventHandler<EventArgs>(extr_ExtractionFinished);

                        Logger.Instance.InfoFormat("Extracting File \"{0}\" to directory \"{1}\" which will contain {2} files...",
                                                    filePath.PathResolved,
                                                    newExtractedFolder.PathResolved,
                                                    extractor.FilesCount);

                        extractor.ExtractArchive(newExtractedFolder.PathResolved);

                        extractedFiles = ((IDirectoryPath)newExtractedFolder).Children();

                        Logger.Instance.InfoFormat("Extracted folder \"{0}\" which contains {1} files...",
                                                   newExtractedFolder.PathResolved,
                                                   extractedFiles.Count);
                    }

                    if(allowRecursiveUnZipping && extractedFiles != null)
                    {
                        var compressedFiles = extractedFiles.Where(f => f is IFilePath
                                                                            && LibrarySettings.ExtractFilesWithExtensions.Contains(((IFilePath)f).FileExtension))
                                                            .Cast<IFilePath>();

                        foreach (var compressedFile in compressedFiles)
                        {
                            IDirectoryPath tempExtractedFolder;

                            if(!UnZipFileToFolder(compressedFile, out tempExtractedFolder, true, true))
                            {
                                Logger.Instance.WarnFormat("Extraction of Sub-File \"{0}\" to directory \"{1}\" failed...",
                                                            compressedFile.PathResolved,
                                                            tempExtractedFolder.PathResolved);
                            }
                        }
                    }
                }

                extractedFolder = (IDirectoryPath)newExtractedFolder;

                return true;
            }

            return false;
        }

        #region JSON

        public static Dictionary<string, JObject> TryGetValues(this JObject jsonObj)
        {
            return jsonObj == null ? null : jsonObj.ToObject<Dictionary<string, JObject>>();
        }

        public static IEnumerable<T> TryGetValues<T>(this JToken jtoken, string key)
        {
            return jtoken == null ? null : jtoken.SelectToken(key).ToObject<IEnumerable<T>>();
        }

        public static JToken TryGetValue(this JObject jsonObj, string key)
        {
            return jsonObj == null ? null : jsonObj.GetValue(key);
        }

        public static T TryGetValue<T>(this JObject jsonObj, string key)
        {
            if (jsonObj != null)
            {
                return jsonObj.Value<T>(key);
            }

            return default(T);
        }

        public static T TryGetValue<T>(this JToken jtoken, string key)
        {
            if (jtoken != null)
            {
                return jtoken.Value<T>(key);
            }

            return default(T);
        }

        public static T TryGetValue<T>(this JToken jtoken, string key, ref T updateField)
        {
            if (jtoken != null)
            {
                return updateField = jtoken.Value<T>(key);
            }

            return default(T);
        }

        public static T TryGetValue<T>(this JToken jtoken)
        {
            if (jtoken != null)
            {
                return jtoken.ToObject<T>();
            }

            return default(T);
        }

        public static JToken TryGetValue(this JToken jtoken, string key)
        {
            if (jtoken != null)
            {
                return jtoken.SelectToken(key);
            }

            return null;
        }

        public static T TryGetValue<T>(this JToken jtoken, int index)
        {
            if (jtoken != null)
            {
                return jtoken.Value<T>(index);
            }

            return default(T);
        }

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TClass"></typeparam>
        /// <typeparam name="TProperty"></typeparam>
        /// <param name="jtoken"></param>
        /// <param name="input"></param>
        /// <param name="outObj"></param>
        /// <param name="outExpr"></param>
        /// <example>
        /// <code>
        /// jsonObj.TryGetValue("10.14.150.121").TryGetValue("devices").TryGetValue("other").NullSafeSet(myInstance, i => i.field);
        /// </code>
        /// </example>
        public static void NullSafeSet<TClass, TProperty>(this JToken jtoken, TClass outObj, Expression<Func<TClass, TProperty>> outExpr)
        {
            if (jtoken != null)
            {
                var expr = (MemberExpression)outExpr.Body;
                var prop = (PropertyInfo)expr.Member;

                var jsonValue = jtoken.ToObject(prop.PropertyType);

                if (jsonValue != null)
                {
                    prop.SetValue(outObj, jsonValue, null);
                }
            }
        }

        public static void NullSafeSet<TClass, TProperty>(this JToken jtoken, int index, TClass outObj, Expression<Func<TClass, TProperty>> outExpr)
        {
            if (jtoken != null)
            {
                var expr = (MemberExpression)outExpr.Body;
                var prop = (PropertyInfo)expr.Member;

                var jsonValue = jtoken.ElementAtOrDefault(index)?.ToObject(prop.PropertyType);

                if (jsonValue != null)
                {
                    prop.SetValue(outObj, jsonValue, null);
                }
            }
        }

        public static void NullSafeSet<JValue>(this JToken jtoken, int index, Action<JValue> setOutput)
        {
            if (jtoken != null)
            {
                var jsonValue = jtoken.ElementAtOrDefault(index)?.ToObject(typeof(JValue));

                if (jsonValue != null)
                {
                    setOutput((JValue)jsonValue);
                }
            }
        }

        public static void NullSafeSet<JValue>(this JToken jtoken, Action<JValue> setOutput)
        {
            if (jtoken != null)
            {
                var jsonValue = jtoken.ToObject(typeof(JValue));

                if (jsonValue != null)
                {
                    setOutput((JValue)jsonValue);
                }
            }
        }

        public static void EmptySafeSet<TClass, TProperty>(this JToken jtoken, int index, TClass outObj, Expression<Func<TClass, TProperty>> outExpr)
        {
            if (jtoken != null)
            {
                var expr = (MemberExpression)outExpr.Body;
                var prop = (PropertyInfo)expr.Member;

                var jsonValue = jtoken.ElementAtOrDefault(index)?.ToObject(prop.PropertyType);

                if (jsonValue != null)
                {
                    if (prop.PropertyType.IsClass || prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        if (prop.GetValue(outObj) == null)
                        {

                            prop.SetValue(outObj, jsonValue, null);
                        }
                    }
                    else
                    {
                        prop.SetValue(outObj, jsonValue, null);
                    }
                }
            }
        }


        public static void EmptySafeSet<TClass, TProperty>(this JToken jtoken, TClass outObj, Expression<Func<TClass, TProperty>> outExpr)
        {
            if (jtoken != null)
            {
                var expr = (MemberExpression)outExpr.Body;
                var prop = (PropertyInfo)expr.Member;

                var jsonValue = jtoken.ToObject(prop.PropertyType);

                if (jsonValue != null)
                {
                    if (prop.PropertyType.IsClass || prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        if (prop.GetValue(outObj) == null)
                        { prop.SetValue(outObj, jsonValue, null); }
                    }
                    else
                    {
                        prop.SetValue(outObj, jsonValue, null);
                    }
                }
            }
        }
        #endregion

    }
}
