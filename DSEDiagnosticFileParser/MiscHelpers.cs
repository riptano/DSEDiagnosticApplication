using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
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

        static public IPAddress DetermineIPAddress(string possibleIPAddress)
        {
            IPAddress ipAddress = null;
            IPAddress.TryParse(possibleIPAddress, out ipAddress);
            return ipAddress;
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
        public static int UnZipFileToFolder(this IFilePath filePath, out IDirectoryPath extractedFolder,
                                                bool forceExtraction = false,
                                                bool extractToParentFolder = false,
                                                bool allowRecursiveUnZipping = true,
                                                bool renameOnceFileExtracted = true,
                                                CancellationToken? cancellationToken = null)
        {
            var extractFileInfo = LibrarySettings.ExtractFilesWithExtensions.FirstOrDefault(i => i.Item1.ToLower() == filePath.FileExtension.ToLower());
            int nbrExtractedFiles = 0;

            extractedFolder = filePath.ParentDirectoryPath;

            if (extractFileInfo != null && filePath.Exist())
            {
                var newExtractedFolder = extractToParentFolder ? extractedFolder : (IDirectoryPath) extractedFolder.MakeChild(filePath.FileNameWithoutExtension);
                var fileAttrs = filePath.GetAttributes();
                
                if (forceExtraction || extractToParentFolder || !newExtractedFolder.Exist())
                {                    
                    List<IPath> extractedFiles = null;
                    var extractType = extractFileInfo.Item2.ToLower();
                    int nbrChildrenInFolder = 0;

                    if (!newExtractedFolder.Exist())
                    {
                        newExtractedFolder.Create();
                    }
                    else
                    {
                        nbrChildrenInFolder = newExtractedFolder.Children().Count();
                    }
                    
                    Logger.Instance.InfoFormat("Extracting File \"{0}\" to directory \"{1}\"...",
                                                   filePath.PathResolved,
                                                   newExtractedFolder.PathResolved);

                    if (extractType == "zip")
                    {
                        var zip = new ICSharpCode.SharpZipLib.Zip.FastZip();
                        zip.RestoreDateTimeOnExtract = true;                        
                        zip.ExtractZip(filePath.PathResolved, newExtractedFolder.PathResolved, ICSharpCode.SharpZipLib.Zip.FastZip.Overwrite.Never, null, null, null, true);                        
                    }
                    else if (extractType == "gz")
                    {
                        using (var stream = filePath.OpenRead())
                        using (var gzipStream = new ICSharpCode.SharpZipLib.GZip.GZipInputStream(stream))
                        using (var tarArchive = ICSharpCode.SharpZipLib.Tar.TarArchive.CreateInputTarArchive(gzipStream))
                        {
                            tarArchive.SetKeepOldFiles(true);
                            tarArchive.ExtractContents(newExtractedFolder.PathResolved);                           
                        }
                    }
                    else if(extractType == "tar")
                    {
                        using (var stream = filePath.OpenRead())
                        using (var tarArchive = ICSharpCode.SharpZipLib.Tar.TarArchive.CreateInputTarArchive(stream))
                        {
                            tarArchive.SetKeepOldFiles(true);
                            tarArchive.ExtractContents(newExtractedFolder.PathResolved);
                        }
                    }
                    else
                    {
                        Logger.Instance.ErrorFormat("Unkown Extraction Type of \"{0}\" for Extracting File \"{1}\" to directory \"{2}\"",
                                                        extractType,
                                                        filePath.PathResolved,
                                                        newExtractedFolder.PathResolved);
                        return 0;
                    }

                    if(renameOnceFileExtracted)
                    {
                        var newName = filePath.Clone(filePath.FileName, "extracted");

                        if (filePath.Move(newName))
                        {
                            Logger.Instance.WarnFormat("Renamed Extraction file from \"{0}\" to file \"{1}\"",
                                                            filePath.PathResolved,
                                                            newExtractedFolder.PathResolved);
                        }
                        else
                        {
                            Logger.Instance.ErrorFormat("Renamed Extraction file from \"{0}\" to file \"{1}\" Failed...",
                                                            filePath.PathResolved,
                                                            newExtractedFolder.PathResolved);
                        }
                    }

                    extractedFiles = ((IDirectoryPath)newExtractedFolder).Children();
                    
                    Logger.Instance.InfoFormat("Extracted into folder \"{0}\" {1} files...",
                                               newExtractedFolder.PathResolved,
                                               extractedFiles == null ? 0 : extractedFiles.Count - nbrChildrenInFolder);
                    
                    if(allowRecursiveUnZipping && extractedFiles != null)
                    {                        
                        var compressedFiles = extractedFiles.Where(f => f is IFilePath
                                                                            && f != filePath                                                                       
                                                                            && LibrarySettings.ExtractFilesWithExtensions.Any(i => i.Item1.ToLower() == ((IFilePath) f).FileExtension.ToLower()))
                                                            .Cast<IFilePath>();

                        var parallelOptions = new ParallelOptions();
                       
                        if(cancellationToken.HasValue)
                        {
                            parallelOptions.CancellationToken = cancellationToken.Value;
                        }

                        Parallel.ForEach(compressedFiles, parallelOptions, compressedFile =>
                        //foreach (var compressedFile in compressedFiles)
                        {
                            IDirectoryPath tempExtractedFolder;
                            var subNbrFiles = UnZipFileToFolder(compressedFile, out tempExtractedFolder, true, true, true, renameOnceFileExtracted, cancellationToken);

                            if (subNbrFiles == 0)
                            {
                                Logger.Instance.ErrorFormat("Extraction of Sub-File \"{0}\" to directory \"{1}\" failed...",
                                                                compressedFile.PathResolved,
                                                                tempExtractedFolder.PathResolved);
                            }
                            else
                            {
                                nbrExtractedFiles += subNbrFiles;
                            }

                            parallelOptions.CancellationToken.ThrowIfCancellationRequested();
                        });
                    }
                }

                extractedFolder = (IDirectoryPath)newExtractedFolder;

                return nbrExtractedFiles;
            }

            return 0;
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
