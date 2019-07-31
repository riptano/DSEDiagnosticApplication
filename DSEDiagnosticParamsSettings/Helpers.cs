using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft;
using Newtonsoft.Json;
using Common;
using Common.Path;
using DSEDiagnosticLogger;

namespace DSEDiagnosticParamsSettings
{
    public static class Helpers
    {        
        public static T ParseEnumString<T>(string enumString)
            where T : struct
        {
            enumString = enumString.Replace("|", ",").Replace(" + ", ",").Replace(" - ", ", !").Replace(" ~", ", !");

            var enumValues = enumString.Split(',');
            T enumValue = (T)Enum.Parse(typeof(T), string.Join(",", enumValues.Where(i => !i.TrimStart().StartsWith("!"))), true);
            string removeValues = string.Join(",", enumValues.Where(i => i.TrimStart().StartsWith("!")).Select(i => i.TrimStart().Substring(1).TrimStart()));

            if (!string.IsNullOrEmpty(removeValues))
            {
                enumValue &= ~(dynamic)(T)Enum.Parse(typeof(T), removeValues, true);
            }

            return enumValue;
        }

        public static T ParseEnumString<T>(string enumString, T appendValue)
            where T : struct
        {
            enumString = enumString?.Trim();

            if (string.IsNullOrEmpty(enumString))
            {
                return default(T);
            }

            if (enumString[0] == '|'
                    || enumString[0] == ','
                    || enumString[0] == '~')
            {
                return ParseEnumString<T>(appendValue.ToString()
                                                + " "
                                                + enumString);
            }

            if (enumString[0] == '+'
                    || enumString[0] == '-')
            {
                return ParseEnumString<T>(appendValue.ToString()
                                                + " "
                                                + enumString[0]
                                                + " "
                                                + enumString.Substring(1));
            }

            if (enumString[0] == '!')
            {
                return ParseEnumString<T>(appendValue.ToString() + ", " + enumString);
            }

            return ParseEnumString<T>(enumString);
        }

        public static IDirectoryPath MakeDirectoryPath(string dirPath, IDirectoryPath defaultPath = null)
        {
            if (string.IsNullOrEmpty(dirPath)) return null;

            var newPath = PathUtils.BuildDirectoryPath(dirPath);

            if (defaultPath != null && newPath.IsRelativePath && defaultPath.IsAbsolutePath)
            {
                if (newPath.MakePathFrom((IAbsolutePath)defaultPath, out IAbsolutePath absPath))
                {
                    newPath = (IDirectoryPath)absPath;
                }
            }

            return newPath;
        }
        public static IFilePath MakeFilePath(string filePath, IDirectoryPath defaultDirPath = null, IFilePath defaultFile = null)
        {
            if (string.IsNullOrEmpty(filePath)) return null;

            if(defaultDirPath == null && defaultFile == null)
            {
                return PathUtils.BuildFilePath(filePath);
            }

#pragma warning disable IDE0019 // Use pattern matching
            var newFile = PathUtils.BuildPath(filePath,
                                                defaultDirPath?.Path,
                                                defaultFile?.FileExtension,
                                                true,
                                                true,
                                                true,
                                                false) as IFilePath;
#pragma warning restore IDE0019 // Use pattern matching

            if (newFile == null)
            {
                newFile = PathUtils.BuildFilePath(filePath);
            }

            return newFile;
        }

        public static Dictionary<string, string> CreateDictionary(string configString)
        {
            var configObj = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string>[]>(configString);
            var dict = new Dictionary<string, string>();

            if (configObj != null)
            {
                foreach (var item in configObj)
                {
                    dict.Add(item.Item1, item.Item2);
                }
            }

            return dict;
        }

        public static T ReadJsonFileIntoObject<T>(string jsonStringOrFile)
        {
            if (string.IsNullOrEmpty(jsonStringOrFile))
            {
                Logger.Instance.DebugFormat("ReadJsonFileIntoObject detected a Null or empty string. \"jsonStringOrFile\" Value is \"{0}\", returning default value", jsonStringOrFile);
                return default(T);
            }

            try
            {

                if (jsonStringOrFile[0] == '{' || jsonStringOrFile.IndexOf('{') > 0)
                {
                    //string removeComments = jsonStringOrFile;
                    //DSEDiagnosticLibrary.StringHelpers.RemoveInLineComment(jsonStringOrFile, out removeComments);

                    return JsonConvert.DeserializeObject<T>(jsonStringOrFile);
                }

                IFilePath jsonPath = null;
                List<string> triedPaths = new List<string>();

                foreach (var filePath in jsonStringOrFile.Split(','))
                {
                    jsonPath = Common.Path.PathUtils.BuildFilePath(filePath);

                    if (jsonPath.Exist())
                    {
                        break;
                    }
                    triedPaths.Add(jsonPath.PathResolved);
                    jsonPath = null;
                }

                if (jsonPath == null)
                {
                    throw new System.IO.FileNotFoundException(string.Format("Json File Path was not found. Tried: {{{0}}}", string.Join(", ", triedPaths)), jsonStringOrFile);
                }

                return ReadJsonFileIntoObject<T>(jsonPath.ReadAllText());
            }
            catch (Exception e)
            {               
                Logger.Instance.Error(string.Format("Invalid Json Value or File Path. Value is: \"{0}\"", jsonStringOrFile), e);
                throw;
            }
        }

        public static DateTimeRange ToDateTimeRange(this DateTime dt)
        {
            return new DateTimeRange(dt, DateTime.MaxValue);
        }
        
        public static List<string> CreateMergeList(string strList, IEnumerable<string> defaultCollection = null)
        {
            var splitItems = Common.StringFunctions.Split(strList, ',');
            var merge = splitItems.Any(i => !string.IsNullOrEmpty(i) && (i[0] == '+' || i[0] == '-'));

            if (merge)
            {
                bool defaultActionAdd = splitItems[0][0] != '-';

                if (defaultCollection == null)
                {
                    return splitItems.Where(i => !string.IsNullOrEmpty(i) && (i[0] == '+' || defaultActionAdd)).Select(i => i[0] != '+' ? i : i.Substring(1)).ToList();
                }

                var mergeList = defaultCollection.ToList();

                foreach (var item in splitItems)
                {
                    if (!string.IsNullOrEmpty(item))
                    {
                        if (item[0] == '+')
                        {
                            mergeList.Add(item.Substring(1));
                        }
                        else if (item[0] == '-')
                        {
                            mergeList.Remove(item.Substring(1));
                        }
                        else if (defaultActionAdd)
                        {
                            mergeList.Add(item);
                        }
                        else
                        {
                            mergeList.Remove(item);
                        }
                    }
                }

                return mergeList;
            }

            return splitItems.ToList();
        }
        
        public static string SettingValues(Type classType)           
        {
            StringBuilder settingsValues = new StringBuilder();
            var settingValueList = SettingValueList(classType);

            foreach (var settingItem in settingValueList)
            {
                settingsValues.AppendLine(string.Format("\t{0} ({1}): {2}", settingItem.Item1, settingItem.Item2, settingItem.Item3));
            }

            return settingsValues.ToString();
        }

        public static IEnumerable<Tuple<string, string, string>> SettingValueList(Type classType)           
        {
            var settingsValues = new List<Tuple<string, string, string>>();
            var fields = classType.GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);

            foreach (var fld in fields)
            {
                var item = fld.GetValue(null);
                var strItem = item is Enum ? item.ToString() : Common.JSONExtensions.ToJSON(item, true);
                var strType = fld.FieldType.IsGenericType
                                    ? string.Format("{0}<{1}>",
                                                        fld.FieldType.Name,
                                                        string.Join(", ", fld.FieldType.GenericTypeArguments.Select(t => t.Name)))
                                    : fld.FieldType.Name;

                settingsValues.Add(new Tuple<string, string, string>(fld.Name, strType, strItem));
            }

            var props =classType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);

            foreach (var prop in props)
            {
                var item = prop.GetValue(null);
                var strItem = item is Enum ? item.ToString() : Common.JSONExtensions.ToJSON(item, true);
                var strType = prop.PropertyType.IsGenericType
                                    ? string.Format("{0}<{1}>",
                                                        prop.PropertyType.Name,
                                                        string.Join(", ", prop.PropertyType.GenericTypeArguments.Select(t => t.Name)))
                                    : prop.PropertyType.Name;

                settingsValues.Add(new Tuple<string, string, string>(prop.Name, strType, strItem));
            }

            return settingsValues;
        }
    }
}
