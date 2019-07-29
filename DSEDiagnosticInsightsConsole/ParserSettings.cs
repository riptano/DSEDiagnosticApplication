using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Path;

namespace DSEDiagnosticInsightsConsole
{
    public static class ParserSettings
    {
        #region Functions

        public static DateTimeRange ToDateTimeRange(this DateTime dt)
        {
            return new DateTimeRange(dt, DateTime.MaxValue);
        }

        public static T ParseEnumString<T>(string enumString)
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

            return ParserSettings.ParseEnumString<T>(enumString);
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

        public static string SettingValues()
        {
            StringBuilder settingsValues = new StringBuilder();
            var settingValueList = SettingValueList();

            foreach (var settingItem in settingValueList)
            {
                settingsValues.AppendLine(string.Format("\t{0} ({1}): {2}", settingItem.Item1, settingItem.Item2, settingItem.Item3));
            }

            return settingsValues.ToString();
        }

        public static IEnumerable<Tuple<string, string, string>> SettingValueList()
        {
            var settingsValues = new List<Tuple<string, string, string>>();
            var fields = typeof(ParserSettings).GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);

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

            var props = typeof(ParserSettings).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);

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
        #endregion

        public static IDirectoryPath ExcelWorkbookPath = MakeDirectoryPath(Properties.Settings.Default.ExcelWorkbookPath);
        public static IFilePath ExcelWorkbookFile = MakeFilePath(Properties.Settings.Default.ExcelWorkbookFile, ExcelWorkbookPath);

        public static List<string> IgnoreKeySpaces = DSEDiagnosticLibrary.LibrarySettings.SystemKeyspaces.Append(DSEDiagnosticLibrary.LibrarySettings.DSEKeyspaces).ToList();
        public static List<string> WarnWhenKSTblIsDetected
        {
            get { return DSEDiagnosticLibrary.LibrarySettings.TablesUsageFlag.ToList(); }
            set { DSEDiagnosticLibrary.LibrarySettings.TablesUsageFlag = value?.ToArray() ?? new string[0]; }
        }

        public static List<string> WhiteListKeyspaceInWS = Properties.Settings.Default.WhiteListKeyspaceInWS.ToList();
        public static IEnumerable<string> IgnoreWarningsErrosInKeySpaces = DSEDiagnosticLibrary.LibrarySettings.SystemKeyspaces.Append(DSEDiagnosticLibrary.LibrarySettings.DSEKeyspaces).ToArray();

        public static IFilePath ExcelFileTemplatePath = MakeFilePath(Properties.Settings.Default.ExcelFileTemplatePath, ExcelWorkbookPath?.ParentDirectoryPath);

        public static bool AppendToWorkSheet
        {
            get { return DSEDiagtnosticToExcel.LibrarySettings.AppendToWorkSheet; }
            set { DSEDiagtnosticToExcel.LibrarySettings.AppendToWorkSheet = value; }
        }

        public static bool LoadSystemDSEKeyspaces = Properties.Settings.Default.LoadSystemDSEKeyspaces;
        
    }
}
