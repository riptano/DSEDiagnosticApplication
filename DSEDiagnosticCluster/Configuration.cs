using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticLibrary
{
    public enum ConfigTypes
    {
        Unkown = 0,
        Cassandra,
        DSE,
        Hadoop,
        Solr,
        Spark,
        Snitch
    }

    public interface IConfigurationLine : IParsed
	{
        ConfigTypes Type { get; }
        string Property { get; }
        string Value { get; }
        string NormalizeValue();
        string NormalizeProperty();

        bool Match(IConfigurationLine configItem);
	}

    public sealed class YamlConfigurationLine : IConfigurationLine
    {

        public sealed class ConfigTypeMapper
        {
            public static List<ConfigTypeMapper> Mappings = new List<ConfigTypeMapper>();

            [Flags]
            public enum MatchActions
            {
                Path = 0x0001,
                FileNamewExtension = 0x0002,
                FileNameOnly = 0x0004,
                FileExtension = 0x0008,
                StartsWith = 0x0010,
                EndsWith = 0x0020,
                Contains = 0x0040,
                Equals = 0x0080,
                Default = Path | Contains
            }

            private ConfigTypeMapper() { }
            public ConfigTypeMapper(string containsStringOrRegEx, bool isRegEx, ConfigTypes configType, MatchActions matchActions = MatchActions.Default)
            {
                this.ConfigType = configType;

                if(isRegEx)
                {
                    this.RegExString = containsStringOrRegEx;
                }
                else
                {
                    this.ContainsString = containsStringOrRegEx;
                }

                this.MatchAction = matchActions;

                if(LibrarySettings.ConfigTypeMappers != null) LibrarySettings.ConfigTypeMappers.Add(this);
            }

            public ConfigTypes ConfigType { get; internal set; }
            public Regex MatchRegEx { get; private set; }

            private string _regexString = null;
            public string RegExString
            {
                get { return this._regexString; }
                internal set
                {
                    if(this.MatchRegEx == null)
                    {
                        if(!string.IsNullOrEmpty(value))
                        {
                            this.MatchRegEx = new Regex(value, RegexOptions.Compiled | RegexOptions.IgnoreCase);
                        }
                    }
                    else if(this._regexString != value)
                    {
                        this.MatchRegEx = string.IsNullOrEmpty(value)
                                                ? null
                                                : new Regex(value, RegexOptions.Compiled | RegexOptions.IgnoreCase);
                    }
                    this._regexString = value;

                    if(this._regexString != null)
                    {
                        this._containsString = null;
                    }
                }
            }

            private string _containsString = null;
            public string ContainsString
            {
                get { return this._containsString; }
                internal set
                {
                    this._containsString = value;

                    if(this._containsString != null)
                    {
                        this._regexString = null;
                    }
                }
            }

            public MatchActions MatchAction { get; internal set; }

            public ConfigTypes GetConfigType(IFilePath configFilePath)
            {
                string compareString = configFilePath.PathResolved;

                if((int) this.MatchAction == 0)
                {
                    this.MatchAction = MatchActions.Default;
                }

                if(this.MatchAction.HasFlag(MatchActions.FileExtension))
                {
                    compareString = configFilePath.FileExtension;
                }
                else if (this.MatchAction.HasFlag(MatchActions.FileNamewExtension))
                {
                    compareString = configFilePath.FileName;
                }
                else if (this.MatchAction.HasFlag(MatchActions.FileNameOnly))
                {
                    compareString = configFilePath.FileNameWithoutExtension;
                }

                if(this.MatchRegEx == null)
                {
                    switch (this.MatchAction)
                    {
                        case MatchActions.Equals:
                            if (string.Equals(this.ContainsString, compareString, StringComparison.OrdinalIgnoreCase)) return this.ConfigType;
                            break;
                        case MatchActions.Contains:
                            if(compareString.IndexOf(this.ContainsString, StringComparison.OrdinalIgnoreCase) >= 0) return this.ConfigType;
                            break;
                        case MatchActions.StartsWith:
                            if (compareString.StartsWith(this.ContainsString, StringComparison.OrdinalIgnoreCase)) return this.ConfigType;
                            break;
                        case MatchActions.EndsWith:
                            if (compareString.EndsWith(this.ContainsString, StringComparison.OrdinalIgnoreCase)) return this.ConfigType;
                            break;
                        default:
                            break;
                    }
                }
                else if(this.MatchRegEx.IsMatch(compareString))
                {
                    return this.ConfigType;
                }

                return ConfigTypes.Unkown;
            }

            public static ConfigTypes FindConfigType(IFilePath configFilePath)
            {
                var configType = LibrarySettings.ConfigTypeMappers?.FirstOrDefault(t => t.GetConfigType(configFilePath) != ConfigTypes.Unkown);

                return configType == null ? ConfigTypes.Unkown : configType.ConfigType;
            }
        }

        private YamlConfigurationLine() { }

        public YamlConfigurationLine(IFilePath configFile,
                                    INode node,
                                    uint lineNbr,
                                    string property,
                                    string value,
                                    ConfigTypes type = ConfigTypes.Unkown)
        {
            if(type == ConfigTypes.Unkown)
            {
                this.Type = ConfigTypeMapper.FindConfigType(configFile);
            }
            else
            {
                this.Type = type;
            }
            this.Path = configFile;
            this.Node = node;
            this.LineNbr = lineNbr;
            this.Property = property;
            this.Value = value;
        }

        #region IParsed
        public SourceTypes Source { get { return SourceTypes.Yaml; } }
        public IPath Path { get; private set; }
        public Cluster Cluster { get { return this?.Node.Cluster; } }
        public IDataCenter DataCenter { get { return this?.Node.DataCenter; } }
        public INode Node { get; private set; }
        public int Items { get { return 1; } }
        public uint LineNbr { get; private set; }

        #endregion

        #region IConfigurationLine

        public ConfigTypes Type { get; private set; }
        public string Property { get; private set; }

        private string _normalizedProperty = null;
        readonly Regex NormalizePropIndexRegEx = new Regex("\\[\\d+\\]",
                                                            RegexOptions.IgnoreCase
                                                            | RegexOptions.Singleline
                                                            | RegexOptions.Compiled);
        readonly string NormalizePropRegexReplace = "[X]";

        public string NormalizeProperty()
        {
            return this._normalizedProperty == null
                        ? this._normalizedProperty = NormalizePropIndexRegEx.Replace(this.Property, NormalizePropRegexReplace)
                        : this._normalizedProperty;
        }
        public string Value { get; private set; }

        private string _normalizedValue = null;
        public string NormalizeValue()
        {
            return this._normalizedValue == null
                        ? this._normalizedValue = NormalizeValue(this.Value)
                        : this._normalizedValue;
        }

        public bool Match(IConfigurationLine configItem)
        {
            return configItem != null
                    && this.Type == configItem.Type
                    && this.Node.DataCenter.Equals(configItem.Node.DataCenter)
                    && this.NormalizeProperty() == configItem.NormalizeProperty()
                    && this.NormalizeValue() == configItem.NormalizeValue();
        }

        #endregion

        public override string ToString()
        {
            return string.Format("{3}.YamlConfigLine{{{0}{{{1}:{2}}}}}", this.Node?.Id, this.Property, this.Value, this.Type);
        }

        #region static methods

        static string NormalizeValue(string value)
        {
            if (string.IsNullOrEmpty(value)) return value;

            if(value.Contains(','))
            {
                var valueItems = StringFunctions.Split(value, ",")
                                    .Select(s => StringHelpers.DetermineProperFormat(s))
                                    .Sort();
                return string.Join(", ", valueItems);
            }

            return StringHelpers.DetermineProperFormat(value);
        }

        #endregion
    }
}
