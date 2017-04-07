using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Common;
using Newtonsoft.Json;

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
        bool IsCommonToDataCenter { get; }
        ConfigTypes Type { get; }
        string Property { get; }
        string Value { get; }
        string NormalizeValue();
        string NormalizeProperty();
        object ParsedValue();

        bool Match(IConfigurationLine configItem);
        bool Match(IDataCenter dataCenter,
                    string property,
                    string value,
                    ConfigTypes type,
                    SourceTypes source);

        object ToDump();
	}

    public sealed class YamlConfigurationLine : IConfigurationLine
    {

        public sealed class ConfigTypeMapper
        {
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

            public ConfigTypes ConfigType { get; set; }
            public Regex MatchRegEx { get; private set; }

            private string _regexString = null;
            public string RegExString
            {
                get { return this._regexString; }
                set
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
                set
                {
                    this._containsString = value;

                    if(this._containsString != null)
                    {
                        this._regexString = null;
                    }
                }
            }

            public MatchActions MatchAction { get; set; }

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
                    if (this.MatchAction.HasFlag(MatchActions.Equals))
                    {
                        if (string.Equals(this.ContainsString, compareString, StringComparison.OrdinalIgnoreCase)) return this.ConfigType;
                    }
                    else if (this.MatchAction.HasFlag(MatchActions.Contains))
                    {
                        if (compareString.IndexOf(this.ContainsString, StringComparison.OrdinalIgnoreCase) >= 0) return this.ConfigType;
                    }
                    else if (this.MatchAction.HasFlag(MatchActions.StartsWith))
                    {
                        if (compareString.StartsWith(this.ContainsString, StringComparison.OrdinalIgnoreCase)) return this.ConfigType;
                    }
                    else if (this.MatchAction.HasFlag(MatchActions.EndsWith))
                    {
                        if (compareString.EndsWith(this.ContainsString, StringComparison.OrdinalIgnoreCase)) return this.ConfigType;
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
                                    ConfigTypes type = ConfigTypes.Unkown,
                                    SourceTypes source = SourceTypes.Yaml)
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
            this.LineNbr = lineNbr;
            this.Property = property;
            this.Value = value;
            this.Source = source;
            this.Node = node;
        }

        #region IParsed
        public SourceTypes Source { get; private set; }
        public IPath Path { get; private set; }
        public Cluster Cluster { get { return this.DataCenter?.Cluster; } }
        public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
        public INode Node
        {
            get;
            private set;
        }
        public int Items { get { return this.DataCenter is DataCenter ? 1 : this.DataCenter.Nodes.Count(); } }
        public uint LineNbr { get; private set; }

        #endregion

        #region IConfigurationLine

        public bool IsCommonToDataCenter
        {
            get
            {
                return this.DataCenter is DataCenter
                            ? this.DataCenter.Nodes.Count() == 1
                            : this.DataCenter.Nodes.First().DataCenter.Nodes.All(n => this.Node.Equals(n));
            }
        }
        public ConfigTypes Type { get; private set; }
        public string Property { get; private set; }

        private string _normalizedProperty = null;
        static readonly Regex NormalizePropIndexRegEx = new Regex("\\[\\d+\\]",
                                                                    RegexOptions.IgnoreCase
                                                                        | RegexOptions.Singleline
                                                                        | RegexOptions.Compiled);
        static readonly string NormalizePropRegexReplace = "[X]";

        public string NormalizeProperty()
        {
            return this._normalizedProperty == null
                        ? this._normalizedProperty = NormalizeProperty(this.Property)
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

        private object _parsedValue = null;
        public object ParsedValue()
        {
            if(this._parsedValue == null)
            {
                if (string.IsNullOrEmpty(this.Value)) return null;

                if (this.Value.Contains(','))
                {
                    var collectionValue = this.Value;

                    if(collectionValue.Last() == '}'
                        && collectionValue.IndexOf('{') > 0)
                    {
                        return this._parsedValue = (IDictionary<string,object>) JsonConvert.DeserializeObject<Dictionary<string, object>>(collectionValue);
                    }

                    return this._parsedValue = StringFunctions.Split(StringHelpers.RemoveQuotes(collectionValue), ",")
                                                                .Select(s => StringHelpers.DetermineProperObjectFormat(s, true, false, true, this.Property));
                }

                this._parsedValue = StringHelpers.DetermineProperObjectFormat(this.Value, true, false, true, this.Property);
            }

            return this._parsedValue;
        }
        public bool Match(IConfigurationLine configItem)
        {
            return configItem != null
                    && this.Type == configItem.Type
                    && this.Node.DataCenter != null
                    && configItem.DataCenter != null
                    && this.DataCenter.Equals(configItem.DataCenter)
                    && this.NormalizeProperty() == configItem.NormalizeProperty()
                    && this.NormalizeValue() == configItem.NormalizeValue();
        }

        public bool Match(IDataCenter dataCenter,
                            string property,
                            string value,
                            ConfigTypes type,
                            SourceTypes source)
        {
            return dataCenter != null
                    && this.DataCenter != null
                    && this.Type == type
                    && this.DataCenter.Equals(dataCenter)
                    && this.NormalizeProperty() == NormalizeProperty(property)
                    && this.NormalizeValue() == NormalizeValue(value);
        }

        public object ToDump()
        {
            return new
            {
                Common = this.IsCommonToDataCenter,
                Node = this.Node,
                File = this.Path.PathResolved,
                Source = this.Source,
                Type = this.Type,
                Property = this.Property,
                Value = this.Value,
                ParsedValueType = this.ParsedValue()?.GetType().Name,
                ParsedValue = this.ParsedValue()
            };
        }

        #endregion

        public override string ToString()
        {
            return string.Format("{3}.YamlConfigLine{{{0}{{{1}:{2}}}}}", this.Node?.Id.ToString() ?? this.DataCenter.Name, this.Property, this.Value, this.Type);
        }

        #region static methods

        static public IConfigurationLine AddConfiguration(IFilePath configFile,
                                                            INode node,
                                                            uint lineNbr,
                                                            string property,
                                                            string value,
                                                            ConfigTypes type = ConfigTypes.Unkown,
                                                            SourceTypes source = SourceTypes.Yaml)
        {
            if (type == ConfigTypes.Unkown)
            {
                type = ConfigTypeMapper.FindConfigType(configFile);
            }

            var currentConfig = node.DataCenter.ConfigurationMatch(property, value, type, source);

            if(currentConfig == null)
            {
                ((DataCenter)node.DataCenter).AssociateItem(currentConfig = new YamlConfigurationLine(configFile, node, lineNbr, property, value, type, source));
            }
            else
            {
                lock(currentConfig)
                {
                    if(currentConfig.DataCenter is DataCenter)
                    {
                        var placeHolderDC = new PlaceholderDataCenter(currentConfig.Node, currentConfig.DataCenter.Name, "CommonConfig");
                        var configNode = new Node(currentConfig.Cluster, currentConfig.Node.Id.Clone());

                        configNode.Id.AddIPAddress(node.Id);
                        configNode.SetNodeToDataCenter(placeHolderDC);
                        placeHolderDC.AddNode(node);
                        ((YamlConfigurationLine)currentConfig).Node = configNode;
                    }
                    else
                    {
                        ((PlaceholderDataCenter)currentConfig.DataCenter).AddNode(node);
                        currentConfig.Node.Id.AddIPAddress(node.Id);
                    }
                }
            }

            return currentConfig;
        }

        static string NormalizeValue(string value)
        {
            if (string.IsNullOrEmpty(value)) return value;

            if(value.Contains(','))
            {
                if (value.Last() == '}'
                        && value.IndexOf('{') > 0)
                {
                    var collection = JsonConvert.DeserializeObject<Dictionary<string, object>>(value)
                                        .OrderBy(d => d.Key);
                    return "{" + string.Join(", ", collection.Select(p => p.Key + ":" + p.Value.ToString())) + "}";
                }

                var valueItems = StringFunctions.Split(value, ",")
                                    .Select(s => StringHelpers.DetermineProperFormat(s))
                                    .Sort();
                return string.Join(", ", valueItems);
            }

            return StringHelpers.DetermineProperFormat(value);
        }

        static string NormalizeProperty(string property)
        {
            return NormalizePropIndexRegEx.Replace(property, NormalizePropRegexReplace);
        }
        #endregion
    }
}
