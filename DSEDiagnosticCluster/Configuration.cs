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
        Spark
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
                this.Type = DetermineType(configFile);
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

        static ConfigTypes DetermineType(IFilePath configFile)
        {
            var strFilePath = configFile.PathResolved.ToUpper();

            if(strFilePath.Contains("CASSANDRA"))
            {
                return ConfigTypes.Cassandra;
            }
            if (strFilePath.Contains("DSE"))
            {
                return ConfigTypes.DSE;
            }
            if (strFilePath.Contains("HADOOP"))
            {
                return ConfigTypes.Hadoop;
            }
            if (strFilePath.Contains("SOLR"))
            {
                return ConfigTypes.Solr;
            }
            if (strFilePath.Contains("SPARK"))
            {
                return ConfigTypes.Spark;
            }

            var strDir = configFile.PathResolved.Split(System.IO.Path.DirectorySeparatorChar).Take(2).Select(i => i.ToUpper());

            if (strDir.Any(i => i.Contains("CASSANDRA")))
            {
                return ConfigTypes.Cassandra;
            }
            if (strDir.Any(i => i.Contains("DSE")))
            {
                return ConfigTypes.DSE;
            }
            if (strDir.Any(i => i.Contains("HADOOP")))
            {
                return ConfigTypes.Hadoop;
            }
            if (strDir.Any(i => i.Contains("SOLR")))
            {
                return ConfigTypes.Solr;
            }
            if (strDir.Any(i => i.Contains("SPARK")))
            {
                return ConfigTypes.Spark;
            }
            
            return ConfigTypes.Unkown;
        }

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
