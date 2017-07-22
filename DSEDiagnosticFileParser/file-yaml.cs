using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public class file_yaml : DiagnosticFile
    {
        public file_yaml(CatagoryTypes catagory,
                                IDirectoryPath diagnosticDirectory,
                                IFilePath file,
                                INode node,
                                string defaultClusterName,
                                string defaultDCName,
                                Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this._result = new YamlResult(this);
            this.ConfigType = YamlConfigurationLine.ConfigTypeMapper.FindConfigType(file);
        }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class YamlResult : IResult
        {
            private YamlResult() { }
            internal YamlResult(file_yaml yamlFileInstance)
            {
                this.Path = yamlFileInstance.File;
                this.Node = yamlFileInstance.Node;
            }

            [JsonProperty]
            internal List<IConfigurationLine> ConfigLines = new List<IConfigurationLine>();

            #region IResult
            [JsonConverter(typeof(DSEDiagnosticLibrary.IPathJsonConverter))]
            public IPath Path { get; private set; }
            [JsonIgnore]
            public Cluster Cluster { get { return this.DataCenter?.Cluster; } }
            [JsonIgnore]
            public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
            public INode Node { get; private set; }
            [JsonIgnore]
            public int NbrItems { get { return this.ConfigLines.Count; } }
            [JsonIgnore]
            public IEnumerable<IParsed> Results { get { return this.ConfigLines; } }

            #endregion
        }

        [JsonProperty(PropertyName="Results")]
        protected YamlResult _result;

        public override IResult GetResult()
        {
            return this._result;
        }

        protected char SplitLineDelimiter = ':';
        protected char CommentDelimiter = '#';
        protected char ContinuousDelimiter = '-';

        public ConfigTypes ConfigType { get; protected set; }
        public virtual SourceTypes Source { get { return SourceTypes.Yaml; } }

        virtual protected string[] SplitLine(string line)
        {
            string[] lineSplit;

            lineSplit = line.Split(SplitLineDelimiter);

            //If there are more than 2 splits, it is possibly due to splitting within a delimiter. So let's use an advance more expense form of split.
            if (lineSplit.Length > 2)
            {
                lineSplit = StringFunctions.Split(line,
                                                    SplitLineDelimiter,
                                                    StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                    StringFunctions.SplitBehaviorOptions.IgnoreMismatchedWithinDelimiters)
                                                .ToArray();

                //Still have more than 2, maybe colon has been escaped
                //fe80\:0\:0\:0\:202\:b3ff\:fe1e\:8329=DC1:RAC3
                if (lineSplit.Length > 2 && lineSplit.Any(i => i.Last() == '\\'))
                {
                    int fndPos = -1;

                    for(int nIdx = 0; nIdx < lineSplit.Length; ++nIdx)
                    {
                        if(lineSplit[nIdx].Last() == '\\')
                        {
                            if(fndPos == -1)
                            {
                                lineSplit[nIdx] += SplitLineDelimiter;
                                fndPos = nIdx;
                            }
                            else
                            {
                                lineSplit[fndPos] += lineSplit[nIdx] + SplitLineDelimiter;
                                lineSplit[nIdx] = null;
                            }
                        }
                        else if(fndPos >= 0)
                        {
                            lineSplit[fndPos] += lineSplit[nIdx];
                            lineSplit[fndPos] = lineSplit[fndPos].Replace("\\", string.Empty);
                            lineSplit[nIdx] = null;
                            fndPos = -1;
                        }
                    }

                    lineSplit = lineSplit.Where(i => i != null).ToArray();
                }
            }

            return lineSplit;
        }

        protected Tuple<string,string> GenerateCmdValuePair(string cmd, string value)
        {
            if(!string.IsNullOrEmpty(value)
                && LibrarySettings.ObscureFiledValues.Any(c => cmd.ToUpper().Contains(c.ToUpper())))
            {
                value = new string('*', value.Length);
            }

            return new Tuple<string, string>(cmd, value);
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="line"></param>
        /// <param name="cmdPathValueList"></param>
        /// <returns>True to continue processing</returns>
        virtual protected bool PreSplitLine(string line, List<Tuple<string, string>> cmdPathValueList)
        {
            return true;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="line"></param>
        /// <param name="splits"></param>
        /// <param name="cmdPathValueList"></param>
        /// <returns>True to continue processing</returns>
        virtual protected bool PostSplitLine(string line, string[] splits, List<Tuple<string, string>> cmdPathValueList)
        {
            return true;
        }

        public override uint ProcessFile()
        {
            if(this.ConfigType == ConfigTypes.Hadoop)
            {
                if(!this.Node.DSE.InstanceType.HasFlag(DSEInfo.InstanceTypes.Hadoop))
                {
                    this.Processed = false;
                    this.NbrItemsParsed = 0;
                    return 0;
                }
            }
            else if (this.ConfigType == ConfigTypes.Solr)
            {
                if (!this.Node.DSE.InstanceType.HasFlag(DSEInfo.InstanceTypes.Search))
                {
                    this.Processed = false;
                    this.NbrItemsParsed = 0;
                    return 0;
                }
            }
            else if (this.ConfigType == ConfigTypes.Spark)
            {
                if (!this.Node.DSE.InstanceType.HasFlag(DSEInfo.InstanceTypes.Analytics))
                {
                    this.Processed = false;
                    this.NbrItemsParsed = 0;
                    return 0;
                }
            }
            else if (this.ConfigType == ConfigTypes.Snitch && this.Node.DSE.EndpointSnitch != null)
            {
                var snitchFiles = LibrarySettings.SnitchFileMappings.TryGetValue(this.Node.DSE.EndpointSnitch);
                var snitchFileSplit = snitchFiles?.Split(',');
                var processThisFilePos = snitchFileSplit == null
                                            ? -1
                                            : snitchFileSplit.IndexOf(f => f.Trim().ToLower() == this.File.FileName.ToLower());
                var processThisFile = processThisFilePos >= 0;

                if (processThisFile
                        && snitchFileSplit.Length > 1
                        && processThisFilePos + 1 < snitchFileSplit.Length)
                {
                    IFilePath checkSnitchFile;
                    foreach (var snitchFile in snitchFileSplit.GetRange(processThisFilePos + 1))
                    {
                        if(this.File.ParentDirectoryPath.MakeFile(snitchFile, out checkSnitchFile)
                                && checkSnitchFile.Exist())
                        {
                            processThisFile = false;
                            break;
                        }
                    }
                }

                if (!processThisFile)
                {
                    this.Processed = false;
                    this.NbrItemsParsed = 0;
                    return 0;
                }
            }
            else if(this.ConfigType == ConfigTypes.Unkown)
            {
                Logger.Instance.WarnFormat("{0}\t{1}\tUnknown configuration type for this file. File will be ignored.",
                                                this.Node.Id,
                                                this.File);
                this.Processed = false;
                this.NbrItemsParsed = 0;
                return 0;
            }

            var fileLines = this.File.ReadAllLines();
            int fileLineIdx = 0;
            string line = null;
            int nPos = -1;
            int nLineLevel = 0;
            var nestedCmdLevels = new List<Tuple<string,int>>();
            int cmdListPos = 0;
            string[] lineSplit = null;
            string lastValue = null;
            bool listCmdElement = false;
            StringBuilder multipleLine = null;
            string[] multipleLineItems = null;
            int multipleLineItemPos = 0;

            List<Tuple<string,string>> cmdPathValueList = new List<Tuple<string, string>>();

            for(;fileLineIdx < fileLines.Length;)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                ++this.NbrItemsParsed;

                if (multipleLineItems == null)
                {
                    line = fileLines[fileLineIdx].TrimStart();

                    if (string.IsNullOrEmpty(line)
                        || line[0] == CommentDelimiter)
                    {
                        ++fileLineIdx;
                        continue;
                    }

                    line = fileLines[fileLineIdx].Replace('\t', ' ').TrimEnd();
                    ++fileLineIdx;
                }
                else if(multipleLine == null)
                {
                    line = multipleLineItems[multipleLineItemPos++].TrimEnd();


                    if (nestedCmdLevels.Count > 0)
                    {
                        line = new string(' ', nestedCmdLevels.Last().Item2 + 1) + line;
                    }

                    if (multipleLineItemPos >= multipleLineItems.Length)
                    {
                        multipleLineItemPos = 0;
                        multipleLineItems = null;
                    }
                }

                nPos = line.IndexOf(CommentDelimiter);

                if(nPos >= 0)
                {
                    line = line.Substring(0, nPos).TrimEnd();
                }

                nLineLevel = line.TakeWhile(Char.IsWhiteSpace).Count();
                line = line.TrimStart();

                if(multipleLine == null
                        && line.Last() == '}'
                        && line.IndexOf('{') > 0)
                {
                    if (nLineLevel > 0)
                    {
                        multipleLine = new StringBuilder(new string(' ', nLineLevel));
                    }
                    else
                    {
                        multipleLine = new StringBuilder();
                    }
                }

                //Multiple Lines
                if(multipleLine != null)
                {
                    if (line.Last() == '}')
                    {
                        multipleLine.Append(line);
                        line = multipleLine.ToString();
                        multipleLine = null;

                        var multipleLineCmdPos = line.ToString().IndexOf(':');
                        var braceFirst = line.IndexOf('{');
                        var braceLast = line.LastIndexOf('}');

                        multipleLineItems = StringFunctions.Split(line.Substring(0, braceFirst) + ' ' + line.Substring(braceFirst + 1, line.Length - braceFirst - 2),
                                                                        ',',
                                                                        StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                                        StringFunctions.SplitBehaviorOptions.IgnoreMismatchedWithinDelimiters)
                                                                    .ToArray();
                        multipleLineItemPos = 0;

                        if (multipleLineCmdPos < braceFirst)
                        {
                            line = multipleLineItems[0].Substring(0, multipleLineCmdPos + 1).TrimEnd();
                            multipleLineItems[0] = multipleLineItems[0].Substring(multipleLineCmdPos + 1).Trim();
                            nLineLevel = line.TakeWhile(Char.IsWhiteSpace).Count();
                            line = line.TrimStart();
                        }
                        else
                        {
                            continue;
                        }
                    }
                    else if (line.Last() == ',')
                    {
                        multipleLine.Append(line);
                    }
                    else
                    {
                        Logger.Instance.ErrorFormat("{0}\t{1}\tInvalid configuration line \"{2}\" at line position {3}.", this.Node.Id, this.File, multipleLine, this.NbrItemsParsed);
                        multipleLine = null;
                    }
                }
                else if (line.Last() == ',')
                {
                    var bracePos = line.LastIndexOf('{');

                    if (bracePos >= 0
                            && line.IndexOf('}', bracePos) < 0)
                    {
                        if (nLineLevel > 0)
                        {
                            multipleLine = new StringBuilder(new string(' ', nLineLevel));
                        }
                        else
                        {
                            multipleLine = new StringBuilder();
                        }

                        multipleLine.Append(line);
                        continue;
                    }
                }

                if (nestedCmdLevels.Count > 0
                        && nLineLevel <= nestedCmdLevels.Last().Item2)
                {
                    if (lastValue != null)
                    {
                        cmdPathValueList.Add(GenerateCmdValuePair(string.Join(".", nestedCmdLevels.Select(i => i.Item1)),
                                                                    lastValue));
                        lastValue = null;
                    }

                    cmdListPos = 0;

                    for (; nestedCmdLevels.Last().Item2 >= nLineLevel;)
                    {
                        nestedCmdLevels.RemoveAt(nestedCmdLevels.Count - 1);

                        if (nestedCmdLevels.Count == 0) break;
                    }
                }

                //List Item
                if (line[0] == ContinuousDelimiter)
                {
                    ++cmdListPos;
                    listCmdElement = true;
                    line = line.Substring(1).TrimStart();
                }
                else
                {
                    cmdListPos = 0;
                    listCmdElement = false;
                }

                if(!this.PreSplitLine(line, cmdPathValueList))
                {
                    continue;
                }

                lineSplit = SplitLine(line);

                if (!this.PostSplitLine(line, lineSplit, cmdPathValueList))
                {
                    continue;
                }

                if (lineSplit.Length == 1 || string.IsNullOrEmpty(lineSplit[1]))
                {
                    if (listCmdElement)
                    {
                        cmdPathValueList.Add(GenerateCmdValuePair(string.Format("{0}[{1}]",
                                                                                string.Join(".", nestedCmdLevels.Select(i => i.Item1)),
                                                                                cmdListPos),
                                                                        lineSplit[0].Trim()));
                    }
                    else
                    {
                        nestedCmdLevels.Add(new Tuple<string,int>(lineSplit[0].Trim(),nLineLevel));
                    }

                    lastValue = lineSplit.Length == 1 ? null : lineSplit[1].Trim();

                    continue;
                }

                //lineSplit is more than two elements... assume command/value pair....
                if(lineSplit.Length > 2)
                {
                    Logger.Instance.ErrorFormat("{0}\t{1}\tInvalid configuration line \"{2}\" at line position {3}.", this.Node.Id, this.File, line, this.NbrItemsParsed);
                    continue;
                }

                if (listCmdElement && nestedCmdLevels.Count > 0)
                {
                    nestedCmdLevels[nestedCmdLevels.Count - 1] = new Tuple<string, int>(string.Format("{0}[{1}]",
                                                                                                        nestedCmdLevels.Last().Item1,
                                                                                                        cmdListPos),
                                                                                        nestedCmdLevels.Last().Item2);
                }

                cmdPathValueList.Add(GenerateCmdValuePair(string.Format("{0}{1}{2}",
                                                                        string.Join(".", nestedCmdLevels.Select(i => i.Item1)),
                                                                        nestedCmdLevels.Count == 0 ? string.Empty : ".",
                                                                        lineSplit[0].Trim()),
                                                            lineSplit[1].Trim()));
                lastValue = null;
            }

            this.ProcessPropertyValuePairs(cmdPathValueList);

            this.Processed = true;
            return (uint) this._result.ConfigLines.Count;
        }

        protected virtual void SetNodeAttribuesFromConfig(Tuple<string, string> propvaluePair)
        {
            switch (propvaluePair.Item1.ToLower())
            {
                case "cluster_name":
                    Cluster.NameClusterAssociatewItem(this.Node, propvaluePair.Item2);
                    break;
                case "num_tokens":
                    if (!this.Node.DSE.NbrTokens.HasValue)
                    {
                        uint nbrTokens;
                        if (uint.TryParse(propvaluePair.Item2, out nbrTokens))
                        {
                            this.Node.DSE.NbrTokens = nbrTokens;
                            this.Node.DSE.VNodesEnabled = nbrTokens > 1;
                        }
                    }
                    break;
                case "hints_directory":
                    this.Node.DSE.Locations.HintsDir = propvaluePair.Item2;
                    break;
                case "commitlog_directory":
                    this.Node.DSE.Locations.CommitLogDir = propvaluePair.Item2;
                    break;
                case "saved_caches_directory":
                    this.Node.DSE.Locations.SavedCacheDir = propvaluePair.Item2;
                    break;
                case "cassandra-conf":
                    this.Node.DSE.Locations.CassandraYamlFile = propvaluePair.Item2;
                    break;
                case "endpoint_snitch":
                    this.Node.DSE.EndpointSnitch = propvaluePair.Item2;
                    break;
                case "advanced_replication_options.enabled":
                    if (propvaluePair.Item2.ToLower() == "true" || propvaluePair.Item2 == "1")
                    {
                        this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.AdvancedReplication;
                    }
                    break;
                case "hadoop_enabled":
                    if (propvaluePair.Item2 == "1")
                    {
                        this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.Hadoop;
                    }
                    break;
                case "graph_enabled":
                    if (propvaluePair.Item2 == "1")
                    {
                        this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.Graph;
                    }
                    break;
                case "solr_enabled":
                    if (propvaluePair.Item2 == "1")
                    {
                        this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.Search;
                    }
                    break;
                case "spark_enabled":
                    if (propvaluePair.Item2 == "1")
                    {
                        this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.Analytics;
                    }
                    break;
                case "cfs_enabled":
                    if (propvaluePair.Item2 == "1")
                    {
                        this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.CFS;
                    }
                    break;
                case "partitioner":
                    this.Node.DSE.Partitioner = propvaluePair.Item2;
                    break;
                case "server_id":
                    this.Node.DSE.PhysicalServerId = propvaluePair.Item2;
                    break;
                default:
                    break;
            }

            //seed_provider[1].parameters[1].seeds: "10.14.148.34,10.14.148.51"
            if (propvaluePair.Item1.StartsWith("seed_provider[", true, System.Globalization.CultureInfo.CurrentCulture)
                    && propvaluePair.Item1.EndsWith("seeds", true, System.Globalization.CultureInfo.CurrentCulture)
                    && !string.IsNullOrEmpty(propvaluePair.Item2))
            {
                this.Node.DSE.IsSeedNode = StringHelpers.RemoveQuotes(propvaluePair.Item2, false)
                                                .Split(',')
                                                .Any(s => this.Node.Id.Equals(s));
            }
        }

        protected virtual void ProcessPropertyValuePairs(IEnumerable<Tuple<string, string>> propvaluePairs)
        {
            uint nPos = 0;
            var datafiledirectories = new List<string>();

            foreach (var propvaluePair in propvaluePairs)
            {
                this.SetNodeAttribuesFromConfig(propvaluePair);

                //data_file_directories[1]
                if (propvaluePair.Item1.StartsWith("data_file_directories["))
                {
                    datafiledirectories.Add(propvaluePair.Item2);
                }

                this._result.ConfigLines.Add(YamlConfigurationLine.AddConfiguration(this.File,
                                                                                        this.Node,
                                                                                        ++nPos,
                                                                                        propvaluePair.Item1,
                                                                                        propvaluePair.Item2,
                                                                                        this.ConfigType,
                                                                                        this.Source));
            }

            if(datafiledirectories.Count > 0)
            {
                this.Node.DSE.Locations.DataDirs = datafiledirectories;
            }
        }
    }
}
