using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    internal class file_yaml : DiagnosticFile
    {
        public file_yaml(CatagoryTypes catagory,
                                IDirectoryPath diagnosticDirectory,
                                IFilePath file,
                                INode node,
                                string defaultClusterName,
                                string defaultDCName)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName)
        {
            this._result = new Result(this);
        }

        public sealed class Result : IResult
        {
            private Result() { }
            internal Result(file_yaml yamlFileInstance)
            {
                this.Path = yamlFileInstance.File;
                this.Node = yamlFileInstance.Node;
            }

            internal List<YamlConfigurationLine> ConfigLines = new List<YamlConfigurationLine>();

            #region IResult
            public IPath Path { get; private set; }
            public Cluster Cluster { get { return this.DataCenter?.Cluster; } }
            public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
            public INode Node { get; private set; }
            public int NbrItems { get { return this.ConfigLines.Count; } }

            public IEnumerable<IParsed> Results { get { return this.ConfigLines; } }

            #endregion
        }

        private Result _result;

        public override IResult GetResult()
        {
            return this._result;
        }

        private Tuple<string,string> GenerateCmdValuePair(string cmd, string value)
        {
            if(!string.IsNullOrEmpty(value)
                && LibrarySettings.ObscureFiledValues.Any(c => cmd.ToUpper().Contains(c.ToUpper())))
            {
                value = new string('*', value.Length);
            }

            return new Tuple<string, string>(cmd, value);
        }

        public override uint ProcessFile()
        {
            var fileLines = this.File.ReadAllLines();
            string line = null;
            int nPos = -1;
            int nLineLevel = 0;
            var nestedCmdLevels = new List<Tuple<string,int>>();
            int cmdListPos = 0;
            string[] lineSplit = null;
            string lastValue = null;
            bool listCmdElement = false;

            List<Tuple<string,string>> cmdPathValueList = new List<Tuple<string, string>>();

            foreach (var fileLine in fileLines)
            {
                ++this.NbrItemsParsed;

                line = fileLine.TrimStart();

                if(string.IsNullOrEmpty(line)
                    || line[0] == '#')
                {
                    continue;
                }

                line = fileLine.Replace('\t', ' ').TrimEnd();

                nPos = line.IndexOf('#');

                if(nPos >= 0)
                {
                    line = line.Substring(0, nPos).TrimEnd();
                }

                nLineLevel = line.TakeWhile(Char.IsWhiteSpace).Count();
                line = line.TrimStart();

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
                if (line[0] == '-')
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

                lineSplit = line.Split(':');

                //If there are more than 2 splits, it is possibly due to splitting within a delimiter. So let's use an advance more expense form of split.
                if(lineSplit.Length > 2)
                {
                    lineSplit = StringFunctions.Split(line,
                                                        ':',
                                                        StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                        StringFunctions.SplitBehaviorOptions.IgnoreMismatchedWithinDelimiters | StringFunctions.SplitBehaviorOptions.StringTrimEachElement)
                                                    .ToArray();
                }

                if(lineSplit.Length == 1 || string.IsNullOrEmpty(lineSplit[1]))
                {
                    if (listCmdElement)
                    {
                        cmdPathValueList.Add(GenerateCmdValuePair(string.Format("{0}[{1}]",
                                                                                string.Join(".", nestedCmdLevels.Select(i => i.Item1)),
                                                                                cmdListPos),
                                                                        lineSplit[0]));
                    }
                    else
                    {
                        nestedCmdLevels.Add(new Tuple<string,int>(lineSplit[0].Trim(),nLineLevel));
                    }

                    lastValue = lineSplit.Length == 1 ? null : lineSplit[1];

                    continue;
                }

                //lineSplit is more than two elements... assume command/value pair....
                System.Diagnostics.Debug.Assert(lineSplit.Length == 2);

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
                                                                        lineSplit[0]),
                                                            lineSplit[1]));
                lastValue = null;
            }

            this.ProcessPropertyValuePairs(cmdPathValueList);

            this.Processed = true;
            return (uint) this._result.ConfigLines.Count;
        }

        private void ProcessPropertyValuePairs(IEnumerable<Tuple<string, string>> propvaluePairs)
        {
            uint nPos = 0;

            foreach (var propvaluePair in propvaluePairs)
            {
                switch (propvaluePair.Item1.ToLower())
                {
                    case "cluster_name":
                        Cluster.NameClusterAssociatewItem(this.Node, propvaluePair.Item2);
                        break;
                    case "num_tokens":
                        if(!this.Node.DSE.NbrTokens.HasValue)
                        {
                            uint nbrTokens;
                            if(uint.TryParse(propvaluePair.Item2, out nbrTokens))
                            {
                                this.Node.DSE.NbrTokens = nbrTokens;
                                this.Node.DSE.VNodesEnabled = nbrTokens > 1;
                            }
                        }
                        break;
                    default:
                        break;
                }

                this._result.ConfigLines.Add(new YamlConfigurationLine(this.File, this.Node, ++nPos, propvaluePair.Item1, propvaluePair.Item2));
            }
        }
    }
}
