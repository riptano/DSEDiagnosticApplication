using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public class file_DSE_env : file_yaml
    {
        public file_DSE_env(CatagoryTypes catagory,
                                IDirectoryPath diagnosticDirectory,
                                IFilePath file,
                                INode node,
                                string defaultClusterName,
                                string defaultDCName,
                                Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this.SplitLineDelimiter = '=';            
        }

        [JsonIgnore]
        public override SourceTypes Source { get { return SourceTypes.EnvFile; } }

        private int ifCount = 0;
        private string ifString = string.Empty;

        /// <summary>
        /// if [ -d /usr/share/dse ]; then export DSE_HOME =/ usr / share / dse fi
        /// </summary>
        /// <param name="line"></param>
        /// <returns></returns>
        protected override bool PreSplitLine(string line, List<Tuple<string, string>> cmdPathValueList)
        {
            bool continueProcessing = true;

            line = line.Trim();

            if (line.StartsWith("if", StringComparison.OrdinalIgnoreCase))
            {
                ++ifCount;
            }

            if(ifCount > 0)
            {
                ifString += ' ' + line;

                if (line.EndsWith("fi", StringComparison.OrdinalIgnoreCase))
                {
                    --ifCount;

                    if (ifCount == 0)
                    {
                        cmdPathValueList.Add(GenerateCmdValuePair(string.Format("Expression[{0}]", this.NbrItemsParsed),
                                                                        ifString));
                        ifString = string.Empty;
                    }
                }
                continueProcessing = false;
            }

            return continueProcessing;
        }

        protected override void SetNodeAttribuesFromConfig(Tuple<string, string> propvaluePair)
        {
            switch (propvaluePair.Item1)
            {
                case "HADOOP_ENABLED":
                    if (propvaluePair.Item2 == "1")
                    {
                        this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.Hadoop;
                    }
                    break;
                case "GRAPH_ENABLED":
                    if (propvaluePair.Item2 == "1")
                    {
                        this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.Graph;
                    }
                    break;
                case "SOLR_ENABLED":
                    if (propvaluePair.Item2 == "1")
                    {
                        this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.Search;
                    }
                    break;
                case "SPARK_ENABLED":
                    if (propvaluePair.Item2 == "1")
                    {
                        this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.Analytics;
                    }
                    break;
                case "CFS_ENABLED":
                    if (propvaluePair.Item2 == "1")
                    {
                        this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.CFS;
                    }
                    break;
                default:
                    break;
            }

        }

    }
}
