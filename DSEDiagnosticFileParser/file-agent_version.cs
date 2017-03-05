using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    internal sealed class file_agent_version : DiagnosticFile
    {
        public file_agent_version(CatagoryTypes catagory, 
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node,
                                    string defaultClusterName,
                                    string defaultDCName)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName)
        {
        }

        public override IResult GetResult()
        {
            throw new NotImplementedException();
        }

        public override uint ProcessFile()
        {
            var fileLine = this.File.ReadAllText();
            uint nbrItems = 0;

            if (!string.IsNullOrEmpty(fileLine))
            {
                this.Node.DSE.Versions.OpsCenterAgent = new Version(fileLine);
                ++nbrItems;
            }

            return nbrItems;
        }
    }
}
