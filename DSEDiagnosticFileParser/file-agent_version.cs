using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    public sealed class file_agent_version : DiagnosticFile
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
            
            if (!string.IsNullOrEmpty(fileLine))
            {
                this.Node.DSE.Versions.OpsCenterAgent = new Version(fileLine);
                ++this.NbrItemsParsed;
            }

            this.Processed = true;
            return 0;
        }
    }
}
