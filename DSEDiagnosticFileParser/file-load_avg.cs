using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    internal sealed class file_load_avg : DiagnosticFile
    {
        public file_load_avg(CatagoryTypes catagory,
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
            
            if(!string.IsNullOrEmpty(fileLine))
            {
                this.Node.Machine.CPULoad.Average = UnitOfMeasure.Create(fileLine, UnitOfMeasure.Types.Percent | UnitOfMeasure.Types.Utilization);
                ++this.NbrItemsParsed;
            }

            this.Processed = true;
            return 0;
        }
    }
}
