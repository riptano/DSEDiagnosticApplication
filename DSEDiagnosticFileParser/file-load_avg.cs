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
        public file_load_avg(CatagoryTypes catagory, IDirectoryPath diagnosticDirectory, IFilePath file, INode node)
            : base(catagory, diagnosticDirectory, file, node)
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

            if(!string.IsNullOrEmpty(fileLine))
            {
                this.Node.Machine.CPULoad.Average = UnitOfMeasure.Create(fileLine, UnitOfMeasure.Types.Percent | UnitOfMeasure.Types.Utilization);
                ++nbrItems;
            }

            return nbrItems;
        }
    }
}
