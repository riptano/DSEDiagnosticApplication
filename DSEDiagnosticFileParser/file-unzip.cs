using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    public sealed class file_unzip : DiagnosticFile
    {
        public file_unzip(CatagoryTypes catagory,
                            IDirectoryPath diagnosticDirectory,
                            IFilePath file,
                            INode node,
                            string defaultClusterName,
                            string defaultDCName)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName)
        {
        }

        public sealed class ExtractionResult : IResult
        {
            internal ExtractionResult(IPath extractionDirectory, int nbrFilesExtracted)
            {
                this.Path = extractionDirectory;
                this.NbrItems = nbrFilesExtracted;
            }

            public IPath Path { get; private set; }
            public Cluster Cluster { get; private set; }
            public IDataCenter DataCenter { get; private set; }
            public INode Node { get; private set; }
            public int NbrItems { get; private set; }

            public IEnumerable<IParsed> Results { get; }
        }
        public ExtractionResult Result { get; private set; }

        public override IResult GetResult()
        {
            return this.Result;
        }

        public override uint ProcessFile()
        {
            IDirectoryPath newDirectory;

            var nbrFilesExtracted =  MiscHelpers.UnZipFileToFolder(this.File, out newDirectory, false, true, true, true, this.CancellationToken);

            this.Result = new ExtractionResult(newDirectory, nbrFilesExtracted);

            return (uint)nbrFilesExtracted;
        }
    }
}
