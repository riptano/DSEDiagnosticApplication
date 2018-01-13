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
    public sealed class file_unzip : DiagnosticFile
    {
        public file_unzip(CatagoryTypes catagory,
                            IDirectoryPath diagnosticDirectory,
                            IFilePath file,
                            INode node,
                            string defaultClusterName,
                            string defaultDCName,
                            Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
        }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class ExtractionResult : IResult
        {
            internal ExtractionResult(IPath extractionDirectory, int nbrFilesExtracted, bool extractionFileReNamed)
            {
                this.Path = extractionDirectory;
                this.NbrItems = nbrFilesExtracted;
                this.ExtractionFileReNamed = extractionFileReNamed;
            }

            [JsonConverter(typeof(DSEDiagnosticLibrary.IPathJsonConverter))]
            public IPath Path { get; }
            public Cluster Cluster { get; }
            public IDataCenter DataCenter { get; }
            public INode Node { get; }
            public int NbrItems { get; }

            /// <summary>
            /// If true the extraction file was renamed such that &quot;extracted&quot; was appended to the end of the orginal file name.
            /// </summary>
            public bool ExtractionFileReNamed { get; }

            public IEnumerable<IParsed> Results { get; }
        }

        [JsonProperty(PropertyName="Results")]
        private ExtractionResult _result;

        public override IResult GetResult()
        {
            return this._result;
        }

        public override uint ProcessFile()
        {
            IDirectoryPath newDirectory = null;
            int nbrFilesExtracted = 0;
            //If file name has an IP address or host name embedded, the extractor will create a direction based on this file name. Otherwise just extract into the file's parent directory.
            var extractToParentFolder = !NodeIdentifier.DetermineIfNameHasIPAddressOrHostName(this.File.FileNameWithoutExtension);
            

            nbrFilesExtracted = MiscHelpers.UnZipFileToFolder(this.File, out newDirectory, false, extractToParentFolder, true, true, this.CancellationToken, false);
            
            this._result = new ExtractionResult(newDirectory, nbrFilesExtracted, nbrFilesExtracted > 0);

            this.NbrItemsParsed = 1;
            this.Processed = nbrFilesExtracted > 0;
            return (uint)nbrFilesExtracted;
        }
    }
}
