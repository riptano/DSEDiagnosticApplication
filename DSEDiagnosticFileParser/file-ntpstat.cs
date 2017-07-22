using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class file_ntpstat : DiagnosticFile
    {
        public file_ntpstat(CatagoryTypes catagory,
                                IDirectoryPath diagnosticDirectory,
                                IFilePath file,
                                INode node,
                                string defaultClusterName,
                                string defaultDCName,
                                Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
        }

        public override IResult GetResult()
        {
            return new EmptyResult(this.File, null, null, this.Node);
        }

        public override uint ProcessFile()
        {
            var fileLine = this.File.ReadAllText().Replace('\n', ' ');
            
            if (!string.IsNullOrEmpty(fileLine))
            {
                //synchronised to NTP server (10.12.13.19) at stratum 2     time correct to within 3 ms   polling server every 16 s
                //10.12.13.19, 2, 3 ms, 16 s
                var splits = this.RegExParser.Split(fileLine);

                if (splits.Length > 4)
                {
                    this.Node.Machine.NTP.NTPServer = StringHelpers.DetermineIPAddress(splits[1]);
                    this.Node.Machine.NTP.Stratum = int.Parse(splits[2]);
                    this.Node.Machine.NTP.Correction = UnitOfMeasure.Create(splits[3], UnitOfMeasure.Types.Time);
                    this.Node.Machine.NTP.Polling = UnitOfMeasure.Create(splits[4], UnitOfMeasure.Types.Time);
                }

                ++this.NbrItemsParsed;
            }

            this.Processed = true;
            return 0;
        }
    }
}
