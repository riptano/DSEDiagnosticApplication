﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    internal sealed class file_ntpstat : DiagnosticFile
    {
        public file_ntpstat(CatagoryTypes catagory, IDirectoryPath diagnosticDirectory, IFilePath file, INode node)
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

            if (!string.IsNullOrEmpty(fileLine))
            {
                //synchronised to NTP server (10.12.13.19) at stratum 2     time correct to within 3 ms   polling server every 16 s
                //10.12.13.19, 2, 3 ms, 16 s
                var splits = this.RegExParser.Split(fileLine);

                if (splits.Length > 0)
                {
                    this.Node.Machine.NTP.NTPServer = MiscHelpers.DetermineIPAddress(splits[0]);
                    this.Node.Machine.NTP.Stratum = int.Parse(splits[1]);
                    this.Node.Machine.NTP.Correction = UnitOfMeasure.Create(splits[2], UnitOfMeasure.Types.Time);
                    this.Node.Machine.NTP.Polling = UnitOfMeasure.Create(splits[3], UnitOfMeasure.Types.Time);
                }

                ++nbrItems;
            }

            return nbrItems;
        }
    }
}
