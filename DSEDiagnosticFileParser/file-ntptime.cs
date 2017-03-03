using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    internal sealed class file_ntptime : DiagnosticFile
    {
        public file_ntptime(CatagoryTypes catagory, IDirectoryPath diagnosticDirectory, IFilePath file, INode node)
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
                //ntp_gettime() returns code 0 (OK)  time dbf817f7.8ecde16c  Sun, Dec 11 2016 19:22:47.557, (.557829621),  maximum error 11481 us, estimated error 20 us, TAI offset 36 ntp_adjtime() returns code 0 (OK)  modes 0x0 (),  offset -13.848 us, frequency 5.088 ppm, interval 1 s,  maximum error 11481 us, estimated error 20 us,  status 0x2001 (PLL,NANO),  time constant 4, precision 0.001 us, tolerance 500 ppm,
                //for ntp_adjtime split (0) -- 5.088 ppm, 1 s, 11481 us, 20 us, 4, 0.001 us, 500 ppm
                //for ntp_gettime split (1) -- 11481 us, 20 us
                var splits = this.RegExParser.Split(fileLine, 0);

                if (splits.Length > 0)
                {
                    this.Node.Machine.NTP.Frequency = UnitOfMeasure.Create(splits[0], UnitOfMeasure.Types.Time);
                    //No NTP interval field
                    this.Node.Machine.NTP.MaximumError = UnitOfMeasure.Create(splits[2], UnitOfMeasure.Types.Time);
                    this.Node.Machine.NTP.EstimatedError = UnitOfMeasure.Create(splits[3], UnitOfMeasure.Types.Time);
                    //No NTP time constant field
                    this.Node.Machine.NTP.Precision = UnitOfMeasure.Create(splits[5], UnitOfMeasure.Types.Time);
                    this.Node.Machine.NTP.Tolerance = UnitOfMeasure.Create(splits[6], UnitOfMeasure.Types.Time);
                    ++nbrItems;
                }
                else
                {
                    splits = this.RegExParser.Split(fileLine, 1);

                    if (splits.Length > 0)
                    {
                        this.Node.Machine.NTP.MaximumError = UnitOfMeasure.Create(splits[0], UnitOfMeasure.Types.Time);
                        this.Node.Machine.NTP.EstimatedError = UnitOfMeasure.Create(splits[1], UnitOfMeasure.Types.Time);
                        ++nbrItems;
                    }
                }
            }

            return nbrItems;
        }
    }
}
