using Microsoft.VisualStudio.TestTools.UnitTesting;
using ICSharpCode.SharpZipLib.Tar;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace ICSharpCode.SharpZipLib.Tar.Tests
{
    [TestClass()]
    public class TarArchiveTests
    {
        [TestMethod()]
        public void ExtractContentsTest()
        {
            using (FileStream stream = new FileStream(@"E:\Libraries\Projects\DataStax\Diag-Customer\CapOne\10.186.71.169.tar", FileMode.Open, FileAccess.Read))
            using (var gzipStream = new ICSharpCode.SharpZipLib.GZip.GZipInputStream(stream))
            using (var tarArchive = ICSharpCode.SharpZipLib.Tar.TarArchive.CreateInputTarArchive(gzipStream))
            {
                tarArchive.FileOverwriteOption = TarArchive.FileOverWriteOptions.Always;
                tarArchive.RestoreDateTimeOnExtract = true;
                tarArchive.ExtractContents(@"E:\Libraries\Projects\DataStax\Diag-Customer\CapOne\");
            }
        }
    }
}