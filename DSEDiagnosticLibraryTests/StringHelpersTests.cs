using Microsoft.VisualStudio.TestTools.UnitTesting;
using DSEDiagnosticLibrary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticLibrary.Tests
{
    [TestClass()]
    public class StringHelpersTests
    {
        [TestMethod()]
        public void DetermineProperObjectFormatTest()
        {
            object expected = System.Net.IPAddress.Parse("10.11.12.14");
            var actual = StringHelpers.DetermineProperObjectFormat("10.11.12.14", false, true, true, null, true);

            Assert.AreEqual(expected, actual);

            actual = StringHelpers.DetermineProperObjectFormat("/10.11.12.14", false, true, true, null, true);

            Assert.AreEqual(expected, actual);
           
            expected = System.DateTime.Parse(@"12/25/1991 13:50:25.34");
            actual = StringHelpers.DetermineProperObjectFormat(@"12/25/1991 13:50:25.34", false, true, true, null, true);

            Assert.AreEqual(expected, actual);

            expected = System.DateTime.Parse(@"1991-12-25 13:50:25.34");
            actual = StringHelpers.DetermineProperObjectFormat(@"1991-12-25 13:50:25,34", false, true, true, null, true);

            Assert.AreEqual(expected, actual);
            
            expected = System.DateTime.Parse(@"2017-02-11 00:00:30,042");
            actual = StringHelpers.DetermineProperObjectFormat(@"2017-02-11 00:00:30,042", false, true, true, null, true);

            Assert.AreEqual(expected, actual);

            expected = 123.45;
            actual = StringHelpers.DetermineProperObjectFormat(@"123.45", false, true, true, null, true);

            Assert.AreEqual(expected, actual);

            expected = -123.45;
            actual = StringHelpers.DetermineProperObjectFormat(@"-123.45", false, true, true, null, true);

            Assert.AreEqual(expected, actual);

        }
    }
}