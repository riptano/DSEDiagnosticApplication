using Microsoft.VisualStudio.TestTools.UnitTesting;
using DSEDiagnosticFileParser;
using DSEDiagnosticLibrary;
using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticFileParser.Tests
{
    [TestClass()]
    public class CLogLineTypeParser_Test
    {
        [TestMethod()]
        public void TagLinkTest()
        {
            var masterCollection = LibrarySettings.MasterLog4NetParser();
            var parser = new CLogLineTypeParser() { LinkedTagId = 5.0m };
            var expectedParser = masterCollection.Parsers.First(p => p.TagId == 5.0m);

            Assert.AreEqual(expectedParser.TagId, parser.TagId);
            Assert.AreEqual(expectedParser.Description, parser.Description);
            Assert.AreEqual(expectedParser.LevelMatch, parser.LevelMatch);
            Assert.AreEqual(expectedParser.FileNameMatch, parser.FileNameMatch);
            Assert.AreEqual(expectedParser.MessageMatch, parser.MessageMatch);
            Assert.AreEqual(expectedParser.ParseMessage, parser.ParseMessage);
            Assert.AreEqual(expectedParser.EventType, parser.EventType);
            Assert.AreEqual(expectedParser.EventClass, parser.EventClass);
            Assert.AreEqual(expectedParser.SessionKey, parser.SessionKey);
            Assert.AreEqual(expectedParser.SessionKeyAction, parser.SessionKeyAction);
            Assert.AreEqual(expectedParser.SubClass, parser.SubClass);

            parser = new CLogLineTypeParser() { LinkedTagId = 18.0m };
            expectedParser = masterCollection.Parsers.First(p => p.TagId == 18.0m);

            Assert.AreEqual(expectedParser.TagId, parser.TagId);
            Assert.AreEqual(expectedParser.Description, parser.Description);
            Assert.AreEqual(expectedParser.LevelMatch, parser.LevelMatch);
            Assert.AreEqual(expectedParser.FileNameMatch, parser.FileNameMatch);
            Assert.AreEqual(expectedParser.MessageMatch, parser.MessageMatch);
            Assert.AreEqual(expectedParser.ParseMessage, parser.ParseMessage);
            Assert.AreEqual(expectedParser.EventType, parser.EventType);
            Assert.AreEqual(expectedParser.EventClass, parser.EventClass);
            Assert.AreEqual(expectedParser.SessionKey, parser.SessionKey);
            Assert.AreEqual(expectedParser.SessionKeyAction, parser.SessionKeyAction);
            Assert.AreEqual(expectedParser.SubClass, parser.SubClass);

            parser = new CLogLineTypeParser() { LinkedTagId = 7.0m, Description = "test", TagId = 345.0m };
            expectedParser = masterCollection.Parsers.First(p => p.TagId == 7.0m);

            Assert.AreEqual(345.0m, parser.TagId);
            Assert.AreEqual("test", parser.Description);
            Assert.AreEqual(expectedParser.LevelMatch, parser.LevelMatch);
            Assert.AreEqual(expectedParser.FileNameMatch, parser.FileNameMatch);
            Assert.AreEqual(expectedParser.MessageMatch, parser.MessageMatch);
            Assert.AreEqual(expectedParser.ParseMessage, parser.ParseMessage);
            Assert.AreEqual(expectedParser.EventType, parser.EventType);
            Assert.AreEqual(expectedParser.EventClass, parser.EventClass);
            Assert.AreEqual(expectedParser.SessionKey, parser.SessionKey);
            Assert.AreEqual(expectedParser.SessionKeyAction, parser.SessionKeyAction);
            Assert.AreEqual(expectedParser.SubClass, parser.SubClass);

        }
    }
}
