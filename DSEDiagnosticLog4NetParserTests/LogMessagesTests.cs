using Microsoft.VisualStudio.TestTools.UnitTesting;
using DSEDiagnosticLog4NetParser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Path;


namespace DSEDiagnosticLog4NetParser.Tests
{
    [TestClass()]
    public class LogMessagesTests
    {
        [TestMethod()]
        public void AddMessageTest()
        {
            var l = "%-5level [%thread] %date{ISO8601} %F:%L - %msg%n";
            var filePath = Common.Path.PathUtils.BuildFilePath(@"c:\test\test.log");
            var logLines = new string[]
            {
    @"INFO  [Repair#26067:1] 2016-12-07 15:02:05,817  RepairJob.java:172 - [repair #bc579ef0-bc85-11e6-a73a-73d4260c7cfe] Requesting merkle trees for accountnumbertoprivateaccountidreadmodel (to [/10.14.149.10, /10.14.149.8, /10.14.149.182, /10.14.148.51, /10.14.149.19, /10.14.148.34])",
    @"WARN  [SharedPool-Worker-4] 2016-12-09 09:35:15,144  DseAuthenticator.java:411 - Plain text authentication without client / server encryption is strongly discouraged",
    null,
    @"ERROR [SharedPool-Worker-9] 2016-12-09 10:20:41,416  Message.java:617 - Unexpected exception during request; channel = [id: 0xc37b0c1e, /10.14.148.224:61446 => /10.14.148.34:9042]",
    @"com.google.common.util.concurrent.UncheckedExecutionException: java.lang.RuntimeException: org.apache.cassandra.exceptions.UnavailableException: Cannot achieve consistency level LOCAL_ONE",
    "",
    "                    ",
    @"	at com.google.common.cache.LocalCache$Segment.get(LocalCache.java:2203) ~[guava-18.0.jar:na]",
    @"	at com.google.common.cache.LocalCache.get(LocalCache.java:3937) ~[guava-18.0.jar:na]",
    @"Caused by: java.lang.RuntimeException: org.apache.cassandra.exceptions.UnavailableException: Cannot achieve consistency level LOCAL_ONE",
    @"WARN  [SharedPool-Worker-3] 2016-12-09 10:20:41,300  DseAuthenticator.java:411 - Plain text authentication without client / server encryption is strongly discouraged",
    @"WARN  [SharedPool-Worker-3] 2016-12-09 10:20:41,478  warn.java:411 - warning test message"
            };

            var logMessages = new LogMessages(filePath, l);
            uint cnt = 0;

            foreach (var element in logLines)
            {
                logMessages.AddMessage(element, ++cnt);
            }

            Assert.AreEqual(5, logMessages.Messages.Count());
            Assert.AreEqual(new DateTime(2016, 12, 9, 10, 20, 41, 478), logMessages.LogTimeRange.Max.DateTime);
            Assert.AreEqual(new DateTime(2016, 12, 7, 15, 02, 05, 817), logMessages.LogTimeRange.Min.DateTime);
            Assert.AreEqual(0, logMessages.Messages.ElementAt(0).ExtraMessages.Count());
            Assert.AreEqual(4, logMessages.Messages.ElementAt(2).ExtraMessages.Count());
            Assert.AreEqual(@"Unexpected exception during request; channel = [id: 0xc37b0c1e, /10.14.148.224:61446 => /10.14.148.34:9042]",
                                logMessages.Messages.ElementAt(2).Message);
            Assert.AreEqual(@"at com.google.common.cache.LocalCache$Segment.get(LocalCache.java:2203) ~[guava-18.0.jar:na]",
                               logMessages.Messages.ElementAt(2).ExtraMessages.ElementAt(1));
        }
    }
}