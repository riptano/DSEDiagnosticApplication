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
    public class NodeIdentifierTests
    {
        [TestMethod()]
        public void ValidNodeIdNameTest()
        {
            Assert.IsTrue(NodeIdentifier.ValidNodeIdName("10.0.0.1"));
            Assert.IsTrue(NodeIdentifier.ValidNodeIdName("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
            Assert.IsTrue(NodeIdentifier.ValidNodeIdName("hostname"));
            Assert.IsTrue(NodeIdentifier.ValidNodeIdName("hostname.ds.one.us"));
            Assert.IsTrue(NodeIdentifier.ValidNodeIdName("hostname.com"));
            Assert.IsFalse(NodeIdentifier.ValidNodeIdName("localhost"));
            Assert.IsFalse(NodeIdentifier.ValidNodeIdName("0.0.0.0"));
            Assert.IsFalse(NodeIdentifier.ValidNodeIdName("127.0.0.1"));
            Assert.IsFalse(NodeIdentifier.ValidNodeIdName("::1"));
            Assert.IsFalse(NodeIdentifier.ValidNodeIdName("0:0:0:0:0:0:0:1"));
        }

        [TestMethod()]
        public void CreateNodeIdentiferTest()
        {
            NodeIdentifier result = NodeIdentifier.CreateNodeIdentifer("abc999_10.1.2.4.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("10.1.2.4", result.Addresses.First().ToString());

            result = NodeIdentifier.CreateNodeIdentifer("abc999 10.1.2.4.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("10.1.2.4", result.Addresses.First().ToString());

            result = NodeIdentifier.CreateNodeIdentifer("abc999-10.1.2.4.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("10.1.2.4", result.Addresses.First().ToString());
            
            result = NodeIdentifier.CreateNodeIdentifer("abc999-2001:0db8:85a3:0000:0000:8a2e:0370:7334.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("2001:db8:85a3::8a2e:370:7334", result.Addresses.First().ToString());

            result = NodeIdentifier.CreateNodeIdentifer("abc999 2001:0db8:85a3:0000:0000:8a2e:0370:7334.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("2001:db8:85a3::8a2e:370:7334", result.Addresses.First().ToString());


            result = NodeIdentifier.CreateNodeIdentifer("10.1.2.4.abc999.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("10.1.2.4", result.Addresses.First().ToString());

            result = NodeIdentifier.CreateNodeIdentifer("10.1.2.4 abc999.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("10.1.2.4", result.Addresses.First().ToString());

            result = NodeIdentifier.CreateNodeIdentifer("10.1.2.4-abc999.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("10.1.2.4", result.Addresses.First().ToString());

            result = NodeIdentifier.CreateNodeIdentifer("2001:0db8:85a3:0000:0000:8a2e:0370:7334-abc999.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("2001:db8:85a3::8a2e:370:7334", result.Addresses.First().ToString());

            result = NodeIdentifier.CreateNodeIdentifer("2001:0db8:85a3:0000:0000:8a2e:0370:7334 abc999.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("2001:db8:85a3::8a2e:370:7334", result.Addresses.First().ToString());

            result = NodeIdentifier.CreateNodeIdentifer("hostname abc999.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("hostname", result.HostName);

            result = NodeIdentifier.CreateNodeIdentifer("hostname+abc999.txt");
            Assert.IsNotNull(result);
            Assert.AreEqual("hostname", result.HostName);

        }

        [TestMethod()]
        public void CreateTest()
        {
            Assert.Fail();
        }
    }
}