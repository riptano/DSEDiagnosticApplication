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
    public class UnitOfMeasureTests
    {
        [TestMethod()]
        public void UnitOfMeasureTest()
        {
            var result = new UnitOfMeasure(1m, UnitOfMeasure.Types.MiB);

            Assert.AreEqual(1m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.MiB, result.UnitType);

            Assert.AreEqual(decimal.Round(0.0009765625m, LibrarySettings.UnitOfMeasureRoundDecimals),
                                result.ConvertTo(UnitOfMeasure.Types.GiB));
            Assert.AreEqual(decimal.Round(9.5367431640625e-7m, LibrarySettings.UnitOfMeasureRoundDecimals),
                                result.ConvertTo(UnitOfMeasure.Types.TiB));
            Assert.AreEqual(decimal.Round(9.313225746154785e-10m, LibrarySettings.UnitOfMeasureRoundDecimals),
                                result.ConvertTo(UnitOfMeasure.Types.PiB));
            Assert.AreEqual(1m, result.ConvertTo(UnitOfMeasure.Types.MiB));
            Assert.AreEqual(1024m, result.ConvertTo(UnitOfMeasure.Types.KiB));
            Assert.AreEqual(1048576m, result.ConvertTo(UnitOfMeasure.Types.Byte));
            Assert.AreEqual(8388608m, result.ConvertTo(UnitOfMeasure.Types.Bit));

            result = new UnitOfMeasure("10 MB");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.MiB, result.UnitType);

            result = new UnitOfMeasure("10 mb");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.MiB, result.UnitType);

            result = new UnitOfMeasure("10MB");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.MiB, result.UnitType);

            result = new UnitOfMeasure("10mb");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.MiB, result.UnitType);

            result = new UnitOfMeasure("10 MiB");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.MiB, result.UnitType);

            result = new UnitOfMeasure("10 mib");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.MiB, result.UnitType);

            result = new UnitOfMeasure("10 MB");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.MiB, result.UnitType);
        }

        [TestMethod()]
        public void UnitOfMeasureTest1()
        {
            var strTypes = Enum.GetNames(typeof(UnitOfMeasure.Types));

            foreach (var strType in strTypes)
            {
                var eType = (UnitOfMeasure.Types) Enum.Parse(typeof(UnitOfMeasure.Types), strType);

                if (eType == UnitOfMeasure.Types.WholeNumber || eType == UnitOfMeasure.Types.TimeUnits || eType == UnitOfMeasure.Types.SizeUnits)
                {
                    continue;
                }

                if((eType & UnitOfMeasure.Types.TimeUnits) != 0 && (eType & UnitOfMeasure.Types.SizeUnits) != 0)
                {
                    var result = new UnitOfMeasure("10", strType);

                    Assert.AreEqual(10m, result.Value);
                    Assert.AreEqual(eType, result.UnitType);
                }
            }
        }

        [TestMethod()]
        public void UnitOfMeasureTest2()
        {
            var result = new UnitOfMeasure(1m, UnitOfMeasure.Types.GiB | UnitOfMeasure.Types.HR);

            Assert.AreEqual(1m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.GiB | UnitOfMeasure.Types.HR, result.UnitType);

            Assert.AreEqual(0.000277778m, result.ConvertTo(UnitOfMeasure.Types.GiB | UnitOfMeasure.Types.SEC));
            Assert.AreEqual(0.284444444m, result.ConvertTo(UnitOfMeasure.Types.MiB | UnitOfMeasure.Types.SEC));
        }

        [TestMethod()]
        public void UnitOfMeasureTest3()
        {
            var result = new UnitOfMeasure("1 MB/Sec");

            Assert.AreEqual(1m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.SEC | UnitOfMeasure.Types.MiB, result.UnitType);

            result = new UnitOfMeasure("1.234", UnitOfMeasure.Types.Percent | UnitOfMeasure.Types.Utilization);

            Assert.AreEqual(1.234m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.Percent | UnitOfMeasure.Types.Utilization, result.UnitType);

            result = new UnitOfMeasure("nan", UnitOfMeasure.Types.Percent | UnitOfMeasure.Types.Utilization);

            Assert.AreEqual(0m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.Percent | UnitOfMeasure.Types.Utilization | UnitOfMeasure.Types.NaN, result.UnitType);
            Assert.IsTrue(result.NaN);

            result = new UnitOfMeasure(string.Empty, UnitOfMeasure.Types.Percent | UnitOfMeasure.Types.Utilization);

            Assert.AreEqual(0m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.Percent | UnitOfMeasure.Types.Utilization | UnitOfMeasure.Types.NaN, result.UnitType);
            Assert.IsTrue(result.NaN);           
        }

        [TestMethod()]
        public void UnitOfMeasureTest4()
        {
            var actual = new UnitOfMeasure("1024 KB/Sec");
            var result = actual.NormalizedValue;

            Assert.AreEqual(1m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.SEC | UnitOfMeasure.Types.MiB, result.UnitType);

            var resultA = new UnitOfMeasure("1 MB/Sec");

            Assert.AreEqual(resultA.Value, result.Value);
            Assert.AreEqual(resultA.UnitType, result.UnitType);

            Assert.IsTrue(resultA.Equals(result));

            actual = new UnitOfMeasure("1024 KB/Sec", UnitOfMeasure.Types.Rate);
            result = actual.NormalizedValue;

            Assert.AreEqual(1m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.SEC | UnitOfMeasure.Types.MiB, result.UnitType);

            resultA = new UnitOfMeasure("1 MB/Sec");

            Assert.AreEqual(resultA.Value, result.Value);
            Assert.AreEqual(resultA.UnitType, result.UnitType);

            Assert.IsTrue(resultA.Equals(result));
            Assert.IsTrue(resultA == result);
            Assert.IsFalse(resultA != result);
            Assert.AreEqual(result.GetHashCode(), resultA.GetHashCode());

            resultA = new UnitOfMeasure("2 MB/Sec");

            Assert.IsFalse(resultA.Equals(result));
            Assert.IsFalse(resultA == result);
            Assert.IsTrue(resultA != result);
            Assert.AreNotEqual(result.GetHashCode(), resultA.GetHashCode());            
        }

        [TestMethod()]
        public void UnitOfMeasureTest5()
        {
            var result = new UnitOfMeasure(1m, UnitOfMeasure.Types.HR);

            Assert.AreEqual(1m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.HR, result.UnitType);

            Assert.AreEqual(0.041666667m, result.ConvertTo(UnitOfMeasure.Types.Day));
            Assert.AreEqual(60m, result.ConvertTo(UnitOfMeasure.Types.MIN));
            Assert.AreEqual(1m, result.ConvertTo(UnitOfMeasure.Types.HR));
            Assert.AreEqual(3.6e+6m, result.ConvertTo(UnitOfMeasure.Types.MS));
            Assert.AreEqual(3.6e+12m, result.ConvertTo(UnitOfMeasure.Types.NS));

            result = new UnitOfMeasure("10 HR");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.HR, result.UnitType);

            result = new UnitOfMeasure("10 hr");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.HR, result.UnitType);

            result = new UnitOfMeasure("10HR");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.HR, result.UnitType);

            result = new UnitOfMeasure("10hour");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.HR, result.UnitType);

            result = new UnitOfMeasure("10 HOUR");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.HR, result.UnitType);

            result = new UnitOfMeasure("10 hours");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.HR, result.UnitType);

            result = new UnitOfMeasure("10 hrs");

            Assert.AreEqual(10m, result.Value);
            Assert.AreEqual(UnitOfMeasure.Types.HR, result.UnitType);
        }
    }
}