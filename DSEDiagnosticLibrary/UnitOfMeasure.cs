using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DSEDiagnosticLibrary
{
    [JsonObjectAttribute(MemberSerialization.OptOut)]
    public sealed class UnitOfMeasure : IEquatable<UnitOfMeasure>
    {
        /// <summary>
        /// da (deca) = x10 (rarely used)
        /// h(hecto) = x100(rarely used)
        /// k(kilo) = x1000 = e3
        /// M(Mega) = x1000000 = e6
        /// G(Giga) = x1000000000 = e9
        /// T(Tera) = x1000000000000 = e12
        /// P(Peta) = x1000000000000000 = e15
        /// E(Exa) = x1000000000000000000 = e18
        /// Z(Zetta) = x1000000000000000000000 = e21
        ///
        /// d(deci) = /10 = e-1 (rarely used)
        /// c(cent) = /100 = e-2 (rarely used except for cm)
        /// m(milli) = /1000 = e-3
        /// µ(micro) = /1000000 = e-6
        /// n(nano) = /1000000000 = e-9
        /// p(pico) = /1000000000000 = e-12
        /// f(femto) = /1000000000000000 = e-15
        /// a(atto) = /1000000000000000000 = e-18
        /// </summary>

        [Flags]
        public enum Types : ulong
        {

            Unknown = 0,
            Storage = 0x0001,
            Memory = 0x0002,
            Rate = 0x0004,
            Load = 0x100000,
            Time = 0x200000,
            Frequency = 0x1000000,
            Utilization = 0x2000000,
            Bit = 0x0008,
            Byte = 0x0010,
            KiB = 0x0020,
            MiB = 0x0040,
            GiB = 0x0080,
            TiB = 0x0100,
            PiB = 0x0200,
            /// <summary>
            /// nanoseconds
            /// </summary>
            NS = 0x1000,
            /// <summary>
            /// Milliseconds
            /// </summary>
            MS = 0x2000,
            SEC = 0x4000,
            MIN = 0x8000,
            HR = 0x10000,
            Day = 0x20000,
            Percent = 0x40000,
            /// <summary>
            /// Microsecond
            /// </summary>
            us = 0x80000,
            /// <summary>
            /// Not a Number
            /// </summary>
            NaN = 0x4000000,
            Operations = 0x8000000,
            SizeUnits = Bit | Byte | KiB | MiB | GiB | TiB | PiB,
            TimeUnits = NS | MS | SEC | MIN | HR | Day | us,
            WholeNumber = Bit | Byte | NS | us,
            Attrs = NaN | Memory | Rate | Load | Percent | Time | Frequency | Utilization | Operations
        }

        readonly static Regex RegExValueUOF = new Regex(@"^(\-?[0-9,]*(?:\.[0-9]+)?)\s*([a-z%,_/\ ]*)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public UnitOfMeasure()
        {
            this.UnitType = Types.NaN;
            this.Value = decimal.MinValue;
        }

        public UnitOfMeasure(Types uofType)
        {
            this.UnitType = uofType;

            if(this.NaN)
            {
                this.Value = decimal.MinValue;
            }
        }

        public UnitOfMeasure(string uofString, Types uofType = Types.Unknown)
        {
            var uofValues = RegExValueUOF.Match(uofString);

            if (uofValues.Success)
            {
                this.Value = ConvertToDecimal(uofValues.Groups[1].Value);
                this.UnitType = ConvertToType(uofValues.Groups[2].Value, uofType);
            }
        }

        public UnitOfMeasure(string value, string uof, Types uofType = Types.Unknown)
        {
            this.Value = ConvertToDecimal(value);
            this.UnitType = ConvertToType(uof, uofType);
        }

        public UnitOfMeasure(decimal value, Types uofType)
        {
            this.Value = value;
            this.UnitType = uofType;
        }

        public UnitOfMeasure(UnitOfMeasure copy)
        {
            this.Value = copy.Value;
            this.UnitType = copy.UnitType;
        }

        public UnitOfMeasure(UnitOfMeasure copy, Types newUOM)
        {
            this.Value = copy.ConvertTo(newUOM);
            this.UnitType = newUOM;
        }

        public Types UnitType { get; private set; }
        public decimal Value { get; private set; }

        [JsonIgnore]
        public bool NaN { get { return (this.UnitType & Types.NaN) != 0; } }

        /// <summary>
        /// Returns a new UOM based on the default normalized types.
        /// </summary>
        [JsonIgnore]
        public UnitOfMeasure NormalizedValue
        {
            get
            {
                if ((this.UnitType & Types.SizeUnits) != 0 && (this.UnitType & Types.TimeUnits) != 0)
                {
                    var newUOM = ((this.UnitType & ~Types.SizeUnits) & ~Types.TimeUnits) & ~Types.Attrs;

                    if((this.UnitType & Types.Storage) != 0)
                    {
                        newUOM |= LibrarySettings.DefaultStorageRate;
                    }
                    else
                    {
                        newUOM |= LibrarySettings.DefaultMemoryRate;
                    }

                    return (this.UnitType & Types.NaN) != 0
                                ? new UnitOfMeasure(newUOM | Types.NaN)
                                : new UnitOfMeasure(this.ConvertTo(newUOM), newUOM);
                }

                if ((this.UnitType & Types.SizeUnits) != 0)
                {
                    var newUOM = (this.UnitType & ~Types.SizeUnits) & ~Types.Attrs;

                    if ((this.UnitType & Types.Storage) != 0)
                    {
                        newUOM |= LibrarySettings.DefaultStorageSizeUnit;
                    }
                    else
                    {
                        newUOM |= LibrarySettings.DefaultMemorySizeUnit;
                    }

                    return (this.UnitType & Types.NaN) != 0
                                ? new UnitOfMeasure(newUOM | Types.NaN)
                                : new UnitOfMeasure(this.ConvertSizeUOM(newUOM), newUOM);
                }

                if ((this.UnitType & Types.TimeUnits) != 0)
                {
                    var newUOM = ((this.UnitType & ~Types.TimeUnits) & ~Types.Attrs) | LibrarySettings.DefaultTimeUnit;

                    return (this.UnitType & Types.NaN) != 0
                                ? new UnitOfMeasure(newUOM | Types.NaN)
                                : new UnitOfMeasure(this.ConvertTimeUOM(newUOM), newUOM);
                }

                return new UnitOfMeasure(this);
            }
        }

        #region Convertors

        public decimal ConvertTo(Types newUOM)
        {
            if((this.UnitType & Types.NaN) != 0) throw new ArgumentException(string.Format("Cannot convert a NaN from {0} to {1}", this.UnitType, newUOM));

            if ((this.UnitType & Types.SizeUnits) != 0 && (this.UnitType & Types.TimeUnits) != 0)
            {
                if ((newUOM & Types.SizeUnits) != 0 && (newUOM & Types.TimeUnits) != 0)
                {
                    var newsizeValue = ConvertSizeUOM(this.Value, this.UnitType, newUOM);
                    var timeConversion = ConvertTimeUOM(1m, this.UnitType, newUOM);

                    return Decimal.Round(newsizeValue / timeConversion, LibrarySettings.UnitOfMeasureRoundDecimals);
                }

                throw new ArgumentException(string.Format("Cannot convert from {0} to {1}", this.UnitType, newUOM));
            }

            if ((this.UnitType & Types.SizeUnits) != 0)
            {
                return ConvertSizeUOM(this.Value, this.UnitType, newUOM);
            }

            if ((this.UnitType & Types.TimeUnits) != 0)
            {
                return ConvertTimeUOM(this.Value, this.UnitType, newUOM);
            }

            return Decimal.Round(this.Value, LibrarySettings.UnitOfMeasureRoundDecimals);
        }

        public decimal ConvertSizeUOM(Types newUOM)
        {
            if ((this.UnitType & Types.NaN) != 0) throw new ArgumentException(string.Format("Cannot convert a NaN from {0} to {1}", this.UnitType, newUOM));

            return ConvertSizeUOM(this.Value, this.UnitType, newUOM);
        }

        public static decimal ConvertSizeUOM(decimal value, Types uom, Types newUOM)
        {
            decimal newValue = value;

            newUOM &= Types.SizeUnits;
            switch (uom & Types.SizeUnits)
            {
                case Types.Bit:
                    {
                        switch (newUOM)
                        {
                            case Types.Bit:
                                break;
                            case Types.Byte:
                                newValue = value / 8m;
                                break;
                            case Types.KiB:
                                newValue = value / 8192m;
                                break;
                            case Types.MiB:
                                newValue = value / 8388608m;
                                break;
                            case Types.GiB:
                                newValue = value / 8589934592m;
                                break;
                            case Types.TiB:
                                newValue = value / 8796093022208m;
                                break;
                            case Types.PiB:
                                newValue = value / 9007199254740992m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.Byte:
                    {
                        switch (newUOM)
                        {
                            case Types.Bit:
                                newValue = value * 8m;
                                break;
                            case Types.Byte:
                                break;
                            case Types.KiB:
                                newValue = value / 1024m;
                                break;
                            case Types.MiB:
                                newValue = value / 1048576m;
                                break;
                            case Types.GiB:
                                newValue = value / 1073741824m;
                                break;
                            case Types.TiB:
                                newValue = value / 1099511627776m;
                                break;
                            case Types.PiB:
                                newValue = value / 1125899906842624m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.KiB:
                    {
                        switch (newUOM)
                        {
                            case Types.Bit:
                                newValue = value * 8192m;
                                break;
                            case Types.Byte:
                                newValue = value * 1024m;
                                break;
                            case Types.KiB:
                                break;
                            case Types.MiB:
                                newValue = value / 1024m;
                                break;
                            case Types.GiB:
                                newValue = value / 1048576m;
                                break;
                            case Types.TiB:
                                newValue = value / 1073741824m;
                                break;
                            case Types.PiB:
                                newValue = value / 1099511627776m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.MiB:
                    {
                        switch (newUOM)
                        {
                            case Types.Bit:
                                newValue = value * 8388608m;
                                break;
                            case Types.Byte:
                                newValue = value * 1048576m;
                                break;
                            case Types.KiB:
                                newValue = value * 1024m;
                                break;
                            case Types.MiB:
                                break;
                            case Types.GiB:
                                newValue = value / 1024m;
                                break;
                            case Types.TiB:
                                newValue = value / 1048576m;
                                break;
                            case Types.PiB:
                                newValue = value / 1073741824m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.GiB:
                    {
                        switch (newUOM)
                        {
                            case Types.Bit:
                                newValue = value * 8589934592m;
                                break;
                            case Types.Byte:
                                newValue = value * 1073741824m;
                                break;
                            case Types.KiB:
                                newValue = value * 1048576m;
                                break;
                            case Types.MiB:
                                newValue = value * 1024m;
                                break;
                            case Types.GiB:
                                break;
                            case Types.TiB:
                                newValue = value / 1024m;
                                break;
                            case Types.PiB:
                                newValue = value / 1048576m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.TiB:
                    {
                        switch (newUOM)
                        {
                            case Types.Bit:
                                newValue = value * 8796093022208m;
                                break;
                            case Types.Byte:
                                newValue = value * 1099511627776m;
                                break;
                            case Types.KiB:
                                newValue = value * 1073741824m;
                                break;
                            case Types.MiB:
                                newValue = value * 1048576m;
                                break;
                            case Types.GiB:
                                newValue = value * 1024m;
                                break;
                            case Types.TiB:
                                break;
                            case Types.PiB:
                                newValue = value * 1024m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.PiB:
                    {
                        switch (newUOM)
                        {
                            case Types.Bit:
                                newValue = value * 9007199254740992m;
                                break;
                            case Types.Byte:
                                newValue = value * 1125899906842624m;
                                break;
                            case Types.KiB:
                                newValue = value * 1099511627776m;
                                break;
                            case Types.MiB:
                                newValue = value * 1073741824m;
                                break;
                            case Types.GiB:
                                newValue = value * 1048576m;
                                break;
                            case Types.TiB:
                                newValue = value * 1024m;
                                break;
                            case Types.PiB:
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
            }

            if ((newUOM & Types.WholeNumber) != 0)
            {
                return Decimal.Truncate(newValue);
            }

            return Decimal.Round(newValue, LibrarySettings.UnitOfMeasureRoundDecimals);
        }

        public decimal ConvertTimeUOM(Types newUOM)
        {
            if ((this.UnitType & Types.NaN) != 0) throw new ArgumentException(string.Format("Cannot convert a NaN from {0} to {1}", this.UnitType, newUOM));

            return ConvertTimeUOM(this.Value, this.UnitType, newUOM);
        }

        public static decimal ConvertTimeUOM(decimal value, Types uom,Types newUOM)
        {
            decimal newValue = value;

            newUOM &= Types.TimeUnits;
            switch (uom & Types.TimeUnits)
            {
                case Types.us:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                break;
                            case Types.NS:
                                newValue = value * 1000m;
                                break;
                            case Types.MS:
                                newValue = value / 1000m;
                                break;
                            case Types.SEC:
                                newValue = value / 1000000m;
                                break;
                            case Types.MIN:
                                newValue = value / 60000000m;
                                break;
                            case Types.HR:
                                newValue = value / 3600000000m;
                                break;
                            case Types.Day:
                                newValue = value / 86400000000m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.NS:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                newValue = value / 1000m;
                                break;
                            case Types.NS:
                                break;
                            case Types.MS:
                                newValue = value / 1000000m;
                                break;
                            case Types.SEC:
                                newValue = value / 1000000000m;
                                break;
                            case Types.MIN:
                                newValue = value / 60000000000m;
                                break;
                            case Types.HR:
                                newValue = value / 3600000000000m;
                                break;
                            case Types.Day:
                                newValue = value / 86400000000000m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.MS:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                newValue = value * 1000m;
                                break;
                            case Types.NS:
                                newValue = value * 1000000m;
                                break;
                            case Types.MS:
                                break;
                            case Types.SEC:
                                newValue = value / 1000m;
                                break;
                            case Types.MIN:
                                newValue = value / 60000m;
                                break;
                            case Types.HR:
                                newValue = value / 3600000m;
                                break;
                            case Types.Day:
                                newValue = value / 86400000m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.SEC:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                newValue = value * 1e+6m;
                                break;
                            case Types.NS:
                                newValue = value * 1e+9m;
                                break;
                            case Types.MS:
                                newValue = value * 1000m;
                                break;
                            case Types.SEC:
                                break;
                            case Types.MIN:
                                newValue = value / 60m;
                                break;
                            case Types.HR:
                                newValue = value / 3600m;
                                break;
                            case Types.Day:
                                newValue = value / 86400m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.MIN:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                newValue = value * 6e+7m;
                                break;
                            case Types.NS:
                                newValue = value * 6e+10m;
                                break;
                            case Types.MS:
                                newValue = value * 60000m;
                                break;
                            case Types.SEC:
                                newValue = value * 60m;
                                break;
                            case Types.MIN:
                                break;
                            case Types.HR:
                                newValue = value / 60m;
                                break;
                            case Types.Day:
                                newValue = value / 1440m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.HR:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                newValue = value * 3.6e+9m;
                                break;
                            case Types.NS:
                                newValue = value * 3.6e+12m;
                                break;
                            case Types.MS:
                                newValue = value * 3.6e+6m;
                                break;
                            case Types.SEC:
                                newValue = value * 3600m;
                                break;
                            case Types.MIN:
                                newValue = value * 60m;
                                break;
                            case Types.HR:
                                break;
                            case Types.Day:
                                newValue = value / 24m;
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
                case Types.Day:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                newValue = value * 8.64e+10m;
                                break;
                            case Types.NS:
                                newValue = value * 8.64e+13m;
                                break;
                            case Types.MS:
                                newValue = value * 8.64e+7m;
                                break;
                            case Types.SEC:
                                newValue = value * 86400m;
                                break;
                            case Types.MIN:
                                newValue = value * 1440m;
                                break;
                            case Types.HR:
                                newValue = value * 24m;
                                break;
                            case Types.Day:
                                break;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", uom, newUOM));
                        }
                        break;
                    }
            }

            if((newUOM & Types.WholeNumber) != 0)
            {
                return Decimal.Truncate(newValue);
            }

            return Decimal.Round(newValue, LibrarySettings.UnitOfMeasureRoundDecimals);
        }

        #endregion

        #region public statics
        public static explicit operator decimal(UnitOfMeasure uom)
        {
            return uom.Value;
        }

        public static explicit operator decimal?(UnitOfMeasure uom)
        {
            if (uom == null || uom.NaN) return null;

            return uom.Value;
        }

        public static explicit operator TimeSpan(UnitOfMeasure uom)
        {
            if((uom.UnitType & Types.TimeUnits) != 0)
            {
                return TimeSpan.FromMilliseconds((double)uom.ConvertTimeUOM(Types.MS));
            }

            throw new InvalidCastException(string.Format("Trying to cast from a {0} to a TimeSpan which is invalid.", uom.UnitType));
        }

        public static explicit operator TimeSpan?(UnitOfMeasure uom)
        {
            if (uom == null || uom.NaN) return null;

            if ((uom.UnitType & Types.TimeUnits) != 0)
            {
                return TimeSpan.FromMilliseconds((double)uom.ConvertTimeUOM(Types.MS));
            }

            throw new InvalidCastException(string.Format("Trying to cast from a {0} to a TimeSpan which is invalid.", uom.UnitType));
        }

        public static bool operator==(UnitOfMeasure a, UnitOfMeasure b)
        {
            return ReferenceEquals(a, b) ? true : (ReferenceEquals(a,null) ? ReferenceEquals(b, null) : a.Equals(b));
        }

        public static bool operator !=(UnitOfMeasure a, UnitOfMeasure b)
        {
            return ReferenceEquals(a, b) ? false : (ReferenceEquals(a, null) ? !ReferenceEquals(b, null) : !(a.Equals(b)));
        }

        public static UnitOfMeasure Create(string value, string uof, Types uofType = Types.Unknown)
        {
            if (IsNaN(value))
            {
                return null;
            }

            try
            {
                return new UnitOfMeasure(value, uof, uofType);
            }
            catch
            { }

            return null;
        }

        public static UnitOfMeasure Create(string uofString, Types uofType = Types.Unknown, bool emptyNullIsNan = true)
        {
            if (IsNaN(uofString, emptyNullIsNan))
            {
                return emptyNullIsNan ? new UnitOfMeasure(Types.NaN | uofType) : null;
            }

            try
            {
                var uofValues = RegExValueUOF.Match(uofString);

                if (uofValues.Success)
                {
                    return new UnitOfMeasure(ConvertToDecimal(uofValues.Groups[1].Value),
                                                ConvertToType(uofValues.Groups[2].Value,
                                                uofType));
                }
            }
            catch
            {}

            return null;
        }

        public static UnitOfMeasure Create(int uof, Types uofType)
        {
            return new UnitOfMeasure((decimal) uof, uofType);
        }

        public static UnitOfMeasure Create(long uof, Types uofType)
        {
            return new UnitOfMeasure((decimal)uof, uofType);
        }

        public static UnitOfMeasure Create(int? uof, Types uofType)
        {
            return uof.HasValue ? new UnitOfMeasure((decimal)uof.Value, uofType) : null;
        }

        public static UnitOfMeasure Create(long? uof, Types uofType)
        {
            return uof.HasValue ? new UnitOfMeasure((decimal)uof.Value, uofType) : null;
        }

        public static UnitOfMeasure Create(TimeSpan uof, Types uofType)
        {
            return new UnitOfMeasure((decimal) uof.TotalMilliseconds, Types.Time | Types.MS);
        }

        public static UnitOfMeasure Create(TimeSpan? uof, Types uofType)
        {
            return uof.HasValue ? new UnitOfMeasure((decimal)uof.Value.TotalMilliseconds, Types.Time | Types.MS) : null;
        }

        public static UnitOfMeasure Create(decimal uof, Types uofType)
        {
            return new UnitOfMeasure(uof, uofType);
        }

        public static UnitOfMeasure Create(decimal? uof, Types uofType)
        {
            return uof.HasValue ? new UnitOfMeasure(uof.Value, uofType) : null;
        }

        public static Types ConvertToType(string uof, Types uofType = Types.Unknown)
        {
            if (string.IsNullOrEmpty(uof))
            {
                return Types.NaN | uofType;
            }

            string[] splitUOFs = null;

            if (uof.IndexOfAny(new char[] { '/', '_', ',', ' ' }) >= 0)
            {
                splitUOFs = uof.Split(new char[] { '/', '_', ',', ' ' });
            }

            if (splitUOFs != null)
            {
                Types newUOF = uofType;
                Types returnedUOF;

                if(splitUOFs.ElementAtOrDefault(0) == "min") //assume minimal if first word
                {
                    splitUOFs = splitUOFs.Skip(1).ToArray();
                }

                foreach (var item in splitUOFs)
                {
                    returnedUOF = ConvertToType(item, Types.Unknown);

                    if((returnedUOF & Types.TimeUnits) != 0
                        && (newUOF & Types.SEC) != 0)
                    {
                        newUOF &= ~Types.SEC;
                    }

                    newUOF |= returnedUOF;
                }

                if((newUOF & Types.TimeUnits) != 0
                    || (newUOF & Types.SizeUnits) != 0
                    || (newUOF & Types.NaN) != 0)
                {
                    return newUOF;
                }

                return uofType;
            }

            switch (uof.Trim().ToLower())
            {
                case "bit":
                case "bits":
                    return Types.Bit | uofType;
                case "bytes":
                case "byte":
                    return Types.Byte | uofType;
                case "kilobyte":
                case "kilobytes":
                case "kibibyte":
                case "kibibytes":
                case "kb":
                case "kib":
                    return Types.KiB | uofType;
                case "megabyte":
                case "megabytes":
                case "mebibyte":
                case "mebibytes":
                case "mb":
                case "mib":
                    return Types.MiB | uofType;
                case "gigabyte":
                case "gigabytes":
                case "gibibyte":
                case "gibibytes":
                case "gb":
                case "gib":
                    return Types.GiB | uofType;
                case "terabyte":
                case "terabytes":
                case "trbibyte":
                case "tebibytes":
                case "tb":
                case "tib":
                    return Types.TiB | uofType;
                case "petabyte":
                case "petabytes":
                case "pebibyte":
                case "pebibytes":
                case "pb":
                case "pib":
                    return Types.PiB | uofType;
                case "nanoseconds":
                case "nanosecond":
                case "ns":
                    return Types.NS | uofType;
                case "milliseconds":
                case "millisecond":
                case "ms":
                    return Types.MS | uofType;
                case "seconds":
                case "second":
                case "sec":
                case "secs":
                case "s":
                    return Types.SEC | uofType;
                case "minutes":
                case "minute":
                case "min":
                case "mins":
                case "m":
                    return Types.MIN | uofType;
                case "hours":
                case "hour":
                case "hr":
                case "hrs":
                case "h":
                    return Types.HR | uofType;
                case "day":
                case "days":
                case "d":
                    return Types.Day | uofType;
                case "percent":
                case "%":
                case "pct":
                case "chance":
                    return Types.Percent | uofType;
                case "microsecond":
                case "microseconds":
                case "us":
                    return Types.us | uofType;
                case "nan":
                case "notanumber":
                case "not a number":
                case "notanbr":
                case "not a nbr":
                case "notnumber":
                case "notnbr":
                case "not number":
                case "not nbr":
                case "null":
                    return Types.NaN;
                case "mbps":
                    return Types.MiB | Types.SEC | Types.Rate | uofType;
                case "operations":
                case "operation":
                    return Types.Operations | Types.Rate | uofType;
                case "rate":
                case "throughput":
                case "per":
                    return Types.Rate | uofType;
                case "period":
                case "periods":
                case "ttl":
                    return Types.SEC | Types.Time | uofType;
                case "ops":
                    return Types.SEC | Types.Operations | Types.Rate | uofType;
            }

            Types parsedType;
            if (Enum.TryParse<Types>(uof, true, out parsedType))
            {
                return parsedType | uofType;
            }

            return uofType;
        }
        public static decimal ConvertToDecimal(string value)
        {
            return decimal.Parse(value, System.Globalization.NumberStyles.Number);
        }
        public static bool IsNaN(string value, bool emptynullIsNaN = true)
        {
            value = value?.Trim();

            if (emptynullIsNaN && string.IsNullOrEmpty(value))
            {
                return true;
            }

            switch (value.ToLower())
            {
                case "nan":
                case "notanumber":
                case "not a number":
                case "notanbr":
                case "not a nbr":
                case "notnumber":
                case "notnbr":
                case "not number":
                case "not nbr":
                case "null":
                    return true;
            }
            return false;
        }
        #endregion

        #region IEquatable
        public bool Equals(UnitOfMeasure other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (ReferenceEquals(other,null)) return false;

            var thisN = this.NormalizedValue;
            var otherN = other.NormalizedValue;

            if(thisN.UnitType == otherN.UnitType)
            {
                return thisN.Value == otherN.Value;
            }

            return false;
        }
        #endregion

        #region overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj)) return true;
            if (obj is UnitOfMeasure) return this.Equals((UnitOfMeasure)obj);

            return base.Equals(obj);
        }

        [JsonProperty(PropertyName="HashCode")]
        private int _hashcode = 0;
        public override int GetHashCode()
        {
            if (this._hashcode != 0) return this._hashcode;

            var thisN = this.NormalizedValue;
            return this._hashcode = (589 + thisN.UnitType.GetHashCode()) * 31 + thisN.Value.GetHashCode();
        }

        public override string ToString()
        {
            return this.NaN ? this.UnitType.ToString() : string.Format("{0} {1}", this.Value, this.UnitType);
        }

        #endregion
    }
}
