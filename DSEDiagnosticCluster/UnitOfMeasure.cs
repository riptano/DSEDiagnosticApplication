using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DSEDiagnosticLibrary
{
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
            SizeUnits = Bit | Byte | KiB | MiB | GiB | TiB | PiB,
            TimeUnits = NS | MS | SEC | MIN | HR | Day | us
        }

        static Regex RegExValueUOF = new Regex(@"([0-9\-.+,]+)\s*([a-z0-9%#]*)", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);

        public UnitOfMeasure() { }

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

        public Types UnitType { get; set; }
        public decimal Value { get; set; }

        #region Convertors

        public decimal ConvertSizeUOM(Types newUOM)
        {
            newUOM &= Types.SizeUnits;
            switch(this.UnitType & Types.SizeUnits)
            {
                case Types.Bit:
                {
                    switch (newUOM)
                    {
                        case Types.Bit:
                                return this.Value;
                        case Types.Byte:
                                return this.Value * 0.125m;
                        case Types.KiB:
                                return this.Value * 0.00012207m;
                        case Types.MiB:
                                return this.Value * 1.1921e-7m;
                        case Types.GiB:
                                return this.Value * 1.1642e-10m;
                        case Types.TiB:
                                return this.Value * 1.1369e-13m;
                        case Types.PiB:
                                return this.Value * 1.1102e-16m;
                        default:
                            throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", this.UnitType, newUOM));
                    }
                }
                case Types.Byte:
                {
                    switch (newUOM)
                    {
                        case Types.Bit:
                            return this.Value * 8m;
                        case Types.Byte:
                            return this.Value;
                        case Types.KiB:
                            return this.Value * 0.000976563m;
                        case Types.MiB:
                            return this.Value * 9.5367e-7m;
                        case Types.GiB:
                            return this.Value * 9.3132e-10m;
                        case Types.TiB:
                            return this.Value * 9.0949e-13m;
                        case Types.PiB:
                            return this.Value * 8.8818e-16m;
                        default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", this.UnitType, newUOM));
                    }
                }
                case Types.KiB:
                {
                    switch (newUOM)
                    {
                        case Types.Bit:
                            return this.Value * 8192m;
                        case Types.Byte:
                            return this.Value * 1024m;
                        case Types.KiB:
                            return this.Value;
                        case Types.MiB:
                            return this.Value * 0.000976563m;
                        case Types.GiB:
                            return this.Value * 9.5367e-7m;
                        case Types.TiB:
                            return this.Value * 9.3132e-10m;
                        case Types.PiB:
                            return this.Value * 9.0949e-13m;
                        default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", this.UnitType, newUOM));
                    }
                }
                case Types.MiB:
                {
                    switch (newUOM)
                    {
                        case Types.Bit:
                            return this.Value * 8.389e+6m;
                        case Types.Byte:
                            return this.Value * 1.049e+6m;
                        case Types.KiB:
                            return this.Value * 1024m;
                        case Types.MiB:
                            return this.Value;
                        case Types.GiB:
                            return this.Value * 0.000976563m;
                        case Types.TiB:
                            return this.Value * 9.5367e-7m;
                        case Types.PiB:
                            return this.Value * 9.3132e-10m;
                        default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", this.UnitType, newUOM));
                     }
                }
                case Types.GiB:
                {
                    switch (newUOM)
                    {
                        case Types.Bit:
                            return this.Value * 8.59e+9m;
                        case Types.Byte:
                            return this.Value * 1.074e+9m;
                        case Types.KiB:
                            return this.Value * 1.049e+6m;
                        case Types.MiB:
                            return this.Value * 1024m;
                        case Types.GiB:
                            return this.Value;
                        case Types.TiB:
                            return this.Value * 0.000976563m;
                        case Types.PiB:
                            return this.Value * 9.5367e-7m;
                        default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", this.UnitType, newUOM));
                    }
                }
                case Types.TiB:
                {
                    switch (newUOM)
                    {
                        case Types.Bit:
                            return this.Value * 8.796e+12m;
                        case Types.Byte:
                            return this.Value * 1.1e+12m;
                        case Types.KiB:
                            return this.Value * 1.074e+9m;
                        case Types.MiB:
                            return this.Value * 1.049e+6m;
                        case Types.GiB:
                            return this.Value * 1024m;
                        case Types.TiB:
                            return this.Value;
                        case Types.PiB:
                            return this.Value * 0.000976563m;
                        default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", this.UnitType, newUOM));
                    }
                }
                case Types.PiB:
                {
                    switch (newUOM)
                    {
                        case Types.Bit:
                            return this.Value * (decimal)9.007e+15;
                        case Types.Byte:
                            return this.Value * (decimal)1.126e+15;
                        case Types.KiB:
                            return this.Value * (decimal)1.1e+12;
                        case Types.MiB:
                            return this.Value * (decimal)1.074e+9;
                        case Types.GiB:
                            return this.Value * (decimal)1.049e+6;
                        case Types.TiB:
                            return this.Value * 1024m;
                        case Types.PiB:
                            return this.Value;
                        default:
                                throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", this.UnitType, newUOM));
                    }
                }
            }

            throw new ArgumentException(string.Format("Cannot convert a Size Unit from {0} to {1}", this.UnitType, newUOM));
        }

        public decimal ConvertTimeUOM(Types newUOM)
        {
            newUOM &= Types.TimeUnits;
            switch (this.UnitType & Types.TimeUnits)
            {
                case Types.us:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                return this.Value;
                            case Types.NS:
                                return this.Value * 1000m;
                            case Types.MS:
                                return this.Value * 0.001m;
                            case Types.SEC:
                                return this.Value * 1e-6m;
                            case Types.MIN:
                                return this.Value * 1.6667e-8m;
                            case Types.HR:
                                return this.Value * 2.7778e-10m;
                            case Types.Day:
                                return this.Value * 1.1574e-11m;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", this.UnitType, newUOM));
                        }
                    }
                case Types.NS:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                return this.Value * 0.001m;
                            case Types.NS:
                                return this.Value;
                            case Types.MS:
                                return this.Value * 1e-6m;
                            case Types.SEC:
                                return this.Value * 1e-9m;
                            case Types.MIN:
                                return this.Value * 1.6667e-11m;
                            case Types.HR:
                                return this.Value * 2.7778e-13m;
                            case Types.Day:
                                return this.Value * 1.1574e-14m;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", this.UnitType, newUOM));
                        }
                    }
                case Types.MS:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                return this.Value * 1000m;
                            case Types.NS:
                                return this.Value * 1e+6m;
                            case Types.MS:
                                return this.Value;
                            case Types.SEC:
                                return this.Value * 0.001m;
                            case Types.MIN:
                                return this.Value * 1.6667e-5m;
                            case Types.HR:
                                return this.Value * 2.7778e-7m;
                            case Types.Day:
                                return this.Value * 1.1574e-8m;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", this.UnitType, newUOM));
                        }
                    }
                case Types.SEC:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                return this.Value * 1e+6m;
                            case Types.NS:
                                return this.Value * 1e+9m;
                            case Types.MS:
                                return this.Value * 1000m;
                            case Types.SEC:
                                return this.Value;
                            case Types.MIN:
                                return this.Value * 0.0166667m;
                            case Types.HR:
                                return this.Value * 0.000277778m;
                            case Types.Day:
                                return this.Value * 1.1574e-5m;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", this.UnitType, newUOM));
                        }
                    }
                case Types.MIN:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                return this.Value * 6e+7m;
                            case Types.NS:
                                return this.Value * 6e+10m;
                            case Types.MS:
                                return this.Value * 60000m;
                            case Types.SEC:
                                return this.Value * 60m;
                            case Types.MIN:
                                return this.Value;
                            case Types.HR:
                                return this.Value * 0.0166667m;
                            case Types.Day:
                                return this.Value * 0.000694444m;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", this.UnitType, newUOM));
                        }
                    }
                case Types.HR:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                return this.Value * 3.6e+9m;
                            case Types.NS:
                                return this.Value * 3.6e+12m;
                            case Types.MS:
                                return this.Value * 3.6e+6m;
                            case Types.SEC:
                                return this.Value * 3600m;
                            case Types.MIN:
                                return this.Value * 60m;
                            case Types.HR:
                                return this.Value;
                            case Types.Day:
                                return this.Value * 0.0416667m;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", this.UnitType, newUOM));
                        }
                    }
                case Types.Day:
                    {
                        switch (newUOM)
                        {
                            case Types.us:
                                return this.Value * 8.64e+10m;
                            case Types.NS:
                                return this.Value * 8.64e+13m;
                            case Types.MS:
                                return this.Value * 8.64e+7m;
                            case Types.SEC:
                                return this.Value * 86400m;
                            case Types.MIN:
                                return this.Value * 1440m;
                            case Types.HR:
                                return this.Value * 24m;
                            case Types.Day:
                                return this.Value;
                            default:
                                throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", this.UnitType, newUOM));
                        }
                    }
            }

            throw new ArgumentException(string.Format("Cannot convert a Time Unit from {0} to {1}", this.UnitType, newUOM));
        }


        #endregion

        #region public statics
        public static explicit operator decimal(UnitOfMeasure uom)
        {
            return uom.Value;
        }

        public static explicit operator TimeSpan(UnitOfMeasure uom)
        {
            if(uom.UnitType.HasFlag(Types.TimeUnits))
            {
                return TimeSpan.FromMilliseconds((double)uom.ConvertTimeUOM(Types.MS));
            }

            throw new InvalidCastException(string.Format("Trying to cast from a {0} to a TimeSpan which is invalid.", uom.UnitType));
        }

        public static UnitOfMeasure Create(string value, string uof, Types uofType = Types.Unknown)
        {
            if (IsNaN(value))
            {
                return null;
            }

            return new UnitOfMeasure(value, uof, uofType);
        }

        public static UnitOfMeasure Create(string uofString, Types uofType = Types.Unknown)
        {
            if (IsNaN(uofString))
            {
                return null;
            }

            return new UnitOfMeasure(uofString, uofType);
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

        #endregion

        #region private methods
        static Types ConvertToType(string uof, Types uofType)
        {
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
                case "s":
                    return Types.SEC | uofType;
                case "minutes":
                case "minute":
                case "min":
                case "m":
                    return Types.MIN | uofType;
                case "hours":
                case "hour":
                case "hr":
                case "h":
                    return Types.HR | uofType;
                case "day":
                case "days":
                case "d":
                    return Types.Day | uofType;
                case "percent":
                case "%":
                case "pct":
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
                    return Types.NaN;
            }

            Types parsedType;
            if (Enum.TryParse<Types>(uof, true, out parsedType))
            {
                return parsedType | uofType;
            }

            return uofType;
        }
        static decimal ConvertToDecimal(string value)
        {
            return decimal.Parse(value, System.Globalization.NumberStyles.Number);
        }
        static bool IsNaN(string value)
        {
            value = value?.Trim();

            if (string.IsNullOrEmpty(value))
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
                    return true;
            }
            return false;
        }
        #endregion

        #region IEquatable
        public bool Equals(UnitOfMeasure other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (other == null) return false;

            return this.Value == other.Value;
        }
        #endregion

        #region overrides

        public override string ToString()
        {
            return string.Format("{0} {1}", this.Value, this.UnitType);
        }

        #endregion
    }
}
