using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Numerics;

namespace DSEDiagnosticLibrary
{
    public abstract class TokenRangeInfo : IEquatable<TokenRangeInfo>
    {
        static readonly Regex RegExTokenRange = new Regex(Properties.Settings.Default.TokenRangeRegEx, RegexOptions.Compiled);

        /// <summary>
        /// If true, this range wraps from a starting range that is positive but ends in a negative numeric value (e.g., from the maximum numeric value to the minimal numeric value).
        /// </summary>
        abstract public bool WrapsRange { get; }
        /// <summary>
        /// Start Token (exclusive)
        /// </summary>
        abstract public object StartRange { get; }

        /// <summary>
        /// End Token (inclusive)
        /// </summary>
        abstract public object EndRange { get; } //End Token (inclusive)

        /// <summary>
        /// The number of tokens (slots) that can be accommodated within this range
        /// </summary>
        abstract public object Slots { get; }
        abstract public string SlotsFormatted();

        /// <summary>
        /// The size/amount of data that is currently within this token range.
        /// </summary>
        abstract public UnitOfMeasure Load { get; }

        /// <summary>
        /// Returns the Token range as a string representation 
        /// </summary>
        public string Range
        {
            get
            {
                return string.Format("({0},{1}]", this.StartRange, this.EndRange);
            }
        }

        /// <summary>
        /// Checks to determine if checkRange is within this range.
        /// </summary>
        /// <param name="checkRange"></param>
        /// <returns></returns>
        abstract public bool IsWithinRange(TokenRangeInfo checkRange);

        abstract public bool IsWithinRange(object checkRange);
        
        public override string ToString()
        {
            return string.Format("{0}{{Range:{1}, Slots:{2}{3}}}",
                                    this.GetType().Name,
                                    this.Range,
                                    this.SlotsFormatted(),
                                    this.Load.NaN ? string.Empty : string.Format(" Load:{0}", this.Load));
        }

        #region Overrides/Equals
        abstract public bool Equals(TokenRangeInfo other);        

        public override bool Equals(object other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;
            if (other is TokenRangeInfo) return this.Equals((TokenRangeInfo)other);

            return false;
        }

        public override int GetHashCode()
        {
            return (589 + this.StartRange.GetHashCode()) * 31 + this.EndRange.GetHashCode();
        }
        #endregion

        #region Static methods

        public static TokenRangeInfo CreateTokenRange(string startToken, string endToken, string loadToken = null)
        {
            if(startToken.Length > 20 || endToken.Length > 20)
            {
                BigInteger startBRange;
                BigInteger endBRange;

                if (!BigInteger.TryParse(startToken, out startBRange))
                {
                    throw new ArgumentException(string.Format("Could not parse value \"{0}\" into a Big Integer Start Token value.", startToken), "startToken");
                }
                if (!BigInteger.TryParse(endToken, out endBRange))
                {
                    throw new ArgumentException(string.Format("Could not parse value \"{0}\" into a Big Integer End Token value.", endToken), "endToken");
                }

                return new TokenRangeBigInt(startBRange, endBRange, UnitOfMeasure.Create(loadToken));
            }

            long startRange;
            long endRange;
            
            if (!long.TryParse(startToken, out startRange))
            {
                throw new ArgumentException(string.Format("Could not parse value \"{0}\" into a long Start Token value.", startToken), "startToken");
            }
            if (!long.TryParse(endToken, out endRange))
            {
                throw new ArgumentException(string.Format("Could not parse value \"{0}\" into a long End Token value.", endToken), "endToken");
            }

            return new TokenRangeLong(startRange, endRange, UnitOfMeasure.Create(loadToken));
        }

        public static TokenRangeInfo CreateTokenRange(string startToken, string endToken, UnitOfMeasure? loadToken = null)
        {
            if (startToken.Length > 20 || endToken.Length > 20)
            {
                BigInteger startBRange;
                BigInteger endBRange;

                if (!BigInteger.TryParse(startToken, out startBRange))
                {
                    throw new ArgumentException(string.Format("Could not parse value \"{0}\" into a Big Integer Start Token value.", startToken), "startToken");
                }
                if (!BigInteger.TryParse(endToken, out endBRange))
                {
                    throw new ArgumentException(string.Format("Could not parse value \"{0}\" into a Big Integer End Token value.", endToken), "endToken");
                }

                return new TokenRangeBigInt(startBRange, endBRange, loadToken);
            }

            long startRange;
            long endRange;

            if (!long.TryParse(startToken, out startRange))
            {
                throw new ArgumentException(string.Format("Could not parse value \"{0}\" into a long Start Token value.", startToken), "startToken");
            }
            if (!long.TryParse(endToken, out endRange))
            {
                throw new ArgumentException(string.Format("Could not parse value \"{0}\" into a long End Token value.", endToken), "endToken");
            }

            return new TokenRangeLong(startRange, endRange, loadToken);
        }


        public static TokenRangeInfo CreateTokenRange(string tokenRange, string loadToken = null)
        {
            var tokenMatch = RegExTokenRange.Match(tokenRange);

            if (tokenMatch.Success)
            {                
                return CreateTokenRange(tokenMatch.Groups[1].Value, tokenMatch.Groups[2].Value, loadToken);
            }
            else
            {
                throw new ArgumentException(string.Format("Could not parse value \"{0}\" into a Token Range.", tokenRange), "tokenRange");
            }
        }

        #endregion
    }

    public sealed class TokenRangeLong : TokenRangeInfo, IEquatable<TokenRangeLong>
    {

        public TokenRangeLong(long startToken, long endToken, UnitOfMeasure? loadToken = null)
        {
            this.StartRangeNum = startToken;
            this.EndRangeNum = endToken;
            this.Load = loadToken.HasValue ? loadToken.Value : UnitOfMeasure.NaNValue;

            if (this.StartRangeNum > 0 && this.EndRangeNum < 0)
            {
                this.SlotsNum = (ulong)((this.EndRangeNum - long.MinValue)
                                        + (long.MaxValue - this.StartRangeNum));
                this.WrapsRange = true;
            }
            else
            {
                this.SlotsNum = (ulong)(this.EndRangeNum - this.StartRangeNum);
            }
        }

        /// <summary>
        /// If true, this range wraps from a starting range that is positive but ends in a negative numeric value (e.g., from the maximum numeric value to the minimal numeric value).
        /// </summary>
        override public bool WrapsRange { get; }
        /// <summary>
        /// Start Token (exclusive)
        /// </summary>
        override public object StartRange { get { return this.StartRangeNum; } }
        public long StartRangeNum { get; }

        /// <summary>
        /// End Token (inclusive)
        /// </summary>
        override public object EndRange { get { return this.EndRangeNum; } } //End Token (inclusive)
        public long EndRangeNum { get; }

        /// <summary>
        /// The number of tokens (slots) that can be accommodated within this range
        /// </summary>
        override public object Slots { get { return this.SlotsNum; } }
        public ulong SlotsNum { get; }
        public override string SlotsFormatted()
        {
            return this.SlotsNum.ToString("###,###,###,###,###,###,##0");
        }

        /// <summary>
        /// The size/amount of data that is currently within this token range.
        /// </summary>
        override public UnitOfMeasure Load { get; }

        /// <summary>
        /// Checks to determine if checkRange is within this range.
        /// </summary>
        /// <param name="checkRange"></param>
        /// <returns></returns>
        override public bool IsWithinRange(TokenRangeInfo checkRange)
        {
            var numCheckRange = checkRange as TokenRangeLong;

            if (numCheckRange == null) return false;

            if (numCheckRange.EndRangeNum <= this.EndRangeNum)
            {
                if (numCheckRange.EndRangeNum > this.StartRangeNum)
                {
                    return true;
                }
            }

            if (numCheckRange.StartRangeNum > this.StartRangeNum)
            {
                if (numCheckRange.StartRangeNum < this.EndRangeNum)
                {
                    return true;
                }
            }

            return false;
        }

        override public bool IsWithinRange(object checkRange)            
        {
            if (checkRange is long) return this.IsWithinRange((long)checkRange);

            return false;
        }

        public bool IsWithinRange(long checkRange)
        {
            if (checkRange <= this.EndRangeNum)
            {
                if (checkRange > this.StartRangeNum)
                {
                    return true;
                }
            }

            return false;
        }

        public bool Equals(TokenRangeLong other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;
            
            return this.EndRangeNum == other.EndRangeNum
                    && this.StartRangeNum == other.StartRangeNum;
        }

        override public bool Equals(TokenRangeInfo other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;
            if (other is TokenRangeLong) return this.Equals((TokenRangeLong)other);

            return false;
        }

    }

    public sealed class TokenRangeBigInt : TokenRangeInfo, IEquatable<TokenRangeBigInt>
    {
        public static readonly BigInteger MinValue = BigInteger.Parse("-170141183460469231731687303715884105728");
        public static readonly BigInteger MaxValue = BigInteger.Parse("170141183460469231731687303715884105727");

        public TokenRangeBigInt(BigInteger startToken, BigInteger endToken, UnitOfMeasure? loadToken = null)
        {
            this.StartRangeNum = startToken;
            this.EndRangeNum = endToken;
            this.Load = loadToken.HasValue ? loadToken.Value : UnitOfMeasure.NaNValue;
            
            if (this.StartRangeNum.Sign > 0 && this.EndRangeNum.Sign < 0)
            {
                this.SlotsNum = (this.EndRangeNum - MinValue)
                                        + (MaxValue - this.StartRangeNum);
                this.WrapsRange = true;
            }
            else
            {
                this.SlotsNum = this.EndRangeNum - this.StartRangeNum;
            }
        }

        /// <summary>
        /// If true, this range wraps from a starting range that is positive but ends in a negative numeric value (e.g., from the maximum numeric value to the minimal numeric value).
        /// </summary>
        override public bool WrapsRange { get; }
        /// <summary>
        /// Start Token (exclusive)
        /// </summary>
        override public object StartRange { get { return this.StartRangeNum; } }
        public BigInteger StartRangeNum { get; }

        /// <summary>
        /// End Token (inclusive)
        /// </summary>
        override public object EndRange { get { return this.EndRangeNum; } } //End Token (inclusive)
        public BigInteger EndRangeNum { get; }

        /// <summary>
        /// The number of tokens (slots) that can be accommodated within this range
        /// </summary>
        override public object Slots { get { return this.SlotsNum; } }
        public BigInteger SlotsNum { get; }
        public override string SlotsFormatted()
        {
            return this.SlotsNum.ToString("###,###,###,###,###,###,###,###,###,###,###,###,##0");
        }

        /// <summary>
        /// The size/amount of data that is currently within this token range.
        /// </summary>
        override public UnitOfMeasure Load { get; }

        /// <summary>
        /// Checks to determine if checkRange is within this range.
        /// </summary>
        /// <param name="checkRange"></param>
        /// <returns></returns>
        override public bool IsWithinRange(TokenRangeInfo checkRange)
        {
            var numCheckRange = checkRange as TokenRangeBigInt;

            if (numCheckRange == null) return false;

            if (numCheckRange.EndRangeNum <= this.EndRangeNum)
            {
                if (numCheckRange.EndRangeNum > this.StartRangeNum)
                {
                    return true;
                }
            }

            if (numCheckRange.StartRangeNum > this.StartRangeNum)
            {
                if (numCheckRange.StartRangeNum < this.EndRangeNum)
                {
                    return true;
                }
            }

            return false;
        }

        override public bool IsWithinRange(object checkRange)
        {
            if (checkRange is BigInteger) return this.IsWithinRange((BigInteger)checkRange);

            return false;
        }

        public bool IsWithinRange(BigInteger checkRange)
        {
            if (checkRange <= this.EndRangeNum)
            {
                if (checkRange > this.StartRangeNum)
                {
                    return true;
                }
            }

            return false;
        }

        public bool Equals(TokenRangeBigInt other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;

            return this.EndRangeNum == other.EndRangeNum
                    && this.StartRangeNum == other.StartRangeNum;
        }

        override public bool Equals(TokenRangeInfo other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;
            if (other is TokenRangeBigInt) return this.Equals((TokenRangeBigInt)other);

            return false;
        }

    }
}
