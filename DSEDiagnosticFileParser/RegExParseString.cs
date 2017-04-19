using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

namespace DSEDiagnosticFileParser
{
    public sealed class RegExParseString
    {
        private readonly Regex[] _compiledRegEx = null;

        private RegExParseString() { }

        public RegExParseString(params string[] regExStrings)
        {
            this.RegExStrings = regExStrings;
            if (this._compiledRegEx == null || this._compiledRegEx.Length != this.RegExStrings.Length)
            {
                this._compiledRegEx = this.RegExStrings.Select(s => new Regex(s, RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled))
                                                        .ToArray();
            }
        }

        public readonly string[] RegExStrings;

        public string[] Split(string input, int regexIndex = 0)
        {
            return this._compiledRegEx[regexIndex].Split(input);
        }

        public Match Match(string input, int regexIndex = 0)
        {
            return this._compiledRegEx[regexIndex].Match(input);
        }

        public bool IsMatch(string input, int regexIndex = 0)
        {
            return this._compiledRegEx[regexIndex].IsMatch(input);
        }

        /// <summary>
        /// Returns the first success match or null
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public Match MatchFirstSuccessful(string input)
        {
            Match result = null;
            foreach (var regex in this._compiledRegEx)
            {
                result = regex.Match(input);

                if(result.Success)
                {
                    return result;
                }
            }

            return null;
        }

        /// <summary>
        /// If match successful the returned value is the match instance and the regEx value would be the Regex Instance.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="regEx">
        /// if no matches found, null. Otherwise the Regex instance.
        /// </param>
        /// <returns>
        /// If a successful match; the match instance, otherwise null.
        /// </returns>
        public Match MatchFirstSuccessful(string input, out Regex regEx)
        {
            Match result = null;
            for(int nIdx = 0; nIdx < this._compiledRegEx.Length; ++nIdx)
            {
                result = this._compiledRegEx[nIdx].Match(input);

                if (result.Success)
                {
                    regEx = this._compiledRegEx[nIdx];
                    return result;
                }
            }

            regEx = null;
            return null;
        }

        /// <summary>
        /// Returns true if any of the RegEx stings match.
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public bool IsMatchAny(string input)
        {
            return this._compiledRegEx.Any(m => m.IsMatch(input));
        }

        public Regex GetRegEx(int regexIndex = 0)
        {
            return this._compiledRegEx[regexIndex];
        }
    }
}
