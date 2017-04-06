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
    }
}
