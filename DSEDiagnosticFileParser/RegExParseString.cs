using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Runtime.CompilerServices;

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
                this._compiledRegEx = this.RegExStrings
                                                        .Select(s => LibrarySettings.RegExLexiconValues.FindReplaceRegEx(s))
                                                        .Select(s => new Regex(s, RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled))
                                                        .ToArray();
            }
        }

        public readonly string[] RegExStrings;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string[] Split(string input, int regexIndex = 0)
        {
            return this._compiledRegEx[regexIndex].Split(input);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Match Match(string input, int regexIndex = 0)
        {
            return this._compiledRegEx[regexIndex].Match(input);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsMatch(string input, int regexIndex = 0)
        {
            return this._compiledRegEx[regexIndex].IsMatch(input);
        }

        /// <summary>
        /// Returns the first success match or null
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Match MatchFirstSuccessful(string input, out Regex regEx)
        {
            Match result = null;
            foreach(var regexTry in this._compiledRegEx)
            {
                result = regexTry.Match(input);

                if (result.Success)
                {
                    regEx = regexTry;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsMatchAny(string input)
        {
            return this._compiledRegEx.Any(m => m.IsMatch(input));
        }

        /// <summary>
        /// Returns true if any of the RegEx stings match.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="regEx">
        /// if no matches found, null. Otherwise the Regex instance.
        /// </param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsMatchAny(string input, out Regex regEx)
        {
            return (regEx = this._compiledRegEx.FirstOrDefault(m => m.IsMatch(input))) != null;
        }

        /// <summary>
        /// If input matches any of the RegEx items, the replace is created using the replacementString.
        ///
        /// Note that if the replacement string starts with &quot;^&quot;, the replaced string from the RegEx is scanned for ^ and all chars before that position are removed.
        /// This is useful if the RegEx scans from the end of the string or does not consume the entire string. When the replace is executed the beginning chars might be appended on the replace string.
        /// Note that if the replacement string ends with &quot;$&quot;, the replaced string from the RegEx is scanned for $ and all chars after that position are removed.
        /// This is useful if the RegEx scans does not consume the entire string. When the replace is executed those ending chars might be appended on the end of replace string.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="replacementString"></param>
        /// <returns>
        /// A new string based on the replacementString and the RegEx. Null if no regEx items are matched.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string IsMatchAnyAndReplace(string input, string replacementString)
        {
            foreach (var regexTry in this._compiledRegEx)
            {
                if (regexTry.IsMatch(input))
                {
                    var replacedStr = regexTry.Replace(input, replacementString);

                    if(replacementString[0] == '^')
                    {
                        var ignoreBeforePos = replacedStr.IndexOf('^');

                        if(ignoreBeforePos >= 0)
                        {
                            replacedStr = replacedStr.Substring(ignoreBeforePos + 1);
                        }
                    }
                    if (replacementString.Last() == '$')
                    {
                        var ignoreAfterPos = replacedStr.LastIndexOf('$');

                        if (ignoreAfterPos >= 0)
                        {
                            replacedStr = replacedStr.Substring(0,ignoreAfterPos);
                        }
                    }

                    return replacedStr;
                }
            }

            return null;
        }

        /// <summary>
        /// If input matches any of the RegEx items, replacementEvaluator is called used t create the new replacement string.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="replacementEvaluator"></param>
        /// <returns>
        /// A new string based on the replacementEvaluator and the RegEx. Null if no regEx items are matched.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string IsMatchAnyAndReplace(string input, MatchEvaluator replacementEvaluator)
        {
            foreach (var regexTry in this._compiledRegEx)
            {
                if (regexTry.IsMatch(input))
                {
                    return regexTry.Replace(input, replacementEvaluator);
                }
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Regex GetRegEx(int regexIndex = 0)
        {
            return this._compiledRegEx[regexIndex];
        }

        private string _toStringCache = null;
        public override string ToString()
        {
            return this._toStringCache == null ? this._toStringCache = Newtonsoft.Json.JsonConvert.SerializeObject(this) : this._toStringCache;
        }
    }
}
