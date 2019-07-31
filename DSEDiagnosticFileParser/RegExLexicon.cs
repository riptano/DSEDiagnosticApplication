using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticParamsSettings;

namespace DSEDiagnosticFileParser
{
    public sealed class RegExLexicon
    {
        public static readonly RegExLexicon Instance;

        public const string IdentiferStart = "$<";        
        public const string IdentiferEnd = ">";
        public const char IdentiferNot = '!';
        public const string KeyPrefix = "<"; //If null key is not added to new regex
        public const string KeySuffix = ">";

        public readonly KeyValuePair<string, string>[] KeysValues;

        static RegExLexicon()
        {
            Instance = new RegExLexicon(Helpers.ReadJsonFileIntoObject<KeyValuePair<string, string>[]>(Properties.Settings.Default.RegExLexiconValues));
        }

        public RegExLexicon(KeyValuePair<string,string>[] keysvalues)
        {
            this.KeysValues = keysvalues;
        }

        public RegExLexicon(IEnumerable<KeyValuePair<string, string>> keysvalues)
            : this(keysvalues.ToArray())
        {           
        }

        public string FindRegEx(string key)
        {
            return this.KeysValues.FirstOrDefault(a => a.Key == key).Value;
        }

        /// <summary>
        /// This will search the regular expression string for &quot;$&lt;keyname&gt;&quot; and find the corresponding value string and append that string right after the keyname occurrence. Note that the $ is removed. 
        /// Note thatif keyname begins with a &quot;!&quot;, the complete keyname expression is replaced with the value string. 
        /// If keyname is not found, the complete expression is ignored (not removed/replaced in the regex string). 
        /// </summary>
        /// <param name="regExpression"></param>
        /// <returns>
        /// The original regex string or the new modified string.
        /// </returns>
        public string FindReplaceRegEx(string regExpression)
        {
            if (string.IsNullOrEmpty(regExpression)) return regExpression;
            
            var startIdentifierPositions = new List<Tuple<int,int,string,string>>();
            
            for(int lstPos = 0; lstPos >= 0 || lstPos >= regExpression.Length;)
            {
                lstPos = regExpression.IndexOf(IdentiferStart, lstPos);
                                
                if(lstPos >= 0)
                {
                    var endIdPos = regExpression.IndexOf(IdentiferEnd, lstPos + IdentiferStart.Length);

                    if(endIdPos > 0)
                    {
                        var key = regExpression.Substring(lstPos + IdentiferStart.Length, endIdPos - lstPos - IdentiferEnd.Length - 1);
                        bool keyNotIncluded = false;

                        if(key.First() == IdentiferNot)
                        {
                            keyNotIncluded = true;
                            key = key.Substring(1);
                        }

                        var keyvalue = this.KeysValues.FirstOrDefault(a => a.Key == key);

                        if(keyvalue.Value == null)
                        {
                            ++lstPos;
                        }
                        else
                        {
                            var nextRegExPos = endIdPos + IdentiferEnd.Length;
                            
                            startIdentifierPositions.Add(new Tuple<int, int, string, string>(lstPos, nextRegExPos, keyNotIncluded ? null : key, keyvalue.Value));
                            lstPos = nextRegExPos;
                        }
                    }
                    else
                    {
                        ++lstPos;
                    }
                }
            }

            if(startIdentifierPositions.Count > 0)
            {
                string newRegEx = regExpression.Substring(0, startIdentifierPositions.First().Item1);
                Tuple<int, int, string, string> idPos;

                for (int idx = 0; idx < startIdentifierPositions.Count; ++idx)
                {
                    idPos = startIdentifierPositions[idx];

                    if(KeyPrefix != null && idPos.Item3 != null)
                    {
                        newRegEx += KeyPrefix;
                        newRegEx += idPos.Item3;
                        if (KeySuffix != null) newRegEx += KeySuffix;
                    }
                    newRegEx += idPos.Item4;

                    if(idx + 1 < startIdentifierPositions.Count)
                    {
                        newRegEx += regExpression.Substring(idPos.Item2, startIdentifierPositions[idx + 1].Item1 - idPos.Item2);
                    }
                    else
                    {
                        newRegEx += regExpression.Substring(idPos.Item2);
                    }
                }

                if(DSEDiagnosticLogger.Logger.Instance.IsDebugEnabled)
                {
                    DSEDiagnosticLogger.Logger.Instance.DebugFormat("RegEx replaced string \"{0}\"", newRegEx);
                }

                return newRegEx;
            }
            
            return regExpression;
        }

    }
}
