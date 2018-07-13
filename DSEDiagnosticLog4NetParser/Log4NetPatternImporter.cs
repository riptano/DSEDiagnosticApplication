using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.IO;

namespace DSEDiagnosticLog4NetParser
{
    public class Log4NetPatternImporter
    {
        readonly static bool IgnoreNewLineExpression = true;

        StringBuilder _lineRegExStr = new StringBuilder();
        Dictionary<string, int> _captureCounters = new Dictionary<string, int>();

        public class OutputField
        {
            public StringBuilder Code = new StringBuilder();
            public string CodeType;
        };
        Dictionary<string, OutputField> _outputFields = new Dictionary<string, OutputField>();
        //CodeDomProvider _csharpProvider;

        public static IDictionary<string, OutputField> GenerateRegularGrammarElement(string pattern)
        {
            var elements = new Dictionary<string, OutputField>();
            Log4NetPatternImporter obj = new Log4NetPatternImporter();

            obj.GenerateRegularGrammarElementInt(elements, pattern);

            return elements;
        }

        public Log4NetPatternImporter()
        {
            this._lineRegExStr.AppendLine();
        }

        void GenerateRegularGrammarElementInt(Dictionary<string, OutputField> elements, string pattern)
        {
            var patternTokens = this.TokenizePattern(pattern);
            var lstItem = patternTokens.LastOrDefault();
            var lstIdx = patternTokens.Count() - 1;
            int nIdx = 0;

            if (lstItem.Value == "n" || lstItem.Value == "newline")
            {
                lstIdx -= 1;
            }

            foreach (PatternToken t in patternTokens)
            {
                if (t.Type == PatternTokenType.Specifier)
                    this.HandleSpecifier(t, lstIdx == nIdx);
                else
                    this.HandleText(t);

                ++nIdx;
            }
            this.WriteRegularGrammarElement(elements);
        }

        public enum PatternTokenType
        {
            None, Specifier, Text
        };

        [DebuggerDisplay("{Type} '{Value}'")]
        public struct PatternToken
        {
            public PatternTokenType Type;
            public string Value;
            public bool RightPaddingFlag;
            public int Padding;
            public int Trim;
            public string Argument;
        };

        // Regexp to match a conversion specifier in a pattern.
        // Note: all single letter specifiers (like a, c) go after longer specifiers
        // starting from those letters (appdomain, class).
        // This is importatnt because regexp would match only first letters otherwise.
        static readonly Regex patternParserRe = new Regex(
            @"\%(?:(\%)|(?:(\-?)(\d+))?(?:\.(\d+))?(appdomain|a|class|c|C|date|exception|file|F|identity|location|level|line|logger|l|L|message|msg|mdc|method|m|M|newline|ndc|n|properties|property|p|P|r|timestamp|thread|type|t|username|utcdate|u|w|x|X|d)(?:\{([^\}]+)\})?)",
                RegexOptions.Compiled);

        public IEnumerable<PatternToken> TokenizePattern(string pattern)
        {
            int idx = 0; // Current position in the pattern

            for (;;)
            {
                Match m = patternParserRe.Match(pattern, idx);

                if (m.Success)
                {
                    // A specifier is found

                    if (m.Index > idx) // Yield the text before the specifier found (if any)
                    {
                        PatternToken txt1 = new PatternToken();
                        txt1.Type = PatternTokenType.Text;
                        txt1.Value = pattern.Substring(idx, m.Index - idx);
                        yield return txt1;
                    }

                    if (m.Groups[1].Value != "") // If %% was found, yield single '%'
                    {
                        PatternToken txt2 = new PatternToken();
                        txt2.Type = PatternTokenType.Text;
                        txt2.Value = "%";
                        yield return txt2;
                    }
                    else // A 'normal' specifier was found. Fill up the structure and yield it.
                    {
                        PatternToken spec;
                        spec.Type = PatternTokenType.Specifier;
                        spec.RightPaddingFlag = m.Groups[2].Value != "";
                        spec.Padding = m.Groups[3].Value != "" ? int.Parse(m.Groups[3].Value) : 0;
                        spec.Trim = m.Groups[4].Value != "" ? int.Parse(m.Groups[4].Value) : 0;
                        spec.Value = m.Groups[5].Value;
                        spec.Argument = m.Groups[6].Value;
                        yield return spec;
                    }

                    // Move current position to the end of the specifier found
                    idx = m.Index + m.Length;
                }
                else
                {
                    if (idx < pattern.Length) // Yield the rest of the pattern if any
                    {
                        PatternToken txt3 = new PatternToken();
                        txt3.Type = PatternTokenType.Text;
                        txt3.Value = pattern.Substring(idx);
                        yield return txt3;
                    }

                    // Stop parsing
                    break;
                }
            }
        }

        OutputField GetOutputField(string name)
        {
            OutputField ret;
            if (!this._outputFields.TryGetValue(name, out ret))
                this._outputFields[name] = ret = new OutputField();
            return ret;
        }

        void ConcatToBody(string code)
        {
            OutputField f = GetOutputField("Body");
            if (f.Code.Length != 0)
                f.Code.Append(" + ");
            f.Code.Append(code);
        }

        void HandleText(PatternToken t)
        {
            StringBuilder reToAppend = this._lineRegExStr;

            reToAppend.AppendFormat("{0} # fixed text '{1}'{2}", Regex.Escape(t.Value), t.Value, Environment.NewLine);

            //this.ConcatToBody(GetCSharpStringLiteral(t.Value));
            this.ConcatToBody(t.Value);
        }

        string GetDateRe(string format)
        {
            StringBuilder re = new StringBuilder();
            re.AppendLine();
            foreach (string t in this.TokenizeDatePattern(format))
            {
                switch (t)
                {
                    case "d":
                        re.AppendLine(@"  \d{1,2} # day of the month");
                        break;
                    case "dd":
                        re.AppendLine(@"  \d{2} # day of the month");
                        break;
                    case "ddd":
                    case "dddd":
                        re.AppendLine(@"  \w+ # name of the day");
                        break;
                    case "f":
                    case "ff":
                    case "fff":
                    case "ffff":
                    case "fffff":
                    case "ffffff":
                    case "fffffff":
                        re.AppendFormat(@"  \d{0}{1}{2} # the most significant digits of the seconds fraction{3}", "{", t.Length, "}", Environment.NewLine);
                        break;
                    case "F":
                    case "FF":
                    case "FFF":
                    case "FFFF":
                    case "FFFFF":
                    case "FFFFFF":
                    case "FFFFFFF":
                        re.AppendFormat(@"  (\d{0}{1}{2})? # the most significant digits of the seconds fraction (no trailing zeros){3}", "{", t.Length, "}", Environment.NewLine);
                        break;
                    case "g":
                    case "gg":
                        re.AppendLine(@"  \.+ # the era");
                        break;
                    case "h":
                    case "H":
                        re.AppendLine(@"  \d{1,2} # hours");
                        break;
                    case "hh":
                    case "HH":
                        re.AppendLine(@"  \d{2} # hours");
                        break;
                    case "m":
                        re.AppendLine(@"  \d{1,2} # minutes");
                        break;
                    case "mm":
                        re.AppendLine(@"  \d{2} # minutes");
                        break;
                    case "M":
                        re.AppendLine(@"  \d{1,2} # month");
                        break;
                    case "MM":
                        re.AppendLine(@"  \d{2} # month");
                        break;
                    case "MMM":
                    case "MMMM":
                        re.AppendLine(@"  \w+ # name of month");
                        break;
                    case "s":
                        re.AppendLine(@"  \d{1,2} # seconds");
                        break;
                    case "ss":
                        re.AppendLine(@"  \d{2} # seconds");
                        break;
                    case "t":
                        re.AppendLine(@"  \w # the first character of the A.M./P.M. designator");
                        break;
                    case "tt":
                        re.AppendLine(@"  \w+ # A.M./P.M. designator");
                        break;
                    case "y":
                        re.AppendLine(@"  \d{1,2} # year");
                        break;
                    case "yy":
                        re.AppendLine(@"  \d{2} # year");
                        break;
                    case "yyyy":
                        re.AppendLine(@"  \d{4} # year");
                        break;
                    case "z":
                        re.AppendLine(@"  [\+\-]\d{1,2} # time zone offset");
                        break;
                    case "zz":
                        re.AppendLine(@"  [\+\-]\d{2} # time zone offset");
                        break;
                    case "zzz":
                        re.AppendLine(@"  [\+\-]\d{2}\:\d{2} # time zone offset");
                        break;
                    default:
                        re.AppendFormat("  {0} # fixed string '{1}'{2}", Regex.Escape(t), t, Environment.NewLine);
                        break;
                }
            }
            return re.ToString();
        }

        static readonly Regex dateParserRe = new Regex(@"
		(
			(dddd+)|ddd|dd|d| # day of the month
			fffffff|ffffff|fffff|ffff|fff|ff|f| # the N most significant digits of the seconds fraction
			FFFFFFF|FFFFFF|FFFFF|FFFF|FFF|FF|F| # the N most significant digits of the seconds fraction, no trailing zeros
			(gg+)|g| # the era
			(hh+)|h| # 1-12 hour
			(HH+)|h| # 0-24 hour
			(mm+)|m| # minutes
			MMMM|MMM|MM|M| # month
			(ss+)|s| # seconds
			(tt+)|t| # A.M./P.M. designator
			yyyy|yy|y| # year
			(zzz+)|zz|z| # time zone offset 
			\:| # time separator
			\/| # date separator
			\\(\.) # escape character
		)
		", RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled);

        IEnumerable<string> TokenizeDatePattern(string pattern)
        {
            int idx = 0;
            for (;;)
            {
                Match m = dateParserRe.Match(pattern, idx);
                if (m.Success)
                {
                    if (m.Index > idx) // Yield the text before the specifier found (if any)
                    {
                        yield return pattern.Substring(idx, m.Index - idx);
                    }

                    if (m.Groups[2].Value != "")
                        yield return "dddd";
                    else if (m.Groups[3].Value != "")
                        yield return "gg";
                    else if (m.Groups[4].Value != "")
                        yield return "hh";
                    else if (m.Groups[5].Value != "")
                        yield return "HH";
                    else if (m.Groups[6].Value != "")
                        yield return "mm";
                    else if (m.Groups[7].Value != "")
                        yield return "ss";
                    else if (m.Groups[8].Value != "")
                        yield return "tt";
                    else if (m.Groups[9].Value != "")
                        yield return "zzz";
                    else if (m.Groups[10].Value != "")
                        yield return m.Groups[10].Value;
                    else
                        yield return m.Groups[1].Value;

                    idx = m.Index + m.Length;
                }
                else
                {
                    if (idx < pattern.Length) // Yield the rest of the pattern if any
                    {
                        yield return pattern.Substring(idx);
                    }

                    // Stop parsing
                    break;
                }
            }
        }

        void HandleSpecifier(PatternToken t, bool lastToken)
        {
            string captureName;
            string re;
            string outputFieldName = null;
            string outputFieldCode = null;
            string outputFieldCodeType = "string";

            switch (t.Value)
            {
                case "a":
                case "appdomain":
                    captureName = "AppDomain";
                    re = lastToken ? @".*" : @".*?";
                    break;
                case "c":
                case "logger":
                    captureName = "Logger";
                    re = @"[\w\.]+";
                    break;
                case "C":
                case "class":
                case "type":
                    captureName = "Class";
                    re = lastToken ? @".*" : @".*?"; // * because caller location might not be accessible
                    break;
                case "d":
                case "date":
                case "utcdate":
                    captureName = "Time";
                    string fmt;
                    switch (t.Argument)
                    {
                        case "ABSOLUTE":
                            fmt = "HH:mm:ss,fff";
                            break;
                        case "DATE":
                            fmt = "dd MMM yyyy HH:mm:ss,fff";
                            break;
                        case "ISO8601":
                        case "":
                            fmt = "yyyy-MM-dd HH:mm:ss,fff";
                            break;
                        default:
                            fmt = t.Argument;
                            break;
                    }
                    re = GetDateRe(fmt);
                    outputFieldName = "Time";
                    outputFieldCode = fmt;
                    outputFieldCodeType = "DateTime";
                    break;
                case "newline":
                case "n":
                    captureName = "NL";
                    re = @"\n|\r\n";
                    outputFieldCode = Environment.NewLine;
                    outputFieldName = "NL";
                    break;
                case "exception":
                    captureName = "Exception";
                    re = lastToken ? @".*" : @".*?";
                    break;
                case "file":
                case "F":
                    captureName = "File";
                    // insert " with Format() because otherwise I would have to remove @ and escape the re heavily
                    re = string.Format(@"[^\*\?\{0}\<\>\|]*{1}", '"', lastToken ? string.Empty : "?"); // [...]* because caller location might not be accessible
                    break;
                case "identity":
                case "u":
                    captureName = "Identity";
                    re = @"[\w\\\.]*";
                    break;
                case "location":
                case "l":
                    captureName = "Location";
                    re = lastToken ? @".*" : @".*?"; // * because caller location might not be accessible
                    break;
                case "line":
                case "L":
                    captureName = "Line";
                    re = @"\d*"; // * because caller location might not be accessible
                    outputFieldCodeType = "int";
                    outputFieldName = "Line";
                    break;
                case "p":
                case "level":
                    captureName = "Level";
                    re = @"DEBUG|INFO|WARN|ERROR|FATAL";
                    break;
                case "message":
                case "msg":
                case "m":
                    captureName = "Message";
                    re = lastToken ? ".*" : ".*?";
                    break;
                case "method":
                case "M":
                    captureName = "Method";
                    re = lastToken ? ".*" : ".*?";
                    break;
                case "mdc":
                case "property":
                case "P":
                case "X":
                case "properties":
                    captureName = "Prop";
                    re = lastToken ? ".*" : ".*?";
                    break;
                case "r":
                case "timestamp":
                    captureName = "Timestamp";
                    re = @"\d+";
                    outputFieldCodeType = "long";
                    outputFieldName = "Timestamp";
                    break;
                case "thread":
                case "t":
                    captureName = "Thread";
                    re = lastToken ? ".+" : ".+?";
                    break;
                case "username":
                case "w":
                    captureName = "User";
                    re = @"[\w\\\.]+";
                    break;
                case "x":
                case "ndc":
                    captureName = "NDC";
                    re = lastToken ? @".+" : @".+?";
                    break;
                default:
                    return;
            }

            if (IgnoreNewLineExpression && captureName == "NL") return;

            captureName = this.RegisterCaptureName(captureName);

            StringBuilder reToAppend = this._lineRegExStr;

            string paddingString = null;
            if (t.Padding != 0)
                paddingString = string.Format(@"\ {{0,{0}}}", t.Padding);

            if (paddingString != null && !t.RightPaddingFlag)
                reToAppend.Append(paddingString);

            reToAppend.AppendFormat("(?<{0}>{1})", captureName, re);

            if (paddingString != null && t.RightPaddingFlag)
                reToAppend.Append(paddingString);

            reToAppend.AppendFormat(" # field '{0}'{1}", t.Value, Environment.NewLine);

            if (outputFieldName != null)
            {
                OutputField f = this.GetOutputField(outputFieldName);

                // First, check is the code is empty yet.
                // The code may have already been initialized. That may happen if
                // there are several specifiers with the same name and that produce
                // an output field. We want to take in use only the first such specifier.
                if (f.Code.Length == 0)
                {
                    f.Code.Append(outputFieldCode);
                    f.CodeType = outputFieldCodeType;

                    if (outputFieldName == "Time")
                        return;
                }
            }
            this.ConcatToBody(captureName);
        }

        string RegisterCaptureName(string captureName)
        {
            int captureCounter;
            if (!this._captureCounters.TryGetValue(captureName, out captureCounter))
                captureCounter = 0;
            captureCounter++;
            this._captureCounters[captureName] = captureCounter;

            if (captureCounter > 1)
                captureName += captureCounter;

            return captureName;
        }

        void WriteRegularGrammarElement(Dictionary<string, OutputField> elements)
        {
            elements.Add("LineRegEx", new OutputField() { Code = this._lineRegExStr, CodeType = "RegEx" });
            WriteFields(elements);
        }

        void WriteFields(Dictionary<string, OutputField> elements)
        {
            foreach (KeyValuePair<string, OutputField> f in this._outputFields)
            {
                elements.Add(f.Key, f.Value);
            }
        }
    }


}
