using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticInsights
{
    public static class MiscHelpers
    {

        #region Property

        public static IEnumerable<T> TryGetValues<T>(this IReadOnlyDictionary<string,dynamic> dict, string key)
        {
            if (dict != null)
            {
                return dict.TryGetValue(key, out dynamic value) ? ((IEnumerable<dynamic>)value).Cast<T>() : null;
            }

            return null;
        }
        
        public static T TryGetValue<T>(this IReadOnlyDictionary<string, dynamic> dict, string key)           
        {
            if (dict != null)
            {
                return dict.TryGetValue(key, out dynamic value) ? (T)value : default(T);
            }

            return default(T);
        }

        public static Nullable<T> TryGetStructValue<T>(this IReadOnlyDictionary<string, dynamic> dict, string key)
            where T : struct
        {
            if (dict != null)
            {               
                if(dict.TryGetValue(key, out dynamic value))
                {
                    if (value is string strValue)
                    {
                        return (T)(dynamic)DSEDiagnosticLibrary.StringHelpers.DetermineProperObjectFormat(strValue, removeNamespace: false, checkforDate: true);
                    }

                    return (T)value;                    
                }   
            }

            return null;
        }

        public static dynamic TryGetValue(this IReadOnlyDictionary<string, dynamic> dict, string key)
        {
            if (dict != null)
            {
                return dict.TryGetValue(key, out object value) ? value : null;
            }

            return null;
        }


        public static T TryGetValue<T>(this IReadOnlyDictionary<string, dynamic> dict, string key, ref T updateField)
        {            
            if (dict != null)
            {
                return updateField = dict.TryGetValue(key, out dynamic value) ? (T)value : default(T);
            }

            return default(T);
        }       

        public static bool NullSafeSet<TClass, TProperty>(this IReadOnlyDictionary<string, dynamic> dict, string key, TClass outObj, Expression<Func<TClass, TProperty>> outExpr)
        {
            if (dict != null)
            {
                var expr = (MemberExpression)outExpr.Body;
                var prop = (PropertyInfo)expr.Member;

                var dictValue = dict.TryGetValue(key);

                if (dictValue != null)
                {
                    prop.SetValue(outObj, dictValue, null);
                    return true;
                }
            }
            return false;
        }
               
        public static bool NullSafeSet<DictValue>(this IReadOnlyDictionary<string, dynamic> dict, string key, Action<DictValue> setOutput)
        {
            if (dict != null)
            {
                var dictValue = dict.TryGetValue<DictValue>(key);

                if (dictValue != null)
                {
                    setOutput((DictValue)dictValue);
                    return true;
                }
            }

            return false;
        }
       
        public static bool NullSafeSet<DictValue>(this IReadOnlyDictionary<string, dynamic> dict, string key, Action<DictValue> setOutput, Action onNullValue)
            where DictValue : struct
        {
            if (dict != null)
            {
                var dictValue = dict.TryGetValue(key);

                if (dictValue != null)
                {
                    DictValue setValue;

                    if(dictValue is string strValue)
                    {
                        setValue = (DictValue) (dynamic) DSEDiagnosticLibrary.StringHelpers.DetermineProperObjectFormat(strValue, removeNamespace: false, checkforDate: true);
                    }
                    else
                    {
                        setValue = (DictValue)dictValue;
                    }

                    setOutput(setValue);
                    return true;
                }
                else
                {
                    onNullValue?.Invoke();
                }
            }

            return false;
        }
        
        
        public static bool EmptySafeSet<DictValue>(this IReadOnlyDictionary<string, dynamic> dict, string key, DictValue inputValue, Action<DictValue> setOutput)
            where DictValue : class
        {
            if (dict != null)
            {
                if (inputValue == null)
                {
                    return dict.NullSafeSet<DictValue>(key, setOutput);
                }
            }

            return false;
        }

        public static bool EmptySafeSet(this IReadOnlyDictionary<string, dynamic> dict, string key, string inputValue, Action<string> setOutput)
        {
            if (dict != null)
            {
                if (string.IsNullOrEmpty(inputValue))
                {
                    return dict.NullSafeSet<string>(key, setOutput);
                }
            }

            return false;
        }
        
        public static bool EmptySafeSet<DValue>(this IReadOnlyDictionary<string, dynamic> dict, string key, Nullable<DValue> inputValue, Action<DValue> setOutput)
            where DValue : struct
        {
            if (dict != null)
            {
                if (!inputValue.HasValue)
                {
                    return dict.NullSafeSet<DValue>(key, setOutput, null);
                }
            }

            return false;
        }
        #endregion

    }
}
