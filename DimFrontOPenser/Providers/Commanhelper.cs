using Common;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Web;
using System.Web.Http;

namespace DimFront.Providers
{
    public class Commanhelper
    {
        public static string GetIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return ip.ToString();
                }
            }
            throw new Exception("No network adapters with an IPv4 address in the system!");
        }

        public static bool CheckWords1<T>(T typeObject) where T : class
        {
            Type type = typeof(T);
            PropertyInfo[] properties = type.GetProperties();
            foreach (PropertyInfo property in properties)
            {
                if (property.GetValue(typeObject, null) != null && ConfigItems.ExWords.Contains(property.GetValue(typeObject, null).ToString().ToLower()))
                {
                    return false;
                }
            }
            return true;
        }

        
    }
}