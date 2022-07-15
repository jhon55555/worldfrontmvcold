using Common;
using StackExchange.Redis;
using System;
using System.Linq;

namespace Services.RedisManager
{
    public class RedisConnectionFoot
    {

        private static readonly Lazy<ConnectionMultiplexer> lazyConnection;

        static RedisConnectionFoot()
        {
            lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
            {
                var connection = ConnectionMultiplexer.Connect(new ConfigurationOptions
                {
                    AbortOnConnectFail = false,
                    EndPoints = { ConfigItems.RedisServerFoot[0].Split('|')[0] },
                    KeepAlive = 4
                    //Ssl = true,
                    //Password = ConfigItems.RedisPasswordLocal,
                    //AllowAdmin = true
                });
                connection.PreserveAsyncOrder = false; // should be on all connections... got it..
                return connection;
                //var configurationOptions = new ConfigurationOptions
                //{
                //    AbortOnConnectFail = false
                //};
                //configurationOptions.EndPoints.Add(new DnsEndPoint(ConfigItems.RedisServer, 6379));
                ////configurationOptions.AllowAdmin = true;
                //return ConnectionMultiplexer.Connect(configurationOptions);
            });
        }

        public static ConnectionMultiplexer ConnectionLocal
        {
            get
            {
                return lazyConnection.Value;
            }
        }

        public static IServer ServerLocal
        {
            get
            {
                return ConnectionLocal.GetEndPoints().Select(x =>
                 {
                     return ConnectionLocal.GetServer(x);
                 }).FirstOrDefault();
            }
        }
    }
}