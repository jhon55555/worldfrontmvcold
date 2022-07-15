using Common;
using StackExchange.Redis;
using System;
using System.Linq;

namespace Services.RedisManager
{
    public class RedisConnectionTrader
    {

        private static readonly Lazy<ConnectionMultiplexer> lazyConnection;

        static RedisConnectionTrader()
        {
            lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
            {
                var connection = ConnectionMultiplexer.Connect(new ConfigurationOptions
                {
                    AbortOnConnectFail = false,
                    EndPoints = { ConfigItems.RedisTrader },
                    KeepAlive = 4
                    //Ssl = true,
                    //Password = ConfigItems.RedisPasswordLocal,
                    //AllowAdmin = true
                });
                connection.PreserveAsyncOrder = false; // should be on all connections... got it..
                return connection;
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