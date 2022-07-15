using Common;
using StackExchange.Redis;
using System;
using System.Linq;

namespace Services.RedisManagerOpen
{
    public  class RedisCon
    {
        private static readonly Lazy<ConnectionMultiplexer> lazyLocal;

         static RedisCon()
        {
            lazyLocal = new Lazy<ConnectionMultiplexer>(() =>
            {
                var connection = ConnectionMultiplexer.Connect(new ConfigurationOptions
                {
                    AbortOnConnectFail = false,
                    EndPoints = { ConfigItems.RedisLocal },
                    //Password = ConfigItems.RedisPasswordLocal,
                    AllowAdmin = true
                });
                connection.PreserveAsyncOrder = false; // should be on all connections... got it..
                return connection;
            });
        }

        public static ConnectionMultiplexer ConnLocal
        {
            get
            {
                return lazyLocal.Value;
            }
        }
        public static IServer ServerLocal
        {
            get
            {
                return ConnLocal.GetEndPoints().Select(x =>
                {
                    return ConnLocal.GetServer(x);
                }).FirstOrDefault();
            }
        }
    }
}