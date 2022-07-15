using Common;
using StackExchange.Redis;
using System;
using System.Linq;

namespace Services.CacheManager
{
    public class RedisCon
    {
        //private readonly Lazy<ConnectionMultiplexer> lazyLocal;
        private static readonly Lazy<ConnectionMultiplexer> lazyTrader;
        private readonly ConfigItems _config;
        //private static readonly Lazy<ConnectionMultiplexer> lazy1;
        //private static readonly Lazy<ConnectionMultiplexer> lazy2;
        //private static readonly Lazy<ConnectionMultiplexer> lazy3;
        //private static readonly Lazy<ConnectionMultiplexer> lazy4;

        static RedisCon()
        {
            //lazyLocal = new Lazy<ConnectionMultiplexer>(() =>
            //{
            //    var connection = ConnectionMultiplexer.Connect(new ConfigurationOptions
            //    {
            //        AbortOnConnectFail = false,
            //        EndPoints = { ConfigItems.RedisLocal },
            //        //Password = ConfigItems.RedisPasswordLocal,
            //        AllowAdmin = true
            //    });
            //    connection.PreserveAsyncOrder = false; // this should have to solve an issue with TIMEOUt
            //    return connection;
            //});
            lazyTrader = new Lazy<ConnectionMultiplexer>(() =>
            {
                var connection = ConnectionMultiplexer.Connect(new ConfigurationOptions
                {
                    AbortOnConnectFail = false,
                    EndPoints = { ConfigItems.RedisTrader },
                    //Password = ConfigItems.RedisPasswordGame,
                    AllowAdmin = true
                });
                connection.PreserveAsyncOrder = false; // should be on all connections... got it..
                return connection;
            });
        }

        public RedisCon(ConfigItems config)
        {
            _config = config;
        }

        //public ConnectionMultiplexer ConnLocal
        //{
        //    get
        //    {
        //        return lazyLocal.Value;
        //    }
        //}
        public ConnectionMultiplexer ConnTrader
        {
            get
            {
                return lazyTrader.Value;
            }
        }
        //public IServer ServerLocal
        //{
        //    get
        //    {
        //        return ConnLocal.GetEndPoints().Select(x =>
        //        {
        //            return ConnLocal.GetServer(x);
        //        }).FirstOrDefault();
        //    }
        //}
        public IServer ServerTrader
        {
            get
            {
                return ConnTrader.GetEndPoints().Select(x =>
                {
                    return ConnTrader.GetServer(x);
                }).FirstOrDefault();
            }
        }
        //public static ConnectionMultiplexer Redis1
    }
}