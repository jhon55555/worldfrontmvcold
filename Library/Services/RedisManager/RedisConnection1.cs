using Common;
using StackExchange.Redis;
using System;
using System.Linq;

namespace Services.RedisManage
{
    public class RedisConnection1
    {

        private readonly Lazy<ConnectionMultiplexer> lazyConnLocal;
        private readonly Lazy<ConnectionMultiplexer> lazyConncric;
        //private readonly Lazy<ConnectionMultiplexer> lazyConncric1;
        //private readonly Lazy<ConnectionMultiplexer> lazyConncric2;
        //private readonly Lazy<ConnectionMultiplexer> lazyConncric3;
        //private readonly Lazy<ConnectionMultiplexer> lazyConncric4;
        //private readonly Lazy<ConnectionMultiplexer> lazyConncric5;
        //private readonly Lazy<ConnectionMultiplexer> lazyConncric6;
        private readonly Lazy<ConnectionMultiplexer> lazyConnfoot;
        private readonly Lazy<ConnectionMultiplexer> lazyConntenni;
        private readonly Lazy<ConnectionMultiplexer> lazyConnother;
        private readonly ConfigItems _config;

        public RedisConnection1(ConfigItems config)
        {
            _config = config;
            /*lazyConnLocal = new Lazy<ConnectionMultiplexer>(() =>
            {
                var connection = ConnectionMultiplexer.Connect(new ConfigurationOptions
                {

                    AbortOnConnectFail = false,
                    EndPoints = { ConfigItems.RedisServerSta },
                    //Ssl = true,
                    //Password = ConfigItems.RedisPasswordLocal,
                    AllowAdmin = true,
                    KeepAlive=4
                });
                connection.PreserveAsyncOrder = false; // should be on all connections... got it..
                return connection;
            });
            lazyConncric = new Lazy<ConnectionMultiplexer>(() =>
            {
                //ConfigItems.Redd = "";
                var connection = ConnectionMultiplexer.Connect(new ConfigurationOptions
                {

                    AbortOnConnectFail = false,
                    EndPoints = { ConfigItems.RedisServerSta1[0].Split('|')[0] },
                    //Ssl = true,
                    //Password = ConfigItems.RedisPasswordLocal,
                    AllowAdmin = true,
                    KeepAlive = 4

                });
                connection.PreserveAsyncOrder = false; // should be on all connections... got it..
                return connection;
            });
            //lazyConncric1 = new Lazy<ConnectionMultiplexer>(() =>
            //{
            //    //ConfigItems.Redd = "";
            //    return ConnectionMultiplexer.Connect(new ConfigurationOptions
            //    {

            //        AbortOnConnectFail = false,
            //        EndPoints = { ConfigItems.RedisServerSta1[1].Split('|')[0] },
            //        //Ssl = true,
            //        //Password = ConfigItems.RedisPasswordLocal,
            //        AllowAdmin = true
            //    });
            //});
            //lazyConncric2 = new Lazy<ConnectionMultiplexer>(() =>
            //{
            //    //ConfigItems.Redd = "";
            //    return ConnectionMultiplexer.Connect(new ConfigurationOptions
            //    {

            //        AbortOnConnectFail = false,
            //        EndPoints = { ConfigItems.RedisServerSta1[2].Split('|')[0] },
            //        //Ssl = true,
            //        //Password = ConfigItems.RedisPasswordLocal,
            //        AllowAdmin = true
            //    });
            //});
            //lazyConncric3 = new Lazy<ConnectionMultiplexer>(() =>
            //{
            //    //ConfigItems.Redd = "";
            //    return ConnectionMultiplexer.Connect(new ConfigurationOptions
            //    {

            //        AbortOnConnectFail = false,
            //        EndPoints = { ConfigItems.RedisServerSta1[3].Split('|')[0] },
            //        //Ssl = true,
            //        //Password = ConfigItems.RedisPasswordLocal,
            //        AllowAdmin = true
            //    });
            //});
            //lazyConncric4 = new Lazy<ConnectionMultiplexer>(() =>
            //{
            //    //ConfigItems.Redd = "";
            //    return ConnectionMultiplexer.Connect(new ConfigurationOptions
            //    {

            //        AbortOnConnectFail = false,
            //        EndPoints = { ConfigItems.RedisServerSta1[4].Split('|')[0] },
            //        //Ssl = true,
            //        //Password = ConfigItems.RedisPasswordLocal,
            //        AllowAdmin = true
            //    });
            //});
            //lazyConncric5 = new Lazy<ConnectionMultiplexer>(() =>
            //{
            //    //ConfigItems.Redd = "";
            //    return ConnectionMultiplexer.Connect(new ConfigurationOptions
            //    {

            //        AbortOnConnectFail = false,
            //        EndPoints = { ConfigItems.RedisServerSta1[5].Split('|')[0] },
            //        //Ssl = true,
            //        //Password = ConfigItems.RedisPasswordLocal,
            //        AllowAdmin = true
            //    });
            //});
            //lazyConncric6 = new Lazy<ConnectionMultiplexer>(() =>
            //{
            //    //ConfigItems.Redd = "";
            //    return ConnectionMultiplexer.Connect(new ConfigurationOptions
            //    {

            //        AbortOnConnectFail = false,
            //        EndPoints = { ConfigItems.RedisServerSta1[6].Split('|')[0] },
            //        //Ssl = true,
            //        //Password = ConfigItems.RedisPasswordLocal,
            //        AllowAdmin = true
            //    });
            //});
            lazyConnfoot = new Lazy<ConnectionMultiplexer>(() =>
            {
                //ConfigItems.Redd = "";
                var connection = ConnectionMultiplexer.Connect(new ConfigurationOptions
                {

                    AbortOnConnectFail = false,
                    EndPoints = { ConfigItems.RedisServerSta1[7].Split('|')[0] },
                    //Ssl = true,
                    //Password = ConfigItems.RedisPasswordLocal,
                    AllowAdmin = true,
                    KeepAlive = 4
                });
                connection.PreserveAsyncOrder = false; // should be on all connections... got it..
                return connection;
            });
            lazyConntenni = new Lazy<ConnectionMultiplexer>(() =>
            {
                //ConfigItems.Redd = "";
                var connection = ConnectionMultiplexer.Connect(new ConfigurationOptions
                {

                    AbortOnConnectFail = false,
                    EndPoints = { ConfigItems.RedisServerSta1[8].Split('|')[0] },
                    //Ssl = true,
                    //Password = ConfigItems.RedisPasswordLocal,
                    AllowAdmin = true,
                    KeepAlive = 4
                });
                connection.PreserveAsyncOrder = false; // should be on all connections... got it..
                return connection;
            });
            lazyConnother = new Lazy<ConnectionMultiplexer>(() =>
            {
                //ConfigItems.Redd = "";
                var connection = ConnectionMultiplexer.Connect(new ConfigurationOptions
                {

                    AbortOnConnectFail = false,
                    EndPoints = { ConfigItems.RedisServerSta1[9].Split('|')[0] },
                    //Ssl = true,
                    //Password = ConfigItems.RedisPasswordLocal,
                    AllowAdmin = true,
                    KeepAlive = 4
                });
                connection.PreserveAsyncOrder = false; // should be on all connections... got it..
                return connection;
            });
            //lazyConnLocalDyn = new Lazy<ConnectionMultiplexer>(() =>
            //{
            //    return ConnectionMultiplexer.Connect(new ConfigurationOptions
            //    {
            //        AbortOnConnectFail = false,
            //        EndPoints = { ConfigItems.Redd }
            //    });
            //});
            //lazyConnOdds = new Lazy<ConnectionMultiplexer>(() =>
            //{
            //    return ConnectionMultiplexer.Connect(new ConfigurationOptions
            //    {
            //        AbortOnConnectFail = false,
            //        EndPoints = { ConfigItems.RedisOdds },
            //        //Ssl = true,
            //        //Password = ConfigItems.RedisPasswordLocal,
            //        //AllowAdmin = true
            //    });
            //});*/

        }

        public ConnectionMultiplexer ConnLocal
        {
            get
            {
                return lazyConnLocal.Value;
            }
        }
        public ConnectionMultiplexer Conncric
        {
            get
            {
                return lazyConncric.Value;
            }
        }
        //public ConnectionMultiplexer Conncric1
        //{
        //    get
        //    {
        //        return lazyConncric1.Value;
        //    }
        //}
        //public ConnectionMultiplexer Conncric2
        //{
        //    get
        //    {
        //        return lazyConncric2.Value;
        //    }
        //}
        //public ConnectionMultiplexer Conncric3
        //{
        //    get
        //    {
        //        return lazyConncric3.Value;
        //    }
        //}
        //public ConnectionMultiplexer Conncric4
        //{
        //    get
        //    {
        //        return lazyConncric4.Value;
        //    }
        //}
        //public ConnectionMultiplexer Conncric5
        //{
        //    get
        //    {
        //        return lazyConncric5.Value;
        //    }
        //}
        //public ConnectionMultiplexer Conncric6
        //{
        //    get
        //    {
        //        return lazyConncric6.Value;
        //    }
        //}
        public ConnectionMultiplexer Connfoot
        {
            get
            {
                return lazyConnfoot.Value;
            }
        }
        public ConnectionMultiplexer Conntenni
        {
            get
            {
                return lazyConntenni.Value;
            }
        }
        public ConnectionMultiplexer Connother
        {
            get
            {
                return lazyConnother.Value;
            }
        }
        //public static ConnectionMultiplexer ConnOdds
        //{
        //    get
        //    {
        //        return lazyConnOdds.Value;
        //    }
        //}
        //public static ConnectionMultiplexer ConnLocalDyn
        //{
        //    get
        //    {
        //        return lazyConnLocalDyn.Value;
        //    }
        //}

        public IServer ServerLocal
        {
            get
            {
                return ConnLocal.GetEndPoints().Select(x =>
                {
                    return ConnLocal.GetServer(x);
                }).FirstOrDefault();
            }
        }
        public IServer Servercric
        {
            get
            {
                return Conncric.GetEndPoints().Select(x =>
                {
                    return Conncric.GetServer(x);
                }).FirstOrDefault();
            }
        }
        //public IServer Servercric1
        //{
        //    get
        //    {
        //        return Conncric1.GetEndPoints().Select(x =>
        //        {
        //            return Conncric1.GetServer(x);
        //        }).FirstOrDefault();
        //    }
        //}
        //public IServer Servercric2
        //{
        //    get
        //    {
        //        return Conncric2.GetEndPoints().Select(x =>
        //        {
        //            return Conncric2.GetServer(x);
        //        }).FirstOrDefault();
        //    }
        //}
        //public IServer Servercric3
        //{
        //    get
        //    {
        //        return Conncric3.GetEndPoints().Select(x =>
        //        {
        //            return Conncric3.GetServer(x);
        //        }).FirstOrDefault();
        //    }
        //}
        //public IServer Servercric4
        //{
        //    get
        //    {
        //        return Conncric4.GetEndPoints().Select(x =>
        //        {
        //            return Conncric4.GetServer(x);
        //        }).FirstOrDefault();
        //    }
        //}
        //public IServer Servercric5
        //{
        //    get
        //    {
        //        return Conncric5.GetEndPoints().Select(x =>
        //        {
        //            return Conncric5.GetServer(x);
        //        }).FirstOrDefault();
        //    }
        //}
        //public IServer Servercric6
        //{
        //    get
        //    {
        //        return Conncric6.GetEndPoints().Select(x =>
        //        {
        //            return Conncric6.GetServer(x);
        //        }).FirstOrDefault();
        //    }
        //}
        public IServer Serverfoot
        {
            get
            {
                return Connfoot.GetEndPoints().Select(x =>
                {
                    return Connfoot.GetServer(x);
                }).FirstOrDefault();
            }
        }
        public IServer Servertenni
        {
            get
            {
                return Conntenni.GetEndPoints().Select(x =>
                {
                    return Conntenni.GetServer(x);
                }).FirstOrDefault();
            }
        }
        public IServer Serverother
        {
            get
            {
                return Connother.GetEndPoints().Select(x =>
                {
                    return Connother.GetServer(x);
                }).FirstOrDefault();
            }
        }

        //private static Lazy<ConnectionMultiplexer> lazyConnLocalDyn(string serverr)
        //{


        //    var options = new ConfigurationOptions
        //    {
        //        AbortOnConnectFail = false,
        //        EndPoints = { serverr }
        //    };
        //    return new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(options));
        //}
        //private static Lazy<ConfigurationOptions> configOptions(string serverr = "")
        //{
        //    var configOptions = new ConfigurationOptions();
        //    configOptions.EndPoints.Add(serverr);
        //    configOptions.AbortOnConnectFail = false;
        //    return new Lazy<ConfigurationOptions>(() => configOptions);
        //}
        //public static ConnectionMultiplexer ConnLocalDyn(string serverr = "")
        //{
        //    return lazyConnLocalDyn(serverr).Value;
        //}


        //private static Lazy<ConnectionMultiplexer> lazyConnLocalDyn(string serverr)
        //{

        //    return new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(new ConfigurationOptions
        //    {
        //        AbortOnConnectFail = false,
        //        EndPoints = { serverr }
        //    }));
        //}
        //public static ConnectionMultiplexer ConnLocalDyn(string serverr = "")
        //{
        //    var options = new ConfigurationOptions
        //    {
        //        AbortOnConnectFail = false,
        //        EndPoints = { serverr }
        //    };
        //    return ConnectionMultiplexer.Connect(options);
        //    //return lazyConnLocalDyn(serverr).Value;
        //}
    }
}