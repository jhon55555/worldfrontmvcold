using StructureMap;
using StructureMap.Graph;

namespace Common
{
    public static class Ioc
    {
        public static IContainer container;

        static Ioc()
        {
            container =  new Container(x =>
            {
                x.Scan(scan =>
                {
                    scan.TheCallingAssembly();
                    scan.Assembly("Data");
                    scan.Assembly("Services");
                    scan.WithDefaultConventions();
                });
            });
        }


        public static IContainer Initialize()
        {
            return container;
        }
    }
}
