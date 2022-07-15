using Models.DimFront;
using System;
using System.Data;

namespace Services.DimFrontOpenser
{
    public interface IDimFrontService
    {
        DataSet Usercreate(Usercreate userMaster);
        DataSet UsercreateData(string json);
        DataSet Userexist(Userexist tvata);
        DataSet Framelogin(Framelogin framelogin);
        DataSet Tvata(Tvata tvata);
    }
}
