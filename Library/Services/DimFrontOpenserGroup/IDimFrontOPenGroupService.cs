using Models.DimFrontGroup;
using System;
using System.Data;

namespace Services.DimFrontOpenserGroup
{
    public interface IDimFrontOPenGroupService
    {
        DataSet Usercreate(Usercreate userMaster);
        DataSet UsercreateData(string json);
        DataSet Userexist(Userexist tvata);
        DataSet Framelogin(Framelogin framelogin);
        DataSet Tvata(Tvata tvata);
    }
}
