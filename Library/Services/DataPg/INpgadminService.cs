using Models.DimFront;
using System;
using System.Data;

namespace Services.DataPg
{
    public interface INpgadminService
    {
        DataSet HighlightDataopen(HighlightDataOpen highlightDataOpen);
        DataSet Hlgamede(String keys, String tablename);
        DataSet Hlgamedata(String keys, String tablename);
        DataSet Gamedetail(String gmid, String tablename);
        DataSet Gamedata(String gmid, String tablename);
        DataSet Treedata(String tablename);
        DataSet Treedatahor(String tablename);
    }
}
