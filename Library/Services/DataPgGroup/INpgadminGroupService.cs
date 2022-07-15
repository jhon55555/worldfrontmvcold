using Models.DimFrontGroup;
using System;
using System.Data;

namespace Services.DataPgGroup
{
    public interface INpgadminGroupService
    {
        DataSet HighlightDataopen(HighlightDataOpen highlightDataOpen);
        DataSet Hlgamede(String keys, String tablename);
        DataSet Hlgamedata(String keys, String tablename);
        DataSet Gamedetail(String gmid, String tablename);
        DataSet Gamedata(String gmid, String tablename);
        DataSet Treedata(String tablename);
    }
}
