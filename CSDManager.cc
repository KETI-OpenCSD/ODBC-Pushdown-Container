#include "CSDManager.h"


void CSDManager::CSDManagerInit(){
    CSDInfo initinfo;
    initinfo.CSDIP = "10.0.5.120+10.1.1.2";
    initinfo.CSDReplica = "4";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("002010.sst");
    initinfo.SSTList.push_back("002025.sst");
    initinfo.SSTList.push_back("002055.sst");
    initinfo.SSTList.push_back("002188.sst");
    initinfo.SSTList.push_back("002320.sst");
    initinfo.SSTList.push_back("002568.sst");
    initinfo.SSTList.push_back("1.sst");
    initinfo.SSTList.push_back("9.sst");
    initinfo.SSTList.push_back("17.sst");
    initinfo.CSDList.push_back("9");
    initinfo.CSDList.push_back("17");
    CSD_Map_.insert(make_pair("1",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.2.2";
    initinfo.CSDReplica = "5";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("002072.sst");
    initinfo.SSTList.push_back("002205.sst");
    initinfo.SSTList.push_back("002337.sst");
    initinfo.SSTList.push_back("002569.sst");
    initinfo.SSTList.push_back("2.sst");
    initinfo.SSTList.push_back("10.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("10");
    initinfo.CSDList.push_back("18");
    CSD_Map_.insert(make_pair("2",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.3.2";
    initinfo.CSDReplica = "5";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("002089.sst");
    initinfo.SSTList.push_back("002221.sst");
    initinfo.SSTList.push_back("002353.sst");
    initinfo.SSTList.push_back("002570.sst");
    initinfo.SSTList.push_back("3.sst");
    initinfo.SSTList.push_back("11.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("11");
    initinfo.CSDList.push_back("19");
    CSD_Map_.insert(make_pair("3",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.4.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("002106.sst");
    initinfo.SSTList.push_back("002238.sst");
    initinfo.SSTList.push_back("002370.sst");
    initinfo.SSTList.push_back("002571.sst");
    initinfo.SSTList.push_back("4.sst");
    initinfo.SSTList.push_back("12.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("12");
    initinfo.CSDList.push_back("20");
    CSD_Map_.insert(make_pair("4",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.5.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("002122.sst");
    initinfo.SSTList.push_back("002254.sst");
    initinfo.SSTList.push_back("002386.sst");
    initinfo.SSTList.push_back("002572.sst");
    initinfo.SSTList.push_back("5.sst");
    initinfo.SSTList.push_back("13.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("13");
    initinfo.CSDList.push_back("21");
    CSD_Map_.insert(make_pair("5",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.6.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("002139.sst");
    initinfo.SSTList.push_back("002271.sst");
    initinfo.SSTList.push_back("002403.sst");
    initinfo.SSTList.push_back("002573.sst");
    initinfo.SSTList.push_back("6.sst");
    initinfo.SSTList.push_back("14.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("14");
    initinfo.CSDList.push_back("22");
    CSD_Map_.insert(make_pair("6",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.7.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("002155.sst");
    initinfo.SSTList.push_back("002187.sst");
    initinfo.SSTList.push_back("002419.sst");
    initinfo.SSTList.push_back("002574.sst");
    initinfo.SSTList.push_back("7.sst");
    initinfo.SSTList.push_back("15.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("15");
    initinfo.CSDList.push_back("23");
    CSD_Map_.insert(make_pair("7",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.8.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("002187.sst");
    initinfo.SSTList.push_back("002319.sst");
    initinfo.SSTList.push_back("002567.sst");
    initinfo.SSTList.push_back("002575.sst");
    initinfo.SSTList.push_back("8.sst");
    initinfo.SSTList.push_back("16.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("16");
    initinfo.CSDList.push_back("24");
    CSD_Map_.insert(make_pair("8",initinfo));
}

CSDInfo CSDManager::getCSDInfo(string CSDID){
    return CSD_Map_[CSDID];
}


void CSDManager::CSDBlockDesc(string id, int num){
    CSD_Map_[id].CSDWorkingBlock = CSD_Map_[id].CSDWorkingBlock - num;
}

vector<string> CSDManager::getCSDIDs(){
    vector<string> ret;
    for(auto i = CSD_Map_.begin(); i != CSD_Map_.end(); i++){
        pair<string,CSDInfo> tmppair;
        tmppair = *i;
        ret.push_back(tmppair.first);
    }
    return ret;
}


string CSDManager::getsstincsd(string sstname){
    for(auto it = CSD_Map_.begin(); it != CSD_Map_.end(); it++){
        pair<string,CSDInfo> tmpp = *it;
        for(int i = 0; i < tmpp.second.SSTList.size(); i++){
            if(sstname == tmpp.second.SSTList[i]){
                return tmpp.first;
            }
        }
    }
    return "";
}