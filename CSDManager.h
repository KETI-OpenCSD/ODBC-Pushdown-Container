#pragma once
#include <iostream>
#include <unordered_map>
#include <vector>

using namespace std;


struct CSDInfo{
    string CSDIP;
    string CSDReplica;
    int CSDWorkingBlock;
    vector<string> SSTList;
};

class CSDManager{
    public:
    void CSDManagerInit();
    CSDManager(){CSDManagerInit();}
    void CSDBlockDesc(string id);


    CSDInfo getCSDInfo(string CSDID);
    vector<string> getCSDIDs();
    string getsstincsd(string sstname);
    
    private:
        unordered_map<string,CSDInfo> CSD_Map_;
};