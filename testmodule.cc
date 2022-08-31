#include "buffer_manager.h"

// using namespace std;




void get_data(int snippetnumber,BufferManager &bufma, string tablealias){
    string tablename[7] = {"snippet4-0","snippet4-1","snippet4-2","snippet4-3","snippet4-4","snippet4-5","snippet4-6"};
    string filename[13] = {"c_custkey", "c_nationkey", "l_orderkey", "l_suppkey", "revenue","o_custkey","o_orderkey","r_regionkey","s_nationkey","s_suppkey","n_name","n_nationkey","n_regionkey"};
    string filetype[13] = {"0","0","0","0","1","0","0","0","0","0","2","0","0"};
    vector<string> aa;
    vector<int> bb;
    bufma.InitWork(4,snippetnumber,tablename[snippetnumber],aa,bb,bb,0);
    // for(int i = 0; i < )
    vector<int> tmpvec;
    unordered_map<string,vector<vectortype>> tmpmap;
    if(snippetnumber == 0){
        tmpvec.push_back(0);
        tmpvec.push_back(1);
        int filefd;
        for(int i = 0; i < tmpvec.size(); i++){
            vector<vectortype> tmpdata;
            string filepath = "testdata/" + filename[tmpvec[i]] + ".txt";
            // if ((filefd = open(filepath.c_str(), O_RDONLY)) < 0) {
            //     fprintf(stderr, "open error for %s\n", filepath);
            //     exit(1);
            // }
            ifstream inputfile(filepath);
            string line;
            if(!inputfile.is_open()){
                exit(1);
            }
            vectortype tmpvect;
            while(getline(inputfile,line)){
                if(filetype[tmpvec[i]] == "0"){
                    tmpvect.type = 1;
                    tmpvect.intvec = stoi(line);
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(stoi(line));
                }else if(filetype[tmpvec[i]] == "1"){
                    tmpvect.type = 2;
                    tmpvect.floatvec = stod(line);
                    tmpdata.push_back(tmpvect);
                }else{
                    tmpvect.type = 0;
                    tmpvect.strvec = line;
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(line);
                }
            }
            tmpmap.insert(make_pair(filename[tmpvec[i]].c_str(),tmpdata));
        }
    }else if(snippetnumber == 1){
        tmpvec.push_back(5);
        tmpvec.push_back(6);
        for(int i = 0; i < tmpvec.size(); i++){
            vector<vectortype> tmpdata;
            string filepath = "testdata/" + filename[tmpvec[i]] + ".txt";
            // if ((filefd = open(filepath.c_str(), O_RDONLY)) < 0) {
            //     fprintf(stderr, "open error for %s\n", filepath);
            //     exit(1);
            // }
            ifstream inputfile(filepath);
            string line;
            if(!inputfile.is_open()){
                exit(1);
            }
            vectortype tmpvect;
            while(getline(inputfile,line)){
                if(filetype[tmpvec[i]] == "0"){
                    tmpvect.type = 1;
                    tmpvect.intvec = stoi(line);
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(stoi(line));
                }else if(filetype[tmpvec[i]] == "1"){
                    tmpvect.type = 2;
                    tmpvect.floatvec = stod(line);
                    tmpdata.push_back(tmpvect);
                }else{
                    tmpvect.type = 0;
                    tmpvect.strvec = line;
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(line);
                }
            }
            tmpmap.insert(make_pair(filename[tmpvec[i]].c_str(),tmpdata));
        }
    }else if(snippetnumber == 2){
        tmpvec.push_back(2);
        tmpvec.push_back(3);
        tmpvec.push_back(4);
        for(int i = 0; i < tmpvec.size(); i++){
            vector<vectortype> tmpdata;
            string filepath = "testdata/" + filename[tmpvec[i]] + ".txt";
            // if ((filefd = open(filepath.c_str(), O_RDONLY)) < 0) {
            //     fprintf(stderr, "open error for %s\n", filepath);
            //     exit(1);
            // }
            ifstream inputfile(filepath);
            string line;
            if(!inputfile.is_open()){
                exit(1);
            }
            vectortype tmpvect;
            while(getline(inputfile,line)){
                if(filetype[tmpvec[i]] == "0"){
                    tmpvect.type = 1;
                    tmpvect.intvec = stoi(line);
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(stoi(line));
                }else if(filetype[tmpvec[i]] == "1"){
                    tmpvect.type = 2;
                    tmpvect.floatvec = stod(line);
                    tmpdata.push_back(tmpvect);
                }else{
                    tmpvect.type = 0;
                    tmpvect.strvec = line;
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(line);
                }
            }
            tmpmap.insert(make_pair(filename[tmpvec[i]].c_str(),tmpdata));
        }
    }else if(snippetnumber == 4){
        tmpvec.push_back(8);
        tmpvec.push_back(9);
        for(int i = 0; i < tmpvec.size(); i++){
            vector<vectortype> tmpdata;
            string filepath = "testdata/" + filename[tmpvec[i]] + ".txt";
            // if ((filefd = open(filepath.c_str(), O_RDONLY)) < 0) {
            //     fprintf(stderr, "open error for %s\n", filepath);
            //     exit(1);
            // }
            ifstream inputfile(filepath);
            string line;
            if(!inputfile.is_open()){
                exit(1);
            }
            vectortype tmpvect;
            while(getline(inputfile,line)){
                if(filetype[tmpvec[i]] == "0"){
                    tmpvect.type = 1;
                    tmpvect.intvec = stoi(line);
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(stoi(line));
                }else if(filetype[tmpvec[i]] == "1"){
                    tmpvect.type = 2;
                    tmpvect.floatvec = stod(line);
                    tmpdata.push_back(tmpvect);
                }else{
                    tmpvect.type = 0;
                    tmpvect.strvec = line;
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(line);
                }
            }
            // cout << tmpdata.size() << endl;
            tmpmap.insert(make_pair(filename[tmpvec[i]].c_str(),tmpdata));
        }
    }else if(snippetnumber == 5){
        tmpvec.push_back(10);
        tmpvec.push_back(11);
        tmpvec.push_back(12);
        for(int i = 0; i < tmpvec.size(); i++){
            vector<vectortype> tmpdata;
            string filepath = "testdata/" + filename[tmpvec[i]] + ".txt";
            // if ((filefd = open(filepath.c_str(), O_RDONLY)) < 0) {
            //     fprintf(stderr, "open error for %s\n", filepath);
            //     exit(1);
            // }
            ifstream inputfile(filepath);
            string line;
            if(!inputfile.is_open()){
                exit(1);
            }
            vectortype tmpvect;
            while(getline(inputfile,line)){
                if(filetype[tmpvec[i]] == "0"){
                    tmpvect.type = 1;
                    tmpvect.intvec = stoi(line);
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(stoi(line));
                }else if(filetype[tmpvec[i]] == "1"){
                    tmpvect.type = 2;
                    tmpvect.floatvec = stod(line);
                    tmpdata.push_back(tmpvect);
                }else{
                    tmpvect.type = 0;
                    tmpvect.strvec = line;
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(line);
                }
            }
            tmpmap.insert(make_pair(filename[tmpvec[i]].c_str(),tmpdata));
        }
    }else if(snippetnumber == 6){
        tmpvec.push_back(7);
        for(int i = 0; i < tmpvec.size(); i++){
            vector<vectortype> tmpdata;
            string filepath = "testdata/" + filename[tmpvec[i]] + ".txt";
            // if ((filefd = open(filepath.c_str(), O_RDONLY)) < 0) {
            //     fprintf(stderr, "open error for %s\n", filepath);
            //     exit(1);
            // }
            ifstream inputfile(filepath);
            string line;
            if(!inputfile.is_open()){
                exit(1);
            }
            vectortype tmpvect;
            while(getline(inputfile,line)){
                if(filetype[tmpvec[i]] == "0"){
                    tmpvect.type = 1;
                    tmpvect.intvec = stoi(line);
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(stoi(line));
                }else if(filetype[tmpvec[i]] == "1"){
                    tmpvect.type = 2;
                    tmpvect.floatvec = stod(line);
                    tmpdata.push_back(tmpvect);
                }else{
                    tmpvect.type = 0;
                    tmpvect.strvec = line;
                    tmpdata.push_back(tmpvect);
                    // tmpdata.push_back(line);
                }
            }
            tmpmap.insert(make_pair(filename[tmpvec[i]].c_str(),tmpdata));
        }
    }else if( snippetnumber == 3){
        tmpmap = bufma.GetTableData(4,"snippet4-2").table_data;
    }
    cout << tablealias << endl;
    bufma.SaveTableData(4,tablealias,tmpmap);
}
