#pragma once
#include <vector>
#include <iostream>
#include <sstream>
#include <string.h>
#include <unordered_map>
#include <sys/socket.h>
#include <cstdlib>
#include <netinet/in.h> 
#include <arpa/inet.h>
// #include <unistd.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

#include "CSDManager.h"

using namespace std;
using namespace rapidjson;

struct vectortype{
  string strvec;
  int64_t intvec;
  double floatvec;
  int type; //0 string, 1 int, 2 float
};


struct lv{
    vector<int> type;
    vector<string> value;
};

struct rv{
    vector<int> type;
    vector<string> value;
};

struct filterstruct{
    lv LV;
    int filteroper;
    rv RV; 
};


struct Projection{
    string value;
    int type; // 0 int, 1 float, 2 string, 3 column
};

struct SnippetData{
        int query_id;
        int work_id;
        string sstfilename;
        Value block_info_list;
        vector<string> table_col;
        // Value table_filter;
        vector<filterstruct> table_filter;
        vector<int> table_offset;
        vector<int> table_offlen;
        vector<int> table_datatype;
        vector<string> sstfilelist;
        string tablename;
        vector<vector<Projection>> column_projection;
        vector<string> column_filtering;
        vector<string> Group_By;
        vector<string> Order_By;
        vector<string> Expr;
        vector<int> returnType;
};


class Scheduler{

    public:
        Scheduler(CSDManager& csdmanager) {init_scheduler(csdmanager);}
        vector<int> blockvec;
        vector<tuple<string,string,string>> savedfilter;
        vector<int> passindex;
        SnippetData snippetdata;
        vector<int> threadblocknum;
    struct Snippet{
        int query_id;
        int work_id;
        string sstfilename;
        Value block_info_list;
        vector<string> table_col;
        vector<filterstruct> table_filter;
        // Value table_filter;
        vector<int> table_offset;
        vector<int> table_offlen;
        vector<int> table_datatype;
        vector<string> sstfilelist;
        vector<string> column_filtering;
        vector<string> Group_By;
        vector<string> Order_By;
        vector<string> Expr;
        vector<vector<Projection>> column_projection;
        vector<int> returnType;

        Snippet(int query_id_,int work_id_, string sstfilename_,
            // Value block_info_list_,
            vector<string> table_col_, 
            // Value table_filter_, 
            vector<int> table_offset_, vector<int> table_offlen_,
            vector<int> table_datatype_, vector<string> column_filtering_,
            vector<string> Group_By_, vector<string> Order_By_, vector<string> Expr_,
            vector<vector<Projection>> column_projection_, 
            vector<int> returnType_)
            : query_id(query_id_),work_id(work_id_), sstfilename(sstfilename_),
            // block_info_list(block_info_list_),
            table_col(table_col_),
            // table_filter(table_filter_),
            table_offset(table_offset_),
            table_offlen(table_offlen_),
            table_datatype(table_datatype_),
            column_filtering(column_filtering_),
            Group_By(Group_By_), Order_By(Order_By_), Expr(Expr_),
            column_projection(column_projection_),
             returnType(returnType_){};
        Snippet();
    };

        typedef enum work_type{
            SCAN = 4,
            SCAN_N_FILTER = 5,
            REQ_SCANED_BLOCK = 6,
            WORK_END = 9
        }KETI_WORK_TYPE;

        void init_scheduler(CSDManager& csdmanager);
        // void sched(int workid, Value& blockinfo,vector<int> offset, vector<int> offlen, vector<int> datatype, vector<string> tablecol, Value& filter,string sstfilename, string tablename, string res);
        void sched(int indexdata, CSDManager& csdmanager);
        void csdworkdec(string csdname, int num);
        void Serialize(PrettyWriter<StringBuffer>& writer, Snippet& s, string csd_ip, string tablename, string CSDName);
        void Serialize(Writer<StringBuffer>& writer, Snippet& s, string csd_ip, string tablename, string CSDName, int blockidnum);
        string BestCSD(string sstname, int blockworkcount, CSDManager& csdmanager);
        void CSDManagerDec(string csdname, int num);
        void sendsnippet(string snippet, string ipaddr);
        // void addcsdip(Writer<StringBuffer>& writer, string s);
        void printcsdblock(){
          for(auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
            pair<std::string, int> k = *i;
            cout << k.first << " " << k.second << endl;
          }
        }
    private:
        unordered_map<string,string> csd_; //csd??? ip????????? ?????? ??? <csdname, csdip>
        unordered_map<string, int> csdworkblock_; //csd??? block work ??? ??? ?????? ??? <csdname, csdworknum>
        vector<string> csdname_;
        unordered_map<string,string> sstcsd_; //csd??? sst?????? ?????? ?????? <sstname, csdlist>
        vector<string> csdpair_;
        unordered_map<string,string> csdreaplicamap_;
        vector<vector<string>> csdlist_;
        // CSDManager &csdmanager_;
        int blockcount_;
};


