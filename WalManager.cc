#include "WalManager.h"

// RapidJSON include
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

// CPP REST include
#include "stdafx.h"
#include <cpprest/http_client.h>

#include <iostream>
#include <vector>
#include <sstream>

#include <unordered_map>


using namespace web::http;
using namespace web::http::client;
using namespace rapidjson;

// std::vector<std::string> split(std::string str, char delimiter){
//     std::vector<std::string> answer;
//     std::stringstream ss(str);
//     std::string temp;
 
//     while (getline(ss, temp, delimiter)) {
//         answer.push_back(temp);
//     }
 
//     return answer;
// }

void WalManager::WalScan() {
    std::string wal_json;
    // get unflushed rows
    web::http::client::http_client client(U("http://10.0.5.37:12345/")); // wal server ip

    web::http::http_request request(web::http::methods::GET);
    request.headers().add(U("Content-Type"), U("application/json"));
    std::string req_str = "{\"tbl_name\":\"" + snippet_.table_name(0) +  "\"}";
    request.set_body(web::json::value::parse(req_str)); // tag change -> tableName

    auto requestTask = client.request(request).then([&](web::http::http_response response)
    {
		return response.extract_string();
	}).then([&](utility::string_t str)
	{
        std::cout << "wal rows :\n" << str << std::endl;
        wal_json = str;
        /*
        Document document;
        document.Parse(str.c_str());
        
        //std::cout << "n_rows : " << n_rows << std::endl;
        Value &values = document["rows"];
        //std::cout << "rows : " << std::endl;
        for(int i=0;i<values.Size();i++){
            rows.push_back(values[i].GetString());
            //std::cout << row << std::endl;
        }
        */
	});

	try
	{
		requestTask.wait();
	}
	catch (const std::exception &e)
	{
		printf("Error exception:%s\n", e.what());
	}

    /*
    // use table info from snippet
    // convert data
    std::string raw_data = "";
    // loop recved n_rows
    for(int i=0;i<n_rows;i++){
        // split row to columns
        size_t pos = 0;
        auto columns = split(rows[i], ',');
        // loop table n_columns
        for(int j=0;j<columns.size();j++){
            switch(snippet_.table_datatype(j)){
                case 3: {//int32                
                int ibuf = stoi(columns[j]);
                std::string tmp((char*)&ibuf,4);
                raw_data += tmp;
                }
                break;
                case 14: {//date
                std::string date_str = columns[j].substr(1,columns[j].size()-2);
                auto ymd = split(date_str, '-');
                int year, month, day;
                year = stoi(ymd[0]);
                month = stoi(ymd[1]);
                day = stoi(ymd[2]);
                int int_date = day + (month * 32) + (year * 16 * 32);

                std::string tmp = "";
                tmp += ((char *)&int_date)[2];
                tmp += ((char *)&int_date)[1];
                tmp += ((char *)&int_date)[0];
                raw_data += tmp;
                }
                break;
                case 15: {//varchar
                std::string varchar_str(columns[j].c_str()+1,columns[j].size()-2<snippet_.table_offlen(j)?columns[j].size()-2:snippet_.table_offlen(j));
                std::string tmp = "";
                if(snippet_.table_offlen(j) < 255){
                    char varchar_len = varchar_str.size();
                    tmp += varchar_len;
                } else {
                    // 2 byte length 
                }
                tmp += varchar_str;
                raw_data += tmp;
                }
                break;
                case 246: {//numeric(decimal), Support Only decimal(15,2)
                auto numeric_data = split(columns[j], '.');
                int integer_part;
                int real_part;
                std::string tmp = "";
                //정수부 변환, big int 지원 X
                {
                    integer_part = stoi(numeric_data[0]);
                }
                //실수부 변환
                {
                    std::string real_string(2,'0');
                    for(int k=0;k<numeric_data[1].size();k++){
                        real_string[k] = numeric_data[1][k];
                    }
                    real_part = stoi(real_string);
                }                
                //byte mapping
                {
                    tmp += (char)0x80;
                    tmp += (char)0x00;
                    tmp += ((char *)&integer_part)[3];
                    tmp += ((char *)&integer_part)[2];
                    tmp += ((char *)&integer_part)[1];
                    tmp += ((char *)&integer_part)[0];
                    tmp += ((char *)&real_part)[0];
                }
                raw_data += tmp;
                }
                break;
                case 254: {//char
                std::string tmp(snippet_.table_offlen(j),' ');
                for(int k=0;k<snippet_.table_offlen(j) && k<columns[j].size()-2;k++){
                    tmp[k] = columns[j][k+1];
                }
                raw_data += tmp;
                }
                break;
            }
        }
    }
    
    printf("wal raw_data\n");
    for(int i=0;i<raw_data.size();i++){
            printf("%02X",(u_char)raw_data.c_str()[i]);
    }
    printf("\n");
    */
}

struct myany {
    snippetsample::Snippet_ValueType type;
    std::string str_val;
    char value[8];
};

void WalManager::WalFilter() {
    /*
    // filtering converted data

    // n_rows
    // raw data
    char *ptr = raw_data.c_str();
    char lv_buffer[8];
    char rv_buffer[8];

    std::unordered_map<std::string,int> column_index;
    for(int i=0;i<snippet_.table_col_size();i++){
        column_index.insert(std::make_pair(snippet_.table_col(i),i));
    }

    // varchar 형 포함 table 검사
    if(hasVarcharMiddle()){
        // calculate column map during filtering
    } else {
        // use table offset
    }

    // for n_rows
    {
        bool filter_result;
        // foreach filters
        for(int i=0;i<snippet_.table_filter_size();i++){
            auto filter = table_filter(i);
            switch(filter.select_type()){
                // 
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_AND :{
                    if(!filter_result){ // skip to next or

                    }
                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_OR :{
                    if(filter_result){ // skip to next and

                    }
                }
                break;
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_BRACKET_OPEN :{

                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_BRACKET_CLOSE :{

                }
                break;
                default :{
                    // check lv 
                    {
                        auto lv = filter.LV();
                        // calculate lv value

                    }
                    // check overator
                    {
                        filter.select_type()
                    }
                    // check rv or extra
                    {

                    }
                }
                break;
                //
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_GE :{
                    // check lv
                    auto lv = filter.LV();
                    lv.type_size();
                    // loop
                    {

                        if(lv.type() = 10){ // column name memcpy
                            // get column index
                            int col_index = column_index[lv.value()];
                            // get column offset
                            int col_offset = snippet_.table_offset(col_index);
                            // get column type
                            int col_type = snippet_.table_datatype(col_index);
                            // get column length
                            int col_offlen = snippet_.table_offlen(col_index);

                            switch(col_type){ //mysql type
                                //convert data
                                case 3 : //int
                                case 14 : //date
                                case 246 : //numeric decimal(15,2)
                                { 
                                    memcpy(lv_buffer,ptr+col_offset,col_offlen);
                                }
                                break;
                                case 15 : //varchar
                                case 254 : //char
                                {
                                    //use ptr
                                }
                            }
                        } else {
                            // type casting
                            // eg. int
                            
                            switch(lv.type()){
                                case snippetsample::Snippet_ValueType::Snippet_ValueType_INT8 :
                                lv_buffer[0] = (char)std::stoi(lv.value(0));
                                break;
                                case snippetsample::Snippet_ValueType::Snippet_ValueType_INT32 :
                                *(int*)lv_buffer = std::stoi(lv.value(0));
                                break;
                                case snippetsample::Snippet_ValueType::Snippet_ValueType_FLOAT32 :
                                break;
                                case snippetsample::Snippet_ValueType::Snippet_ValueType_FLOAT64 :
                                break;
                                case snippetsample::Snippet_ValueType::Snippet_ValueType_NUMERIC :
                                break;
                                case snippetsample::Snippet_ValueType::Snippet_ValueType_DATE :
                                break;
                                //hex to buffer 개발해야함
                            }
                        }
                        
                    }
                    // check column type
                    // get column data from raw data
                    // convert data
                    // compare
                    
                    
                    KETI_GE(filter.LV(),filter.RV());
                    filter_result = true;
                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_LE :{

                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_GT :{

                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_LT :{

                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_ET :{

                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_NE :{

                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_LIKE :{

                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_BETWEEN :{

                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_IN :{

                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_IS :{

                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_ISNOT :{

                }
                break;
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_NOT :{

                }
                case snippetsample::Snippet_Filter_OperType::Snippet_Filter_OperType_KETI_SUBSTRING :{

                }
                break;
            }
        }
        {
            // switch case(operator)

        }
    }
    */
}

bool WalManager::hasVarcharMiddle() {
    int column_len = snippet_.table_datatype_size();
    for(int i=0;i<column_len-1;i++){
        if(snippet_.table_datatype(i) == 15){
            return true;
        }
    }
    return false;
}

void WalManager::WalMerge() {
    // projection filtered data
}