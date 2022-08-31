#include "mergequerykmc.h"

// typedef string any;

// void Init(Value Query)
// {
//     SnippetStruct snippet;
//     // snippet.tablename = Query["tableName"].GetString();
//     if (IsJoin(snippet))
//     { //버퍼매니저에 테이블의 정보가 있을 경우
//         //일단 1번 기준으로만 작성
//         ColumnProjection(snippet);
//     }
//     else
//     { 
//         // JoinTable(snippet);
//     }
// }

unordered_map<string,vector<vectortype>> GetBufMTable(string tablename, SnippetStruct& snippet, BufferManager &buff)
{

        unordered_map<string,vector<vectortype>> table = buff.GetTableData(snippet.query_id, tablename).table_data;
        // cout << tablename << endl;
        // for(auto i = table.begin(); i != table.end(); i++){
        //     pair<string,vector<any>> a = *i;
        //     cout << a.first << endl;
        //     cout << a.second.size() << endl;
        // }
        // buff.GetTableInfo(snippet.query_id, tablename); //테이블 데이터 말고 type, name, rownum, blocknum까지 채워줌
        return table;    
     
}

vector<vectortype> Postfix(unordered_map<string,vector<vectortype>> tablelist, vector<Projection> data, unordered_map<string,vector<vectortype>> savedTable){
    unordered_map<string,int> stackmap;
    vector<vectortype> ret;
    pair<string,vector<vectortype>> tmppair;
    auto tmpiter = tablelist.begin();
    tmppair = *tmpiter;
    int rownum = tmppair.second.size();
    if(data[0].value == "0"){
        // StackType tmpstack;
        stack<vectortype> tmpstack;
        // tmpstack.type = typeid(tablelist[data[1].value]).name;
        for(int i = 0; i < rownum; i++){
            for(int j = 1; j < data.size(); j++){
                vectortype tmpvect;
                if(data[j].type == 10){
                    // cout << any_cast<int>(tablelist[data[j].value][i]) << endl;
                    tmpstack.push(tablelist[data[j].value][i]);
                }else if(data[j].type == 2){
                    if(data[j].value == "+"){
                        vectortype value1 = tmpstack.top();
                        tmpstack.pop();
                        vectortype value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type == 1){
                            tmpvect.intvec = value1.intvec + value2.intvec;
                            tmpvect.type = 1;
                            // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
                            tmpstack.push(tmpvect);
                        }else if(value1.type == 2){
                            tmpvect.intvec = value1.floatvec + value2.floatvec;
                            tmpvect.type = 2;
                            // float retnum = any_cast<float>(value1) + any_cast<float>(value2);
                            tmpstack.push(tmpvect);
                        }
                    }else if(data[j].value == "-"){
                        vectortype value1 = tmpstack.top();
                        tmpstack.pop();
                        vectortype value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type == 1){
                            tmpvect.intvec = value1.intvec - value2.intvec;
                            tmpvect.type = 1;
                            // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
                            tmpstack.push(tmpvect);
                        }else if(value1.type == 2){
                            tmpvect.intvec = value1.floatvec - value2.floatvec;
                            tmpvect.type = 2;
                            // float retnum = any_cast<float>(value1) + any_cast<float>(value2);
                            tmpstack.push(tmpvect);
                        }
                    }else if(data[j].value == "*"){
                        vectortype value1 = tmpstack.top();
                        tmpstack.pop();
                        vectortype value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type == 1){
                            tmpvect.intvec = value1.intvec * value2.intvec;
                            tmpvect.type = 1;
                            // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
                            tmpstack.push(tmpvect);
                        }else if(value1.type == 2){
                            tmpvect.intvec = value1.floatvec * value2.floatvec;
                            tmpvect.type = 2;
                            // float retnum = any_cast<float>(value1) + any_cast<float>(value2);
                            tmpstack.push(tmpvect);
                        }
                    }else if(data[j].value == "/"){
                        vectortype value1 = tmpstack.top();
                        tmpstack.pop();
                        vectortype value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type == 1){
                            tmpvect.intvec = value1.intvec / value2.intvec;
                            tmpvect.type = 1;
                            // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
                            tmpstack.push(tmpvect);
                        }else if(value1.type == 2){
                            tmpvect.intvec = value1.floatvec / value2.floatvec;
                            tmpvect.type = 2;
                            // float retnum = any_cast<float>(value1) + any_cast<float>(value2);
                            tmpstack.push(tmpvect);
                        }
                    }else{
                        //string
                    }
                }else if(data[j].type == 1){
                    tmpvect.type = 2;
                    tmpvect.floatvec = stof(data[j].value);
                    // tmpstack.push(stof(data[j].value));
                    tmpstack.push(tmpvect);
                }else{
                    // cout << data[j].value << endl;
                    tmpvect.type = 1;
                    tmpvect.intvec = stoi(data[j].value);
                    // tmpstack.push(stof(data[j].value));
                    tmpstack.push(tmpvect);
                    // tmpstack.push(stoi(data[j].value));
                }

            }
            // savedTable["asd"].push_back(tmpstack.top());
            ret.push_back(tmpstack.top());
            // cout << 1;
        }
    }else if(data[0].value == "1"){ //sum
        vectortype retdata;
        stack<vectortype> tmpstack;
        // tmpstack.type = typeid(tablelist[data[1].value]).name;
        for(int i = 0; i < rownum; i++){
            for(int j = 1; j < data.size(); j++){
                vectortype tmpvect;
                if(data[j].type == 3){
                    tmpstack.push(tablelist[data[j].value][i]);
                }else if(data[j].type == 2){
                    if(data[j].value == "+"){
                        vectortype value1 = tmpstack.top();
                        tmpstack.pop();
                        vectortype value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type == 1){
                            tmpvect.intvec = value1.intvec + value2.intvec;
                            tmpvect.type = 1;
                            // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
                            tmpstack.push(tmpvect);
                        }else if(value1.type == 2){
                            tmpvect.intvec = value1.floatvec + value2.floatvec;
                            tmpvect.type = 2;
                            // float retnum = any_cast<float>(value1) + any_cast<float>(value2);
                            tmpstack.push(tmpvect);
                        }
                    }else if(data[j].value == "-"){
                        vectortype value1 = tmpstack.top();
                        tmpstack.pop();
                        vectortype value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type == 1){
                            tmpvect.intvec = value1.intvec- value2.intvec;
                            tmpvect.type = 1;
                            // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
                            tmpstack.push(tmpvect);
                        }else if(value1.type == 2){
                            tmpvect.intvec = value1.floatvec - value2.floatvec;
                            tmpvect.type = 2;
                            // float retnum = any_cast<float>(value1) + any_cast<float>(value2);
                            tmpstack.push(tmpvect);
                        }
                    }else if(data[j].value == "*"){
                        vectortype value1 = tmpstack.top();
                        tmpstack.pop();
                        vectortype value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type == 1){
                            tmpvect.intvec = value1.intvec * value2.intvec;
                            tmpvect.type = 1;
                            // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
                            tmpstack.push(tmpvect);
                        }else if(value1.type == 2){
                            tmpvect.intvec = value1.floatvec * value2.floatvec;
                            tmpvect.type = 2;
                            // float retnum = any_cast<float>(value1) + any_cast<float>(value2);
                            tmpstack.push(tmpvect);
                        }
                    }else if(data[j].value == "/"){
                        vectortype value1 = tmpstack.top();
                        tmpstack.pop();
                        vectortype value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type == 1){
                            tmpvect.intvec = value1.intvec / value2.intvec;
                            tmpvect.type = 1;
                            // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
                            tmpstack.push(tmpvect);
                        }else if(value1.type == 2){
                            tmpvect.intvec = value1.floatvec / value2.floatvec;
                            tmpvect.type = 2;
                            // float retnum = any_cast<float>(value1) + any_cast<float>(value2);
                            tmpstack.push(tmpvect);
                        }
                    }else{
                        //string
                    }
                }else if(data[j].type == 1){
                    tmpvect.type = 2;
                    tmpvect.floatvec = stof(data[j].value);
                    // tmpstack.push(stof(data[j].value));
                    tmpstack.push(tmpvect);
                }else{
                    // cout << data[j].value << endl;
                    tmpvect.type = 1;
                    tmpvect.intvec = stoi(data[j].value);
                    // tmpstack.push(stof(data[j].value));
                    tmpstack.push(tmpvect);
                    // tmpstack.push(stoi(data[j].value));
                }

            }
            //이부분이 sum을 해야함
            
            vectortype tmpnum = tmpstack.top(); //int인지 flaot인지 구분 필요
            if(tmpnum.type == 1){
                retdata.type = 1;
                retdata.intvec = retdata.intvec + tmpnum.intvec;
                // retdata = any_cast<int>(retdata) + any_cast<int>(tmpnum);
            }else{
                retdata.type = 2;
                retdata.floatvec = retdata.floatvec + tmpnum.floatvec;
                // retdata = any_cast<float>(retdata) + any_cast<float>(tmpnum);
            }

        }
        ret.push_back(retdata);
    }
    // cout << 3 << endl;
    return ret;

}

void Aggregation(SnippetStruct& snippet, BufferManager &buff, bool tablecount){
    unordered_map<string,vector<vectortype>> tablelist;
    cout << "[Merge Query Manager] Strat Aggregation Time : ";
    time_t t = time(0);
    cout << t << endl;
    if(tablecount){
        for(int i = 0; i < snippet.tablename.size(); i++){
            //혹시 모를 중복 제거 필요
            unordered_map<string,vector<vectortype>> table = GetBufMTable(snippet.tablename[i], snippet, buff);
            for(auto it = table.begin(); it != table.end(); it++){
                pair<string,vector<vectortype>> pair;
                pair = *it;
                cout << pair.first << " " << pair.second.size()<< endl;
                tablelist.insert(pair);
            }
        }
    }else{
        unordered_map<string,vector<vectortype>> table = GetBufMTable(snippet.tableAlias, snippet, buff);
        for(auto it = table.begin(); it != table.end(); it++){
            pair<string,vector<vectortype>> pair;
            pair = *it;
            tablelist.insert(pair);
        }
    }
    // cout << 1 << endl;
    unordered_map<string,vector<vectortype>> savedTable;
    for(int i = 0; i < snippet.columnProjection.size(); i++){
        // any ret;
        // ret = Postfix(tablelist,snippet.columnProjection[i], savedTable);
        // vector<any> tmpvec;
        vector<vectortype> tmpdata = Postfix(tablelist,snippet.columnProjection[i], savedTable);
        savedTable.insert(make_pair(snippet.column_alias[i],tmpdata));
    }
    t = time(0);
    cout << "[Merge Query Manager] End Aggregation Time : ";
    cout << t << endl;
    if(!tablecount){
        buff.DeleteTableData(snippet.query_id,snippet.tableAlias);
    }
    // buff.DeleteTableData(snippet.query_id,snippet.tableAlias);
    buff.SaveTableData(snippet.query_id,snippet.tableAlias,savedTable);
}

void JoinTable(SnippetStruct& snippet, BufferManager &buff){
    time_t t = time(0);
    cout <<"[Merge Query Manager] Start Join Table1.Name : " << snippet.tablename[0] << " | Table2.Name : " << snippet.tablename[1]  <<" Time : " << t << endl;
    unordered_map<string,vector<vectortype>> tablelist;
    vector<vector<string>> tablenamelist;
    for(int i = 0; i < 2; i++){
        vector<string> tmpvector;
        // unordered_map<string,vector<any>> table = GetBufMTable(snippet.tablename[i], snippet, buff);
        cout << "[Merge Query Manager] Buffer Manager Access Get Table, Table Name : " << snippet.tablename[i] << endl;
        unordered_map<string,vector<vectortype>> table = buff.GetTableData(snippet.query_id, snippet.tablename[i]).table_data;
        // tablelist.push_back(table);
        for(auto it = table.begin(); it != table.end(); it++){
            pair<string,vector<vectortype>> tmppair = *it;
            tablelist.insert(make_pair(tmppair.first,tmppair.second));
            tmpvector.push_back(tmppair.first);
        }
        tablenamelist.push_back(tmpvector);
    }



    unordered_map<string,vector<vectortype>> savedTable;
    // for(auto i = tablelist[0].begin(); i != tablelist[0].end(); i++){
    //     pair<string,vector<any>> tabledata;
    //     tabledata = *i;
    //     vector<any> table;
    //     savedTable.insert(make_pair(tabledata.first,table));
        
    // }
    // for(auto i = tablelist[1].begin(); i != tablelist[1].end(); i++){
    //     pair<string,vector<any>> tabledata;
    //     tabledata = *i;
    //     vector<any> table;
    //     if(savedTable.find(tabledata.first) != savedTable.end()){
    //         savedTable.insert(make_pair(tabledata.first + "_2", table));
    //     }else{
    //         savedTable.insert(make_pair(tabledata.first, table));
    //     }
    // }
    // cout << 1 << endl;
    // cout << snippet.table_filter.GetType() << endl;
    // cout << snippet.columnProjection[0][0].value << endl;
    // cout << tablelist.size() << endl;
    // cout << tablelist.size() << endl;
    // cout << tablelist["n_regionkey"].size() << endl;
    // for(auto i = tablelist.begin(); i != tablelist.end(); i++){
    //     pair<string,vector<any>> cc = *i;
    //     cout << cc.first << endl;
    // }
    string joinColumn1 = snippet.table_filter[0].LV.value[0];
    string joinColumn2;
    auto ttit = find(tablenamelist[0].begin(),tablenamelist[0].end(),joinColumn1);
    if(ttit != tablenamelist[0].end()){
        joinColumn1 = snippet.table_filter[0].LV.value[0];
        joinColumn2 = snippet.table_filter[0].RV.value[0];
    }else{
        joinColumn1 = snippet.table_filter[0].RV.value[0];
        joinColumn2 = snippet.table_filter[0].LV.value[0];
    }

    // string joinColumn1 = snippet.table_filter[0].LV.value[0];
    
    cout << "[Merge Query Manager] JoinColumn1 : " << joinColumn1 << " | JoinColumn2 : " << joinColumn2 << endl;
    cout << "[Merge Query Manager] " << joinColumn1 <<".Rows : " << tablelist[joinColumn1].size() << " | "<< joinColumn2 << ".Rows : " <<tablelist[joinColumn2].size() << endl;
    // string joinColumn2 = snippet.table_filter[0].RV.value[0];
    // cout << joinColumn2 << endl;
    // cout << tablelist[joinColumn1].size() << endl;
    // cout << tablelist[joinColumn2].size() << endl;
    // cout << 1 << endl;
    // cout << snippet.table_filter.Size() << endl;
    int countcout = 0;
        for(int i = 0; i < tablelist[joinColumn1].size(); i++){
            for(int j = 0; j < tablelist[joinColumn2].size(); j++){
                bool savedflag = true;
                if(tablelist[joinColumn1][i].type == 1){
                    // cout << any_cast<int>(tablelist[joinColumn1][j]) << endl;
                    if(tablelist[joinColumn1][i].intvec == tablelist[joinColumn2][j].intvec){
                        // cout << tablelist[joinColumn1][i].intvec << " " << tablelist[joinColumn2][j].intvec << endl;
                        for(int k = 1; k < snippet.table_filter.size(); k++){
                            string tmpcolumn1;
                            string tmpcolumn2;
                            if(snippet.table_filter[k].LV.value.size() > 0){
                                tmpcolumn1 = snippet.table_filter[k].LV.value[0];
                                tmpcolumn2 = snippet.table_filter[k].RV.value[0];
                                if(countcout == 0){
                                    cout << "[Merge Query Manager] Join Column 1 : " << tmpcolumn1 << " Join Column 2 : " << tmpcolumn2 << endl;
                                    countcout++;
                                }
                            }else{
                                continue;
                            }
                            if(tablelist[tmpcolumn1][i].type == 1){
                                if(tablelist[tmpcolumn1][i].intvec == tablelist[tmpcolumn2][j].intvec){
                                    continue;
                                }else{
                                    savedflag = false;
                                    break;
                                }
                            }else if(tablelist[tmpcolumn1][i].type == 2){
                                if(tablelist[tmpcolumn1][i].floatvec == tablelist[tmpcolumn2][j].floatvec){
                                    continue;
                                }else{
                                    savedflag = false;
                                    break;
                                }
                            }else{
                                if(tablelist[tmpcolumn1][i].strvec == tablelist[tmpcolumn2][j].strvec){
                                    continue;
                                }else{
                                    savedflag = false;
                                    break;
                                }
                            }
                        }
                        if(savedflag){
                            // for(auto it = tablelist.begin(); it != tablelist.end(); it++){
                            //     pair<string,vector<any>> tabledata;
                            //     tabledata = *it;
                            //     vector<any> tmptable = tabledata.second;
                            //     savedTable[tabledata.first].push_back(tmptable[i]);
                            // }
                            for(auto it = tablelist.begin(); it != tablelist.end(); it++){
                                pair<string,vector<vectortype>> tabledata;
                                tabledata = *it;
                                if(savedTable.find(tabledata.first + "_v2") != savedTable.end()){
                                    vector<vectortype> tmptable = tabledata.second;
                                    savedTable[tabledata.first + "_v2"].push_back(tmptable[j]);
                                }else{
                                    for(int k = 0; k < tablenamelist.size(); k++){
                                        // cout << tabledata.first << endl;
                                        auto tit = find(tablenamelist[k].begin(), tablenamelist[k].end(),tabledata.first);
                                        if(tit != tablenamelist[k].end()){
                                            if(k == 0){
                                                // cout << tabledata.first << endl;
                                                vector<vectortype> tmptable = tabledata.second;
                                                // cout << i << endl;
                                                // cout << tmptable[i].intvec << endl;
                                                savedTable[tabledata.first].push_back(tmptable[i]);
                                            }else{
                                                vector<vectortype> tmptable = tabledata.second;
                                                savedTable[tabledata.first].push_back(tmptable[j]);
                                            }

                                        }
                                    }
                                }
                            }
                        }

                    }
                }else if(tablelist[joinColumn1][i].type == 2){
                    if(tablelist[joinColumn1][i].floatvec == tablelist[joinColumn2][j].floatvec){
                        for(int k = 1; k < snippet.table_filter.size(); k++){
                            string tmpcolumn1;
                            string tmpcolumn2;
                            if(snippet.table_filter[k].LV.type.size() > 0){
                                tmpcolumn1 = snippet.table_filter[k].LV.value[0];
                                tmpcolumn2 = snippet.table_filter[k].RV.value[0];
                            }else{
                                continue;
                            }
                            if(tablelist[tmpcolumn1][i].type == 1){
                                if(tablelist[tmpcolumn1][i].intvec == tablelist[tmpcolumn2][j].intvec){
                                    continue;
                                }else{
                                    savedflag = false;
                                    break;
                                }
                            }else if(tablelist[tmpcolumn1][i].type == 2){
                                if(tablelist[tmpcolumn1][i].floatvec == tablelist[tmpcolumn2][j].floatvec){
                                    continue;
                                }else{
                                    savedflag = false;
                                    break;
                                }
                            }else{
                                if(tablelist[tmpcolumn1][i].strvec == tablelist[tmpcolumn2][j].strvec){
                                    continue;
                                }else{
                                    savedflag = false;
                                    break;
                                }
                            }
                        }
                        if(savedflag){
                            // for(auto it = tablelist[0].begin(); it != tablelist[0].end(); it++){
                            //     pair<string,vector<any>> tabledata;
                            //     tabledata = *it;
                            //     vector<any> tmptable = tabledata.second;
                            //     savedTable[tabledata.first].push_back(tmptable[i]);
                            // }
                            for(auto it = tablelist.begin(); it != tablelist.end(); it++){
                                pair<string,vector<vectortype>> tabledata;
                                tabledata = *it;
                                if(savedTable.find(tabledata.first + "_v2") != savedTable.end()){
                                    vector<vectortype> tmptable = tabledata.second;
                                    savedTable[tabledata.first + "_v2"].push_back(tmptable[j]);
                                }else{
                                    vector<vectortype> tmptable = tabledata.second;
                                    savedTable[tabledata.first].push_back(tmptable[j]);
                                }
                            }
                        }
                    }

                }else if(tablelist[joinColumn1][i].type == 0){
                    if(tablelist[joinColumn1][i].strvec == tablelist[joinColumn2][j].strvec){
                        for(int k = 1; k < snippet.table_filter.size(); k++){
                            string tmpcolumn1;
                            string tmpcolumn2;
                            if(snippet.table_filter[k].LV.type.size() > 0){
                                tmpcolumn1 = snippet.table_filter[k].LV.value[0];
                                tmpcolumn2 = snippet.table_filter[k].RV.value[0];
                            }else{
                                continue;
                            }
                            if(tablelist[tmpcolumn1][i].type == 1){
                                if(tablelist[tmpcolumn1][i].intvec == tablelist[tmpcolumn2][j].intvec){
                                    continue;
                                }else{
                                    savedflag = false;
                                    break;
                                }
                            }else if(tablelist[tmpcolumn1][i].type == 2){
                                if(tablelist[tmpcolumn1][i].floatvec == tablelist[tmpcolumn2][j].floatvec){
                                    continue;
                                }else{
                                    savedflag = false;
                                    break;
                                }
                            }else{
                                if(tablelist[tmpcolumn1][i].strvec == tablelist[tmpcolumn2][j].strvec){
                                    continue;
                                }else{
                                    savedflag = false;
                                    break;
                                }
                            }
                        }
                        if(savedflag){
                            // for(auto it = tablelist[0].begin(); it != tablelist[0].end(); it++){
                            //     pair<string,vector<any>> tabledata;
                            //     tabledata = *it;
                            //     vector<any> tmptable = tabledata.second;
                            //     savedTable[tabledata.first].push_back(tmptable[i]);
                            // }
                            for(auto it = tablelist.begin(); it != tablelist.end(); it++){
                                pair<string,vector<vectortype>> tabledata;
                                tabledata = *it;
                                if(savedTable.find(tabledata.first + "_v2") != savedTable.end()){
                                    vector<vectortype> tmptable = tabledata.second;
                                    savedTable[tabledata.first + "_v2"].push_back(tmptable[j]);
                                }else{
                                    for(int k = 0; k < tablenamelist.size(); k++){
                                        auto tit = find(tablenamelist[k].begin(), tablenamelist[k].end(),tabledata.first);
                                        if(tit != tablenamelist[k].end()){
                                            if(k == 0){
                                                vector<vectortype> tmptable = tabledata.second;
                                                savedTable[tabledata.first].push_back(tmptable[i]);
                                            }else{
                                                vector<vectortype> tmptable = tabledata.second;
                                                savedTable[tabledata.first].push_back(tmptable[j]);
                                            }

                                        }
                                    }
                                    // vector<vectortype> tmptable = tabledata.second;
                                    // savedTable[tabledata.first].push_back(tmptable[j]);
                                }
                            }
                        }

                    }

                }

            }
        }
    // cout << "end" << endl;
    time_t t1 = time(0);
    cout  << "[Merge Query Manager] End Join Snippet 4-" << snippet.work_id <<" Time : " << t1 << endl;
    buff.SaveTableData(snippet.query_id,snippet.tableAlias,savedTable);

}

// void NaturalJoin(SnippetStruct snippet, BufferManager &buff){
//     //조인 조건 없음 컬럼명이 같은 모든 데이터 비교해서 같으면 조인
// }

// void OuterFullJoin(SnippetStruct snippet, BufferManager &buff){
//     //같은 값이 없을경우 null을 채워 모든 데이터 유지
// }

// void OuterLeftJoin(SnippetStruct snippet, BufferManager &buff){
//     //왼쪽 테이블 기준으로 같은 값이 없으면 null을 채움
// }

// void OuterRightJoin(SnippetStruct snippet, BufferManager &buff){
//     //오른쪽 테이블 기준으로 같은 값이 없으면 null을 채움
// }

// void CrossJoin(SnippetStruct snippet, BufferManager &buff){
//     //테이블 곱 조인
// }

// void InnerJoin(SnippetStruct snippet, BufferManager &buff){

// }

void GroupBy(SnippetStruct& snippet, BufferManager &buff){
    int groupbycount = snippet.groupBy.size();
    unordered_map<string,vector<vectortype>> table = GetBufMTable(snippet.tablename[0], snippet, buff);
    //무슨 테이블로 저장이 되어있을지에 대한 논의 필요(스니펫)

    // for(auto it = table.begin(); it != table.end(); it++){
    //     pair<string,vector<vectortype>> tmpp;
    //     tmpp = *it;
    //     for(int i = 0; i < tmpp.second.size(); i++){
    //         cout << tmpp.second[i].floatvec << " " << tmpp.second[i].type << endl;
    //     }
    // }

    // unordered_map<>
    unordered_map<string,groupby> groupbymap;
    for(int i = 0; i < table[snippet.groupBy[0]].size(); i++){
        string key = "";
        vector<vectortype> tmpsavedkey;
        for(int j = 0; j < groupbycount; j++){
            if(table[snippet.groupBy[j]][i].type == 1){
                string tmpstring = to_string(table[snippet.groupBy[j]][i].intvec);
                key = key + tmpstring + ",";
                // cout << 2 << endl;
            }else if(table[snippet.groupBy[j]][i].type == 2){
                // std::stringstream sstream;
                // sstream << table[snippet.groupBy[j]][i].floatvec;
                string tmpstring = to_string(table[snippet.groupBy[j]][i].floatvec);
                key = key + tmpstring + ",";
                // cout << 1 << endl;
            }else{
                key += table[snippet.groupBy[j]][i].strvec;
                key += ",";
                // cout << key << endl;
            }
            tmpsavedkey.push_back(table[snippet.groupBy[j]][i]);
        }
        if(groupbymap.find(key) == groupbymap.end()){
            // cout << "findkey : " << key << endl;
            groupby tmpgroupby;
            tmpgroupby.count = 0;
            // vectortype tmpvt;
            // tmpvt.type = 2;
            // tmpgroupby.value.push_back(tmpvt);
            groupbymap.insert(make_pair(key,tmpgroupby));
        }
        for(int j = 0; j < snippet.columnProjection.size(); j++){
            // cout << j << endl;
            if(snippet.columnProjection[j][0].value == "0"){
                // cout << 11 << endl;
                if(groupbymap[key].count > snippet.columnProjection.size() - 1){
                    // break;
                    continue;
                }else{
                    //산술연산 진행
                    // cout << "else" << endl;
                    groupbymap[key].value.push_back(table[snippet.columnProjection[j][1].value][i]);
                    // cout << groupbymap[key].count << endl;
                    groupbymap[key].count = groupbymap[key].count + 1;
                    // cout << groupbymap[key].count << endl;
                }
            }else{
                // cout << 21 << endl;
                // cout << groupbymap[key].count  << endl;
                //sum
                if(groupbymap[key].count > snippet.columnProjection.size() - 1){
                    //덧셈 연산
                    // cout << groupbymap[key].value[1].type << endl;
                    if(groupbymap[key].value[1].type == 2){
                        groupbymap[key].value[1].floatvec = groupbymap[key].value[1].floatvec + table[snippet.columnProjection[j][1].value][i].floatvec;
                        // cout << groupbymap[key].value[1].floatvec << endl;
                    }
                }else{
                    // 값 넣기
                    // cout << "else" << endl;
                    groupbymap[key].value.push_back(table[snippet.columnProjection[j][1].value][i]);
                    // cout <<table[snippet.columnProjection[j][1].value][i].floatvec << endl;
                    //  cout <<table[snippet.columnProjection[j][1].value][i].type << endl;
                    //  cout << snippet.columnProjection[j][1].value << endl;
                     groupbymap[key].count = groupbymap[key].count + 1;
                    //  cout << groupbymap[key].count << endl;
                }
            }
        }
    }







    //     groupbymap[key].savedkey = tmpsavedkey;
    //     for(int j = 0; j < snippet.columnProjection.size(); j++){
    //         if(snippet.columnProjection[j][0].value == "0"){
    //             if(groupbymap.find(key) == groupbymap.end()){
    //                 groupby tmpgroupby;
    //                 tmpgroupby.count = 0;
    //                 groupbymap.insert(make_pair(key,tmpgroupby));
    //             }else{
    //                 break;
    //             }
    //             if(groupbymap[key].value.size() < j + 1){
    //                 // groupbymap[key].value.push_back(table[])
    //                 //0이라도 산술연산 가능성 있음
    //                 stack<vectortype> tmpstack;
    //                 for(int k = 1; k < snippet.columnProjection[j].size(); k++){
    //                     if(snippet.columnProjection[j][k].type == 2){
    //                         //산술연산자 +-*/
    //                         vectortype tmp1 = tmpstack.top();
    //                         tmpstack.pop();
    //                         vectortype tmp2 = tmpstack.top();
    //                         tmpstack.pop();
    //                         if(snippet.columnProjection[j][k].value == "+"){
    //                             // tmpstack.push()
    //                             if(tmp1.type == 1){
    //                                 vectortype tmpvect;
    //                                 tmpvect.intvec = tmp1.intvec + tmp2.intvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else if(tmp1.type == 2){
    //                                 vectortype tmpvect;
    //                                 tmpvect.floatvec = tmp1.floatvec + tmp2.floatvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else{
    //                                 //string이 산술연산이 가능한가?   
    //                             }
    //                         }else if(snippet.columnProjection[j][k].value == "-"){
    //                             if(tmp1.type == 1){
    //                                 vectortype tmpvect;
    //                                 tmpvect.intvec = tmp1.intvec - tmp2.intvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else if(tmp1.type == 2){
    //                                 vectortype tmpvect;
    //                                 tmpvect.floatvec = tmp1.floatvec - tmp2.floatvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else{
    //                                 //string이 산술연산이 가능한가?   
    //                             }
    //                         }else if(snippet.columnProjection[j][k].value == "*"){
    //                             if(tmp1.type == 1){
    //                                 vectortype tmpvect;
    //                                 tmpvect.intvec = tmp1.intvec * tmp2.intvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else if(tmp1.type == 2){
    //                                 vectortype tmpvect;
    //                                 tmpvect.floatvec = tmp1.floatvec * tmp2.floatvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else{
    //                                 //string이 산술연산이 가능한가?   
    //                             }                            
    //                         }else if(snippet.columnProjection[j][k].value == "/"){
    //                             if(tmp1.type == 1){
    //                                 vectortype tmpvect;
    //                                 tmpvect.intvec = tmp1.intvec / tmp2.intvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else if(tmp1.type == 2){
    //                                 vectortype tmpvect;
    //                                 tmpvect.floatvec = tmp1.floatvec / tmp2.floatvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else{
    //                                 //string이 산술연산이 가능한가?   
    //                             }                              
    //                         }
    //                     }else if(snippet.columnProjection[j][k].type == 10){
    //                         //컬럼
    //                         tmpstack.push(table[snippet.columnProjection[j][k].value][i]);
    //                     }else{
    //                         //벨류
    //                         vectortype tmpvect;
    //                         // if()
    //                         tmpstack.push(tmpvect);
    //                         // tmpstack.push(snippet.columnProjection[j][k].value);
    //                     }
    //                 }
    //                 groupbymap[key].value.push_back(tmpstack.top());
    //             }
    //             continue;
    //         }else{
    //             stack<vectortype> tmpstack;
    //             for(int k = 1; k < snippet.columnProjection[j].size(); k++){
    //                 if(snippet.columnProjection[j][k].type == 2){
    //                         //산술연산자 +-*/
    //                         vectortype tmp1 = tmpstack.top();
    //                         tmpstack.pop();
    //                         vectortype tmp2 = tmpstack.top();
    //                         tmpstack.pop();
    //                         if(snippet.columnProjection[j][k].value == "+"){
    //                             if(tmp1.type == 1){
    //                                 vectortype tmpvect;
    //                                 tmpvect.intvec = tmp1.intvec + tmp2.intvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else if(tmp1.type == 2){
    //                                 vectortype tmpvect;
    //                                 tmpvect.floatvec = tmp1.floatvec + tmp2.floatvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else{
    //                                 //string이 산술연산이 가능한가?   
    //                             }
    //                         }else if(snippet.columnProjection[j][k].value == "-"){
    //                             if(tmp1.type == 1){
    //                                 vectortype tmpvect;
    //                                 tmpvect.intvec = tmp1.intvec - tmp2.intvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else if(tmp1.type == 2){
    //                                 vectortype tmpvect;
    //                                 tmpvect.floatvec = tmp1.floatvec - tmp2.floatvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else{
    //                                 //string이 산술연산이 가능한가?   
    //                             }
    //                         }else if(snippet.columnProjection[j][k].value == "*"){
    //                             if(tmp1.type == 1){
    //                                 vectortype tmpvect;
    //                                 tmpvect.intvec = tmp1.intvec * tmp2.intvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else if(tmp1.type == 2){
    //                                 vectortype tmpvect;
    //                                 tmpvect.floatvec = tmp1.floatvec * tmp2.floatvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else{
    //                                 //string이 산술연산이 가능한가?   
    //                             }                            
    //                         }else if(snippet.columnProjection[j][k].value == "/"){
    //                             if(tmp1.type == 1){
    //                                 vectortype tmpvect;
    //                                 tmpvect.intvec = tmp1.intvec / tmp2.intvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else if(tmp1.type == 2){
    //                                 vectortype tmpvect;
    //                                 tmpvect.floatvec = tmp1.floatvec / tmp2.floatvec;
    //                                 tmpvect.type = tmp1.type;
    //                                 tmpstack.push(tmpvect);
    //                             }else{
    //                                 //string이 산술연산이 가능한가?   
    //                             }                             
    //                         }
    //                     }else if(snippet.columnProjection[j][k].type == 3){
    //                         //컬럼
    //                         tmpstack.push(table[snippet.columnProjection[j][k].value][i]);
    //                     }else{
    //                         //벨류
    //                         vectortype tmpvect;
    //                         tmpstack.push(tmpvect);
    //                         // tmpstack.push(snippet.columnProjection[j][k].value);
    //                     }

    //             }
    //             if(snippet.columnProjection[j][0].value == "1"){
    //                 //sum
    //                 vectortype tmpvect;
    //                 if(groupbymap[key].count == 0){
    //                     groupbymap[key].value.push_back(tmpstack.top());
    //                     groupbymap[key].count++;
    //                 }else{
    //                     if(groupbymap[key].value[j].type == 1){
    //                         tmpvect.type = 1;
    //                         tmpvect.intvec = groupbymap[key].value[j].intvec + tmpstack.top().intvec;
    //                         groupbymap[key].value[j] = tmpvect;
    //                     }else if(groupbymap[key].value[j].type == 2){
    //                         tmpvect.type = 2;
    //                         tmpvect.floatvec = groupbymap[key].value[j].floatvec + tmpstack.top().floatvec;
    //                         groupbymap[key].value[j] = tmpvect;
    //                         // groupbymap[key].value[j] = any_cast<float>(groupbymap[key].value[j]) + any_cast<float>(tmpstack.top());
    //                     }
    //                 }
    //             }else if(snippet.columnProjection[j][0].value == "2"){
    //                 //average
    //                 vectortype tmpvect;
    //                 if(groupbymap[key].count == 0){
    //                     groupbymap[key].value.push_back(tmpstack.top());
    //                     groupbymap[key].count++;
    //                 }else{
    //                     if(groupbymap[key].value[j].type == 1){
    //                         tmpvect.type = 1;
    //                         tmpvect.intvec = groupbymap[key].value[j].intvec + tmpstack.top().intvec;
    //                         groupbymap[key].value[j] = tmpvect;
    //                     }else if(groupbymap[key].value[j].type == 2){
    //                         tmpvect.type = 2;
    //                         tmpvect.floatvec = groupbymap[key].value[j].floatvec + tmpstack.top().floatvec;
    //                         groupbymap[key].value[j] = tmpvect;
    //                     }
    //                     groupbymap[key].count++;
    //                 }
    //             }
    //         }
    //     }
    // }
    //여기서 average 계산 후 저장
    unordered_map<string,vector<vectortype>> savedtable;
    auto it = groupbymap.begin();
    pair<string,groupby> tmpp = *it;
        // cout << "end gr" << endl;
        for(int k = 0; k < tmpp.second.value.size(); k++){
            vector<vectortype> tmpvector;
            // cout << k << endl;
            for(auto j = groupbymap.begin(); j != groupbymap.end(); j++){
                pair<string,groupby> tmppair = *j;
                // cout << tmppair.second.value.size() << endl;
                tmpvector.push_back(tmppair.second.value[k]);
            }
            savedtable.insert(make_pair(snippet.column_alias[k],tmpvector));
        }
    // buff.DeleteTableData(snippet.query_id,snippet.tableAlias);
    buff.SaveTableData(snippet.query_id,snippet.tableAlias,savedtable);
    // cout << "af st" << endl;
}

// void Having(SnippetStruct& snippet, BufferManager &buff){
//     //일반적인 필터링과 다른게 뭘까?(그룹바이 어그리게이션을 미리 해놓는다면) --> 서브쿼리 존재 가능
//     //그룹바이와 해빙은 따로 처리를 해야함(다른 스니펫)
//     //드라이빙 테이블은 무엇인가?
//     unordered_map<string,vector<any>> totaltable;
//     for(int i = 0; i < snippet.tablename.size(); i++){
//         unordered_map<string,vector<any>> table = GetBufMTable(snippet.tablename[i], snippet, buff);
//         for(auto it = table.begin(); it != table.end(); i++){
//             pair<string,vector<any>> pair;
//             pair = *it;
//             totaltable.insert(pair);
//         }
//     }
//     for(int i = 0; i < snippet.table_Having.Size(); i++){

//     }
    
// }

void OrderBy(SnippetStruct& snippet, BufferManager &buff){
    time_t t = time(0);
    cout << t << " Strat Order By Column : " << snippet.orderBy[0] << endl;
    cout << snippet.tableAlias << endl;
    unordered_map<string,vector<vectortype>> table = GetBufMTable(snippet.tableAlias, snippet, buff);
    int ordercount = snippet.orderBy.size();
    vector<sortclass> sortbuf;
    for(int i = 0; i < table[snippet.orderBy[0]].size(); i++){
        sortclass tmpclass;
        unordered_map<string,vectortype> value;
        for(int j = 0; j < ordercount; j++){
            tmpclass.ordername.push_back(snippet.orderBy[j]);
            tmpclass.ordertype.push_back(snippet.orderType[j]);
        }
        for(auto j = table.begin(); j != table.end(); j++){
            pair<string,vector<vectortype>> tmppair = *j;
            value.insert(make_pair(tmppair.first,tmppair.second[i]));
        }
        tmpclass.value = value;
        tmpclass.ordercount = ordercount;
        sortbuf.push_back(tmpclass);
    }
    sort(sortbuf.begin(),sortbuf.end());

    unordered_map<string,vector<vectortype>> orderedtable;
    for(int i = 0; i < sortbuf.size(); i++){
        // orderedtable.insert(make_pair())
        for(auto j = sortbuf[i].value.begin(); j != sortbuf[i].value.end(); j++){
            pair<string,vectortype> tmppair = *j;
            if(i == 0){
                vector<vectortype> tmpvector;
                orderedtable.insert(make_pair(tmppair.first,tmpvector));
            }
            orderedtable[tmppair.first].push_back(tmppair.second);
        }
    }
    buff.DeleteTableData(snippet.query_id,snippet.tableAlias);
    buff.SaveTableData(snippet.query_id,snippet.tableAlias,orderedtable);
    time_t t1 = time(0);
    cout << t1 << " End Order By"<< endl;
}





// void GetAccessData()
// { // access lib 구현 후 작성(구현 x)
// }

// void ColumnProjection(SnippetStruct snippet)
// {
//     int nullsize = 0; //논널비트에 대한 정보 테이블 매니저에 추가 해야함
//     //결과를 저장할 벡터 + 데이터를 가져올 벡터 필요
//     // unordered_map<string, int> typemap;
//     snippet.tabledata.clear();
//     snippet.resultstack.clear();
//     snippet.resultdata.clear();
//     for (int i = 0; i < snippet.tableAlias.size(); i++)
//     {
//         VectorType vectortype;
//         vectortype.type = snippet.savetype[i];
//         snippet.resultdata.insert(make_pair(snippet.tableAlias[i], vectortype));
//         StackType stacktype;
//         stacktype.type = snippet.savetype[i];
//         snippet.resultstack.insert(make_pair(snippet.tableAlias[i], stacktype));
//         // snippet.resultstack.insert(make_pair(snippet.tableAlias[i], StackType{}));
//     }
//     for (int i = 0; i < snippet.table_col.size(); i++)
//     {
//         // typemap.insert(make_pair(snippet.table_col[i], snippet.table_datatype[i]));
//         VectorType tmpvector;

//         snippet.tabledata.insert(make_pair(snippet.table_col[i], tmpvector));
//     }
//     // for (int n = 0; n < snippet.tableblocknum; n++)
//     // {
//     for (int i = 0; i < snippet.columnProjection.size(); i++)
//     {
//         // vector<string> a = buffermanager.gettable(snippet.tableProjection[i])
//         // stack<string> tmpstack;
//         switch (atoi(snippet.columnProjection[i][0].value.c_str()))
//         {
//         case KETI_Column_name:
//             for (int j = 1; j < snippet.columnProjection[i].size(); j++)
//             {
//                 for (int k = 0; k < snippet.tablerownum; k++)
//                 {
//                     switch (snippet.columnProjection[i][j].type)
//                     {
//                     case PROJECTION_STRING:
//                         //진짜 string or decimal
//                         /* code */
//                         break;
//                     case PROJECTION_INT:
//                         /* code */
//                         break;
//                     case PROJECTION_FLOAT:
//                         /* code */
//                         break;
//                     case PROJECTION_COL:
//                         //버퍼매니저에서 데이터 가져와야함
//                         /* code */
//                         // snippet.tabledata[snippet.tableProjection[i][j].value].type
//                         // if(typemap[snippet.tableProjection[i][j].value] == 3){
//                         //     // buffermanager.gettable()


//                         // }else if(typemap[snippet.tableProjection[i][j].value] == 246){

//                         // }else if(typemap[snippet.tableProjection[i][j].value] == 14){

//                         // }else if(typemap[snippet.tableProjection[i][j].value] == 4){

//                         // }
//                         if(snippet.resultstack[snippet.tableAlias[i]].type == 1){ //int
                            
//                         }else if(snippet.resultstack[snippet.tableAlias[i]].type == 2){ //float

//                         }

//                         break;
//                     case PROJECTION_OPER:
//                         break;
//                     default:
//                         break;
//                     }
//                 }
//             }

//             /* code */
//             break;
//         case KETI_SUM:
//             /* code */
//             break;
//         case KETI_AVG:
//             /* code */
//             break;
//         case KETI_COUNT:
//             /* code */
//             break;
//         case KETI_COUNTALL:
//             /* code */
//             break;
//         case KETI_MIN:
//             /* code */
//             break;
//         case KETI_MAX:
//             /* code */
//             break;
//         case KETI_CASE:
//             /* code */
//             break;
//         case KETI_WHEN:
//             /* code */
//             break;
//         case KETI_THEN:
//             /* code */
//             break;
//         case KETI_ELSE:
//             /* code */
//             break;
//         case KETI_LIKE:
//             /* code */
//             break;
//         default:
//             break;
//         }
//         // }
//     }
// }

// void GetColOff()
// {
// }