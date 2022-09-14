#include "buffer_manager.h"

void getColOffset(const char* row_data, int* col_offset_list, vector<int> return_datatype, vector<int> table_offlen);

int BufferManager::InitBufferManager(Scheduler &scheduler){
    BufferManager_Input_Thread = thread([&](){BufferManager::BlockBufferInput();});
    BufferManager_Thread = thread([&](){BufferManager::BufferRunning(scheduler);});
    return 0;
}

int BufferManager::Join(){
    BufferManager_Input_Thread.join();
    BufferManager_Thread.join();
    return 0;
}

void BufferManager::BlockBufferInput(){
    int server_fd, client_fd;
	int opt = 1;
	struct sockaddr_in serv_addr, client_addr;
	socklen_t addrlen = sizeof(client_addr);
    static char cMsg[] = "ok";

	if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0){
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))){
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
	
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons(PORT_BUF); // port
 
	if (bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0){
		perror("bind");
		exit(EXIT_FAILURE);
	} 

	if (listen(server_fd, NCONNECTION) < 0){
		perror("listen");
		exit(EXIT_FAILURE);
	}

	while(1){
		if ((client_fd = accept(server_fd, (struct sockaddr*)&client_addr, (socklen_t*)&addrlen)) < 0){
			perror("accept");
        	exit(EXIT_FAILURE);
		}

		std::string json = "";//크기?
        int njson;
		size_t ljson;

		recv( client_fd , &ljson, sizeof(ljson), 0);

        char buffer[ljson] = {0};
		
		while(1) {
			if ((njson = recv(client_fd, buffer, BUFF_SIZE-1, 0)) == -1) {
				perror("read");
				exit(1);
			}
			ljson -= njson;
		    buffer[njson] = '\0';
			json += buffer;

		    if (ljson == 0)
				break;
		}
		
        send(client_fd, cMsg, strlen(cMsg), 0);

		char data[BUFF_SIZE];//크기?
        char* dataiter = data;
		memset(data, 0, BUFF_SIZE);
        int ndata = 0;
        int totallen = 0;
        size_t ldata = 0;
        recv(client_fd , &ldata, sizeof(ldata),0);
        totallen = ldata;

		while(1) {
			if ((ndata = recv( client_fd , dataiter, ldata,0)) == -1) {
				perror("read");
				exit(1);
			}
            dataiter = dataiter+ndata;
			ldata -= ndata;

		    if (ldata == 0)
				break;
		}

        send(client_fd, cMsg, strlen(cMsg), 0);

		BlockResultQueue.push_work(BlockResult(json.c_str(), data));		
        
        close(client_fd);		
	}   
	close(server_fd);
}

void BufferManager::BufferRunning(Scheduler &scheduler){
    while (1){
        BlockResult blockResult = BlockResultQueue.wait_and_pop();

        printf("Receive Data from CSD Return Interface {ID : %d-%d}\n", blockResult.query_id,blockResult.work_id);

        if((m_BufferManager.find(blockResult.query_id) == m_BufferManager.end()) || 
           (m_BufferManager[blockResult.query_id]->work_buffer_list.find(blockResult.work_id) 
                    == m_BufferManager[blockResult.query_id]->work_buffer_list.end())){
            cout << "<error> Work(" << blockResult.query_id << "-" << blockResult.work_id << ") Initialize First!" << endl;
        }   

        MergeBlock(blockResult, scheduler);
    }
}

void BufferManager::MergeBlock(BlockResult result, Scheduler &scheduler){
    int qid = result.query_id;
    int wid = result.work_id;

    Work_Buffer* myWorkBuffer = m_BufferManager[qid]->work_buffer_list[wid];

    unique_lock<mutex> lock(myWorkBuffer->mu);

    //작업 종료된 id의 데이터인지 확인(종료된게 들어오면 오류임)
    if(myWorkBuffer->is_done){
        cout << "<error> Work(" << qid << "-" << wid << ") Is Already Done!" << endl;
        return;
    }

    // // 테스트 출력
    // cout << "---------------Merged Block Info-----------------" << endl;
    // cout << "| work id: " << result.work_id << " | length: " << result.length
    //         << " | row count: " << result.row_count << endl;
    // cout << "result.length: " << result.length << endl;
    // cout << "result.row_offset: ";
    // for(int i = 0; i < result.row_offset.size(); i++){
    //     printf("%d ",result.row_offset[i]);
    // }
    // cout << "\n----------------------------------------------\n";
    // for(int i = 0; i < result.length; i++){
    //     printf("%02X ",(u_char)result.data[i]);
    // }
    // cout << "\n------------------------------------------------\n";

    //데이터가 있는지 확인
    if(result.length != 0){
        int col_type, col_offset, col_len, origin_row_len, new_row_len, col_count = 0;
        string col_name;
        vector<int> new_row_offset;
        new_row_offset.assign( result.row_offset.begin(), result.row_offset.end() );
        new_row_offset.push_back(result.length);

        //한 row씩 읽기
        for(int i=0; i<result.row_count; i++){
            origin_row_len = new_row_offset[i+1] - new_row_offset[i];
            char row_data[origin_row_len];
            memcpy(row_data,result.data+result.row_offset[i],origin_row_len);

            new_row_len = 0;
            col_count = myWorkBuffer->table_column.size();
            int col_offset_list[col_count + 1];//각 row별 컬럼 오프셋 획득
            getColOffset(row_data, col_offset_list, myWorkBuffer->return_datatype, myWorkBuffer->table_offlen);
            col_offset_list[col_count] = origin_row_len;

            // //테스트 출력
            // cout << "row_col_offset:[";
            // for(int j = 0; j < col_count + 1; j++){
            //     printf("%d ",col_offset_list[j]);
            // }
            // cout << "]" << endl;

            //한 column씩 읽으며 vector에 저장
            for(int j=0; j<myWorkBuffer->table_column.size(); j++){
                col_name = myWorkBuffer->table_column[j];
                col_offset = col_offset_list[j];
                col_len = col_offset_list[j+1] - col_offset_list[j];
                col_type = myWorkBuffer->return_datatype[j];

                vectortype tmpvect;
                switch (col_type){
                    case MySQL_BYTE:{
                        char tempbuf[col_len];//col_len = 1
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        int64_t my_value = *((int8_t *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        // //테스트출력
                        // cout << "col_data: ";
                        // for(int k = 0; k < col_len; k++){
                        //     printf("%02X ",(u_char)tempbuf[k]);
                        // }
                        // cout << endl;
                        // cout << "vec_data: " << my_value << endl;
                        break;
                    }case MySQL_INT16:{
                        char tempbuf[col_len];//col_len = 2
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        int64_t my_value = *((int16_t *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        // //테스트출력
                        // cout << "col_data: ";
                        // for(int k = 0; k < col_len; k++){
                        //     printf("%02X ",(u_char)tempbuf[k]);
                        // }
                        // cout << endl;
                        // cout << "vec_data: " << my_value << endl;                        
                        break;
                    }case MySQL_INT32:{
                        char tempbuf[col_len];//col_len = 4
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        int64_t my_value = *((int32_t *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        // //테스트출력
                        // cout << "col_data: ";
                        // for(int k = 0; k < col_len; k++){
                        //     printf("%02X ",(u_char)tempbuf[k]);
                        // }
                        // cout << endl;
                        // cout << "vec_data: " << my_value << endl;  
                        break;
                    }case MySQL_INT64:{
                        char tempbuf[col_len];//col_len = 8
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        int64_t my_value = *((int64_t *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        // //테스트출력
                        // cout << "col_data: ";
                        // for(int k = 0; k < col_len; k++){
                        //     printf("%02X ",(u_char)tempbuf[k]);
                        // }
                        // cout << endl;
                        // cout << "vec_data: " << my_value << endl;  
                        break;
                    }case MySQL_FLOAT32:{
                        //아직 처리X
                        char tempbuf[col_len];//col_len = 4
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        double my_value = *((float *)tempbuf);
                        tmpvect.type = 2;
                        tmpvect.floatvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        break;
                    }case MySQL_DOUBLE:{
                        //아직 처리X
                        char tempbuf[col_len];//col_len = 8
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        double my_value = *((double *)tempbuf);
                        tmpvect.type = 2;
                        tmpvect.floatvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        break;
                    }case MySQL_NEWDECIMAL:{
                        //decimal(15,2)만 고려한 상황 -> col_len = 7 (integer:6/real:1)
                        char tempbuf[col_len];//col_len = 7
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        bool is_negative = false;
                        if(std::bitset<8>(tempbuf[0])[7] == 0){//음수일때 not +1
                            is_negative = true;
                            for(int i = 0; i<7; i++){
                                tempbuf[i] = ~tempbuf[i];//not연산
                            }
                            // tempbuf[6] = tempbuf[6] +1;//+1
                        }   
                        char integer[8];
                        char real[1];
                        for(int k=0; k<4; k++){
                            integer[k] = tempbuf[5-k];
                        }
                        real[0] = tempbuf[6];
                        int64_t ivalue = *((int64_t *)integer); 
                        double rvalue = *((int8_t *)real); 
                        rvalue *= 0.01;
                        double my_value = ivalue + rvalue;
                        if(is_negative){
                            my_value *= -1;
                        }
                        tmpvect.type = 2;
                        tmpvect.floatvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        // //테스트출력
                        // cout << "col_data: ";
                        // for(int k = 0; k < col_len; k++){
                        //     printf("%02X ",(u_char)tempbuf[k]);
                        // }
                        // cout << endl;
                        // cout << "vec_data: " << my_value << endl;  
                        break;
                    }case MySQL_DATE:{
                        char tempbuf[col_len+1];//col_len = 3
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        tempbuf[3] = 0x00;
                        int64_t my_value = *((int *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        // //테스트출력
                        // cout << "col_data: ";
                        // for(int k = 0; k < col_len; k++){
                        //     printf("%02X ",(u_char)tempbuf[k]);
                        // }
                        // cout << endl;
                        // cout << "vec_data: " << my_value << endl;  
                        break;
                    }case MySQL_TIMESTAMP:{
                        //아직 처리X
                        char tempbuf[col_len];//col_len = 4
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        int my_value = *((int *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        break;
                    }case MySQL_STRING:
                     case MySQL_VARSTRING:{
                        char tempbuf[col_len+1];
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        tempbuf[col_len] = '\0';
                        string my_value(tempbuf);
                        tmpvect.type = 0;
                        tmpvect.strvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        // //테스트출력
                        // cout << "col_data: ";
                        // for(int k = 0; k < col_len; k++){
                        //     printf("%02X ",(u_char)tempbuf[k]);
                        // }
                        // cout << endl;
                        // cout << "vec_data: " << my_value << endl;  
                        break;
                    }default:{
                        cout << "<error> Type:" << col_type << " Is Not Defined!!" << endl;
                    }
                }
            }
        }
    }

    //남은 블록 수 조정
    // cout << "(before left/result cnt/after left)|(" << myWorkBuffer->left_block_count << "/" << result.result_block_count << "/" << myWorkBuffer->left_block_count-result.result_block_count << ")" << endl;
    scheduler.csdworkdec(result.csd_name, result.result_block_count);
    myWorkBuffer->left_block_count -= result.result_block_count;
    printf("[Buffer Manager] Merging Data ... (Left Block : %d)\n",myWorkBuffer->left_block_count);

    //블록을 다 처리했는지 개수 확인
    if(myWorkBuffer->left_block_count == 0){
        // cout << "Storage Engine Instance:Buffer Manager finish the work ID(" << result.query_id << ":" << result.work_id << ")" << endl;
        printf("[Buffer Manager] Merging Data {ID : %d-%d} Done (Table : %s)\n\n",qid,wid,myWorkBuffer->table_alias);
        myWorkBuffer->is_done = true;
        m_BufferManager[qid]->table_status[myWorkBuffer->table_alias].second = true;
        myWorkBuffer->cond.notify_all();
    }
}

int BufferManager::InitWork(int qid, int wid, string table_alias,
                            vector<string> table_column_, vector<int> return_datatype,
                            vector<int> table_offlen_, int total_block_cnt_){
    // cout << "#Init Work! [" << qid << "-" << wid << "] (BufferManager)" << endl;

    if(m_BufferManager.find(qid) == m_BufferManager.end()){
        InitQuery(qid);
    }else if(m_BufferManager[qid]->work_buffer_list.find(wid) 
                != m_BufferManager[qid]->work_buffer_list.end()){
        cout << "<error> Work ID Duplicate Error" << endl;
        return -1;            
    }   

    Work_Buffer* workBuffer = new Work_Buffer(table_alias, table_column_, return_datatype, 
                                                table_offlen_, total_block_cnt_);
    
    m_BufferManager[qid]->work_cnt++;
    m_BufferManager[qid]->work_buffer_list[wid]= workBuffer;
    m_BufferManager[qid]->table_status[table_alias] = make_pair(wid,false);

    return 1;
}

void BufferManager::InitQuery(int qid){
    // cout << "#Init Query! [" << qid << "] (BufferManager)" << endl;

    Query_Buffer* queryBuffer = new Query_Buffer(qid);
    m_BufferManager.insert(pair<int,Query_Buffer*>(qid,queryBuffer));
}

int BufferManager::CheckTableStatus(int qid, string tname){
    if(m_BufferManager.find(qid) == m_BufferManager.end()){
        return QueryIDError;//쿼리 ID 오류
    }else if(m_BufferManager[qid]->table_status.find(tname) == m_BufferManager[qid]->table_status.end()){
        return NonInitTable;//Init한 테이블이 없음
    }else if(!m_BufferManager[qid]->table_status[tname].second){
        return NotFinished;//Init 되었지만 작업 아직 안끝남
    }else{
        return WorkDone;//작업 완료되어 테이블 데이터 존재
    }
}

TableInfo BufferManager::GetTableInfo(int qid, string tname){
    int status = CheckTableStatus(qid,tname);
    cout << "[Buffer Manager] Get Table Info  Tname : " << tname << endl;
    TableInfo tableInfo;

    if(status!=WorkDone){
        int wid = m_BufferManager[qid]->table_status[tname].first;
        Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
        unique_lock<mutex> lock(workBuffer->mu);
        workBuffer->cond.wait(lock);
    }

    int wid = m_BufferManager[qid]->table_status[tname].first;
    tableInfo.table_column = m_BufferManager[qid]->work_buffer_list[wid]->table_column;
    tableInfo.table_datatype = m_BufferManager[qid]->work_buffer_list[wid]->table_datatype;
    tableInfo.table_offlen = m_BufferManager[qid]->work_buffer_list[wid]->table_offlen;
    cout << "[Buffer Manager] Return Table Info  Tname : " << tname << endl;
    return tableInfo;
}

TableData BufferManager::GetTableData(int qid, string tname){
    //여기서 데이터 완료될때까지 대기
    int status = CheckTableStatus(qid,tname);
    TableData tableData;

    switch(status){
        case QueryIDError:
        case NonInitTable:{
            tableData.valid = false;
            break;
        }case NotFinished:{
            int wid = m_BufferManager[qid]->table_status[tname].first;
            Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
            unique_lock<mutex> lock(workBuffer->mu);
            workBuffer->cond.wait(lock);
            tableData.table_data = workBuffer->table_data;
            break;
        }case WorkDone:{
            int wid = m_BufferManager[qid]->table_status[tname].first;
            Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
            unique_lock<mutex> lock(workBuffer->mu);
            tableData.table_data = workBuffer->table_data;
            break;
        }
    }
   
    return tableData;
}

//이건 MQM에서 연산한 결과 테이블 전체 저장할 때
int BufferManager::SaveTableData(int qid, string tname, unordered_map<string,vector<vectortype>> table_data_){
    cout << "[Buffer Manager] Saved Table, Table Name : "  << tname << endl;
    cout << table_data_.size() << endl;
    for(auto it = table_data_.begin(); it != table_data_.end(); it++){
    pair<string,vector<vectortype>> pair;
    pair = *it;
    cout << pair.first << " " << pair.second.size()<< endl;
    // tablelist.insert(pair);
    }
    int wid = m_BufferManager[qid]->table_status[tname].first;
    Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
    unique_lock<mutex> lock(workBuffer->mu);

    m_BufferManager[qid]->work_buffer_list[wid]->table_data = table_data_;
    m_BufferManager[qid]->work_buffer_list[wid]->is_done = true;
    m_BufferManager[qid]->table_status[tname].second = true;

    return 1;
}

int BufferManager::DeleteTableData(int qid, string tname){
    int status = CheckTableStatus(qid,tname);
    switch(status){
        case QueryIDError:
        case NonInitTable:{
            return -1;
        }case NotFinished:{
            int wid = m_BufferManager[qid]->table_status[tname].first;
            Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
            
            unique_lock<mutex> lock(workBuffer->mu);
            workBuffer->cond.wait(lock);

            for(auto i : workBuffer->table_data){//테이블 컬럼 데이터 삭제
                i.second.clear();
            }
        }case WorkDone:{
            //꼭 MQM에서 Get Table Data 한 다음에 Delete Table Data 해야함
            int wid = m_BufferManager[qid]->table_status[tname].first;
            Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
            unique_lock<mutex> lock(workBuffer->mu);

            for(auto i : workBuffer->table_data){//테이블 컬럼 데이터 삭제
                i.second.clear();
            }
            return 0;
        }
    }

    return 1;
}

void getColOffset(const char* row_data, int* col_offset_list, vector<int> return_datatype, vector<int> table_offlen){
    int col_type = 0, col_len = 0, col_offset = 0, new_col_offset = 0, tune = 0;
    int col_count = return_datatype.size();

    for(int i=0; i<col_count; i++){
        col_type = return_datatype[i];
        col_len = table_offlen[i];
        new_col_offset = col_offset + tune;
        col_offset += col_len;
        if(col_type == MySQL_VARSTRING){
            if(col_len < 256){//0~255
                char var_len[1];
                var_len[0] = row_data[new_col_offset];
                uint8_t var_len_ = *((uint8_t *)var_len);
                tune += var_len_ + 1 - col_len;
            }else{//0~65535
                char var_len[2];
                var_len[0] = row_data[new_col_offset];
                int new_col_offset_ = new_col_offset + 1;
                var_len[1] = row_data[new_col_offset_];
                uint16_t var_len_ = *((uint16_t *)var_len);
                tune += var_len_ + 2 - col_len;
            }
        }

        col_offset_list[i] = new_col_offset;
    }
}