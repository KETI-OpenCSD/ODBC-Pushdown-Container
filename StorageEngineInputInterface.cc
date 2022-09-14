#include "StorageEngineInputInterface.h"

// int main(int argc, char const *argv[])
// {
// 	//init table manager	
//   cout << "start" << endl;
// 	tblManager.init_TableManager();
// 	// snippetmanager.InitSnippetManager(tblManager,csdscheduler,csdmanager);
// 	//init WorkID
// 	// WorkID=0;
	
//     // int server_fd;
//     // struct sockaddr_in address;
//     // int opt = 1;
//     // int addrlen = sizeof(address);
//     // testaggregation();
//     // testjoin();
//     testrun(argv[1]);
// 	// RunServer();
//     // char *hello = "Hello from server";
       
//     // Creating socket file descriptor
//     // if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
//     // {
//     //     perror("socket failed");
//     //     exit(EXIT_FAILURE);
//     // }
       
//     // Forcefully attaching socket to the port 8080
//     // if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
//     //                                               &opt, sizeof(opt)))
//     // {
//     //     perror("setsockopt");
//     //     exit(EXIT_FAILURE);
//     // }
//     // address.sin_family = AF_INET;
//     // address.sin_addr.s_addr = INADDR_ANY;
//     // address.sin_port = htons( PORT );
       
//     // Forcefully attaching socket to the port 8080
//     // if (bind(server_fd, (struct sockaddr *)&address, 
//     //                              sizeof(address))<0)
//     // {
//     //     perror("bind failed");
//     //     exit(EXIT_FAILURE);
//     // }
//     // if (listen(server_fd, NCONNECTION) < 0)
//     // {
//     //     perror("listen");
//     //     exit(EXIT_FAILURE);
//     // }
	
// 	// thread(accept_connection, server_fd).detach();

// 	// while (1);
	
// 	// close(server_fd);
// 	// bufma.Join();

//     //send(new_socket , test_buf , 1024 , 0 );
//     //printf("Hello message sent\n");
	
//     return 0;
// }
// void accept_connection(int server_fd){
// 	while (1) {
// 		int new_socket;
// 		struct sockaddr_in address;
// 		int addrlen = sizeof(address);
// 		char buffer[4096] = {0};

// 		if ((new_socket = accept(server_fd, (struct sockaddr *)&address, 
// 			(socklen_t*)&addrlen))<0)
// 		{
// 			perror("accept");
// 			exit(EXIT_FAILURE);
// 		}
		
// 		read( new_socket , buffer, 4096);
// 		std::cout << buffer << std::endl;
				
// 		//parse json	
// 		Document document;
// 		document.Parse(buffer);
//         int type = 0;
		
// 		// Value &type = document["type"]; //이게 없어짐
// 		// string full_query;
// 		// if(document.HasMember("full_query")){
// 		// 	full_query = document["full_query"].GetString();
// 		// 	if(fullquerymap.find(full_query) == fullquerymap.end()){
// 		// 		bufma.InitQuery(full_query);
// 		// 		fullquerymap.insert(make_pair(full_query,1));
// 		// 	}
// 		// }
// 		boost::thread_group tg;

//         //스니펫 받아서 타입 구분 하나 필요
//         /*
//         확인해야할것
//         1. 버퍼매니저에 해당 테이블의 정보가 있는지
//         2.1 -> 없다면, 2.2 -> 있다면
//         2.1.1 인덱스 테이블이 있는지
//         2.1.2 없다면 예측을 통해 csd로 내려가는지
//         2.1.3 있다면 커버링 인덱스인지
//         2.1.4 커버링 인덱스가 아니라면 range가 얼마나 줄어드는지
//         2.2.1 있다면 무조건 위에서 처리
//         */
//        int type = GetSnippetType(document);

// 		// key = document["key"].GetInt();
// 		switch(type){
// 			case KETI_WORK_TYPE::SE_FULL_SCAN :{

// 			}
// 			case KETI_WORK_TYPE::SE_COVERING_INDEX :{

// 			}
// 			case KETI_WORK_TYPE::CSD_FULL_SCAN : {
// 				std::cout << "Do SCAN_N_FILTER Pushdown" << std::endl;

// 				int work_id = WorkID.load();
// 				WorkID++;
				
// 				std::string req_json;
// 				std::string res_json;

// 				Value &table_name = document["table_name"];
// 				//gen req_json
				
// 				tblManager.generate_req_json(table_name.GetString(),req_json);
// 				// std::cout << req_json << std::endl;
// 				// cout << table_name.GetString() << endl;
				
// 				//do LBA2PBA
// 				my_LBA2PBA(req_json,res_json);
// 				// std::cout << res_json << std::endl;
// 				Document reqdoc;
// 				reqdoc.Parse(req_json.c_str());
// 				vector<string> sstfilename;


// 				for (int i = 0; i < reqdoc["REQ"]["Chunk List"].Size(); i ++){
// 					sstfilename.push_back(reqdoc["REQ"]["Chunk List"][i]["filename"].GetString());
// 				}
				

// 				Document blockdoc;
// 				blockdoc.Parse(res_json.c_str());

				
// 				//get table schema
// 				vector<TableManager::ColumnSchema> schema;
// 				tblManager.get_table_schema(table_name.GetString(),schema);
// 				vector<int> offset;
// 				vector<int> offlen;
// 				vector<int> datatype;
// 				vector<string> colname;
//                 vector<string> column_filtering;
//                 vector<string> Group_By;
//                 vector<string> Order_By;
//                 vector<string> Expr;
//                 vector<string> column_projection;
// 				string comma = ".";
// 				// tblManager.print_TableManager();
// 				vector<TableManager::ColumnSchema>::iterator itor = schema.begin();
// 				for(; itor != schema.end(); itor++){
// 					// std::cout << "column_name : " << (*itor).column_name << " " << (*itor).type << " " << (*itor).length << " " << (*itor).offset << std::endl;
// 					offset.push_back((*itor).offset);
// 					offlen.push_back((*itor).length);
// 					datatype.push_back((*itor).type);
// 					colname.push_back(table_name.GetString() + comma + (*itor).column_name);
// 					// cout << table_name.GetString() << endl;
// 				}

// 				Value &Blcokinfo = blockdoc["RES"]["Chunk List"];
// 				Value &filter = document["Snippet"]["table_filter"];
// 				int count = 0;
// 				// cout << Blcokinfo.Size() << endl;
// 				for(int i = 0; i < Blcokinfo.Size(); i ++){
// 					csdscheduler.threadblocknum.push_back(count);
// 					for (int j = 0; j < Blcokinfo[i][sstfilename[i].c_str()].Size(); j++){
// 						csdscheduler.blockvec.push_back(count);
// 						// cout << count << endl;
// 						count++;
// 					}
// 				}
//                 for(int i = 0; i < document["Snippet"]["column_filtering"].Size(); i++){
//                     column_filtering.push_back(document["Snippet"]["column_filtering"][i].GetString());
//                 }
//                 for(int i = 0; i < document["Snippet"]["expr"].Size(); i++){
//                     Expr.push_back(document["Snippet"]["expr"][i].GetString());
//                 }
//                 for(int i = 0; i < document["Snippet"]["Group_by"].Size(); i++){
//                     Group_By.push_back(document["Snippet"]["Group_by"][i].GetString());
//                 }
//                 for(int i = 0; i < document["Snippet"]["Order_by"].Size(); i++){
//                     Order_By.push_back(document["Snippet"]["Order_by"][i].GetString());
//                 }
//                 for(int i = 0; i < document["Snippet"]["column_projection"].Size(); i++){
//                     Order_By.push_back(document["Snippet"]["column_projection"][i].GetString());
//                 }
// 					// bufma.SetWork(work_id,csdscheduler.blockvec);
				
// 				csdscheduler.snippetdata.work_id = work_id;
// 				csdscheduler.snippetdata.table_offset = offset;
// 				csdscheduler.snippetdata.table_offlen = offlen;
// 				csdscheduler.snippetdata.table_filter = filter;
// 				csdscheduler.snippetdata.table_datatype = datatype;
// 				csdscheduler.snippetdata.sstfilelist = sstfilename;
// 				csdscheduler.snippetdata.table_col = colname;
// 				csdscheduler.snippetdata.block_info_list = Blcokinfo;
// 				csdscheduler.snippetdata.tablename = table_name.GetString();
//                 csdscheduler.snippetdata.Order_By = Order_By;
//                 csdscheduler.snippetdata.Group_By = Group_By;
//                 csdscheduler.snippetdata.Expr = Expr;
//                 csdscheduler.snippetdata.column_filtering = column_filtering;
//                 csdscheduler.snippetdata.column_projection = column_projection;

// 				for(int i = 0; i < sstfilename.size(); i++){
// 					tg.create_thread(boost::bind(&Scheduler::sched,&csdscheduler,i));
// 				}
				
// 				tg.join_all();
			
// 				csdscheduler.threadblocknum.clear();
// 				csdscheduler.blockvec.clear();
// 				// cout << table_name.GetString() << endl;
// 				//after sched
// 				send(new_socket,&work_id,sizeof(work_id),0);
// 				std::cout << "WorkID : " << work_id << std::endl;

// 				break;
// 			}
//             case KETI_WORK_TYPE::CSD_INDEX_SEEK : {

//             }
//             case KETI_WORK_TYPE::SE_MERGE_BUFFER_MANAGER : {

//             }
// 			default: {
// 				break;
// 			}
// 		}
// 		close(new_socket);
		
// 	}
// }

// int my_LBA2PBA(std::string &req_json,std::string &res_json){
// 	int sock = 0, valread;
//     struct sockaddr_in serv_addr;
//     char buffer[BUFF_SIZE] = {0};
//     if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
//     {
//         printf("\n Socket creation error \n");
//         return -1;
//     }
   
//     serv_addr.sin_family = AF_INET;
//     serv_addr.sin_port = htons(LBA2PBAPORT);
       
//     // Convert IPv4 and IPv6 addresses from text to binary form
//     if(inet_pton(AF_INET, "10.0.5.119", &serv_addr.sin_addr)<=0) //csd 정보를 통해 ip 입력(std::string 타입)
//     {
//         printf("\nInvalid address/ Address not supported \n");
//         return -1;
//     }
   
//     if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
//     {
//         printf("\nConnection Failed %s\n",strerror(errno));
// 		//sql_print_information("connect error %s", strerror(errno));
//         return -1;
//     }
	
// 	//send json
// 	size_t len = strlen(req_json.c_str());
// 	send(sock,&len,sizeof(len),0);
// 	send(sock,req_json.c_str(),strlen(req_json.c_str()),0);

// 	//read(sock,res_json,BUFF_SIZE);

// 	size_t length;
// 	read( sock , &length, sizeof(length));

// 	int numread;
// 	while(1) {
// 		if ((numread = read( sock , buffer, BUFF_SIZE - 1)) == -1) {
// 			perror("read");
// 			exit(1);
// 		}
// 		length -= numread;
// 	    buffer[numread] = '\0';
// 		res_json += buffer;

// 	    if (length == 0)
// 			break;
// 	}

// 	::close(sock);

// 	return 0;
// }



#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "WalManager.h"

#include <grpcpp/grpcpp.h>
#include <google/protobuf/empty.pb.h>
#include "snippet_sample.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using snippetsample::SnippetSample;
using snippetsample::Snippet;
using snippetsample::SnippetRequest;
using snippetsample::Result;
using snippetsample::Request;
using google::protobuf::Empty;

// Logic and data behind the server's behavior.
class SnippetSampleServiceImpl final : public SnippetSample::Service {
  Status SetSnippet(ServerContext* context,
                   ServerReaderWriter<Result, SnippetRequest>* stream) override {
    SnippetRequest snippetrequest;
    // Snippet snippet;
    queue<SnippetStruct> snippetqueue;
    while (stream->Read(&snippetrequest)) {
      //Snippet snippet = snippetrequest.snippet();
      std::string test_json;
      google::protobuf::util::JsonPrintOptions options;
      options.always_print_primitive_fields = true;
      options.always_print_enums_as_ints = true;
      google::protobuf::util::MessageToJsonString(snippetrequest,&test_json,options);
      std::cout << "Recv Snippet to JSON" << std::endl;
      //std::cout << "Snippet Type : " << snippetrequest.type() << std::endl;
      // std::cout << test_json << std::endl << std::endl;

      Document doc;
      doc.Parse(test_json.c_str());
      // SnippetStruct snippetdata;
      Value& document = doc["snippet"];
    SnippetStruct snippetdata;

    snippetdata.snippetType = doc["type"].GetInt();

    AppendTableFilter(snippetdata,document["tableFilter"]);


    if(snippetdata.snippetType == 6){
      if(document.HasMember("dependency")){
        if(document["dependency"].HasMember("dependencyProjection")){
          AppendDependencyProjection(snippetdata,document["dependency"]["dependencyProjection"]);
        }
        if(document["dependency"].HasMember("dependencyFilter")){
          AppendDependencyFilter(snippetdata,document["dependency"]["dependencyFilter"]);
        }
      }
    }


    for(int i = 0; i < document["tableName"].Size(); i++){
		  snippetdata.tablename.push_back(document["tableName"][i].GetString());
	  }
//     // cout << 2 << endl;
	  for(int i = 0; i < document["columnAlias"].Size(); i++){
		  snippetdata.column_alias.push_back(document["columnAlias"][i].GetString());
	  }
//     // cout << 2 << endl;
	  for(int i = 0; i < document["columnFiltering"].Size(); i++){
		  snippetdata.columnFiltering.push_back(document["columnFiltering"][i].GetString());
	  }
//     // cout << 2 << endl;
//     // cout << document.HasMember("orderBy") << endl;
//     // cout << document["orderBy"].GetType() << endl;
    // for(int i = 0; i < document["orderBy"].Size(); i++){
    if(document.HasMember("orderBy")){
      for(int j = 0; j < document["orderBy"]["columnName"].Size(); j++){
          snippetdata.orderBy.push_back(document["orderBy"]["columnName"][j].GetString());
          snippetdata.orderType.push_back(document["orderBy"]["ascending"][j].GetInt());
        }
    }
//     // }
//     // cout << 2 << endl;
	  for(int i = 0; i < document["groupBy"].Size(); i++){
		  snippetdata.groupBy.push_back(document["groupBy"][i].GetString());
	  }
    for(int i = 0; i < document["tableCol"].Size(); i++){
		  snippetdata.table_col.push_back(document["tableCol"][i].GetString());
	  }
    for(int i = 0; i < document["tableOffset"].Size(); i++){
		  snippetdata.table_offset.push_back(document["tableOffset"][i].GetInt());
	  }
    for(int i = 0; i < document["tableOfflen"].Size(); i++){
		  snippetdata.table_offlen.push_back(document["tableOfflen"][i].GetInt());
	  }
    for(int i = 0; i < document["tableDatatype"].Size(); i++){
		  snippetdata.table_datatype.push_back(document["tableDatatype"][i].GetInt());
	  }
//     // cout << 1 << endl;
    snippetdata.tableAlias = document["tableAlias"].GetString();
//     // cout << document.HasMember("queryID") << endl;
    snippetdata.query_id = document["queryID"].GetInt();
//     // cout << 4 << endl;
    snippetdata.work_id = document["workID"].GetInt();
//     // cout << 5 << endl;
    AppendProjection(snippetdata, document["columnProjection"]);
//     // cout << 6 << endl;
//     // cout << 2 << endl;
    snippetqueue.push(snippetdata);
//     // cout << snippetqueue.front().table_filter.Size() << endl;
//     // tmpque.enqueue(snippetdata);
//   }

      
      
      if(snippetrequest.type() == 0) {
        WalManager test(snippetrequest.snippet());
        test.run();
      }
    }
    string LQNAME = snippetqueue.back().tableAlias;
    int Queryid = snippetqueue.back().query_id;
    snippetmanager.NewQuery(snippetqueue,bufma,tblManager,csdscheduler,csdmanager);
    // snippetmanager.NewQuery(tmpque,bufma,tblManager,csdscheduler,csdmanager);
    // thread t1 = thread(&SnippetManager::NewQuery,&snippetmanager,snippetqueue,bufma,tblManager,csdscheduler,csdmanager);
    // t1.detach();
    // cout << 1 << endl;
    // unordered_map<string,vector<vectortype>> rrr = bufma.GetTableData(4,"snippet4-12").table_data;
    // cout << 2 << endl;
    unordered_map<string,vector<vectortype>> RetTable = bufma.GetTableData(Queryid,LQNAME).table_data;
    // unordered_map<string,vector<vectortype>> RetTable = snippetmanager.ReturnResult(4);
    cout << "+-------------------------+" << endl;
    cout << " ";
    for(auto i = RetTable.begin(); i != RetTable.end(); i++){
      pair<string,vector<vectortype>> tmppair = *i;
      cout << tmppair.first << "      ";
    }
    cout << endl;
    return Status::OK;
  }
  Status Run(ServerContext* context, const Request* request, Result* result) override {
    std::cout << "Run" << std::endl;
    std::cout << "req queryid :" << request->queryid() << std::endl << std::endl;
        
    query_result = "Under Construct";

    result->set_value(query_result);

    query_result = "";
    return Status::OK;
  }
  private:
    std::unordered_map<int, std::vector<std::string>> map;
    std::string query_result = "";
};


void RunServer() {
  std::string server_address("0.0.0.0:50051");
  SnippetSampleServiceImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case, it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char const *argv[]) {
  if(argc > 1){
    testrun(argv[1]);
  }else{
    RunServer();
  }

  return 0;
}



// void RunServer() {
//   std::string server_address("0.0.0.0:50051");
//   SnippetSampleServiceImpl service;

//   ServerBuilder builder;
//   // Listen on the given address without any authentication mechanism.
//   builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
//   // Register "service" as the instance through which we'll communicate with
//   // clients. In this case, it corresponds to an *synchronous* service.
//   builder.RegisterService(&service);
//   // Finally assemble the server.
//   std::unique_ptr<Server> server(builder.BuildAndStart());
//   std::cout << "Server listening on " << server_address << std::endl;

//   // Wait for the server to shutdown. Note that some other thread must be
//   // responsible for shutting down the server for this call to ever return.
//   server->Wait();
// }

// Status SnippetSampleServiceImpl::Run(ServerContext* context, const Request* request, Result* result)  {
//     std::cout << "Run" << std::endl;
//     std::cout << "req queryid :" << request->queryid() << std::endl << std::endl;
//     unordered_map<string,vector<vectortype>> RetTable;
//     RetTable = snippetmanager.ReturnResult(request->queryid());
//     result->set_value(query_result);

//     query_result = "";
//     for(auto i = RetTable.begin(); i != RetTable.end(); i++){
//       pair<string,vector<vectortype>> tmppair = *i;
//       cout << tmppair.first << " ";
//     }
//     cout << endl;
//     auto tmppair = *RetTable.begin();
//     int ColumnCount = tmppair.second.size();
//     // for(int i = 0; i < ColumnCount; i++){
//     //   for(auto j = RetTable.begin(); j != RetTable.end(); j++){
//     //     pair<string,vector<vectortype>> tmppair = *j;
//     //     // cout << tmppair.second[j] << " ";
//     //     if(tmppair.second[i].type == 1){
//     //       cout << any_cast<int>(tmppair.second[i]) << " ";
//     //     }else if(tmppair.second[i].type() == typeid(float&)){
//     //       cout << any_cast<float>(tmppair.second[i]) << " ";
//     //     }else{
//     //       cout << any_cast<string>(tmppair.second[i]) << " ";
//     //     }
//     //   }
//     //   cout << endl;
//     // }
//     return Status::OK;
//   }

// Status SnippetSampleServiceImpl::SetSnippet(ServerContext* context,
//                    ServerReaderWriter<Empty, Snippet>* stream) {
//     Snippet snippet;
//     queue<SnippetStruct> snippetqueue;
//     while (stream->Read(&snippet)) {
//       Empty empty;

//       std::cout << "SetSnippet" << std::endl;
//       std::cout << "queryid :" << snippet.query_id() << std::endl;
//       std::cout << "workid :" << snippet.work_id() << std::endl;
//       // std::cout << "snippet :" << snippet.() << std::endl << std::endl;

//       query_result += "w_id :";
//       query_result += std::to_string(snippet.work_id());    
//       // query_result += "snippet str :";
//       // query_result += snippet.snippet();
//       // query_result += "\n";

      
// 	  // Document doc;
// 	  // doc.Parse(snippet.snippet().c_str());
//     // SnippetStruct snippetdata(doc["table_block_list"]);
//     SnippetStruct snippetdata;
//     snippetdata.query_id = snippet.query_id();
//     snippetdata.work_id = snippet.work_id();
// 	  // snippetdata.tableAlias = doc["table_alias"].GetString();
// 	//   snippetdata.tablename = 
// 	  // for(int i = 0; i < doc["tablename"].Size(); i++){
// 		// snippetdata.tablename.push_back(doc["tablename"][i].GetString());
// 	  // }
// 	  // for(int i = 0; i < doc["column_alias"].Size(); i++){
// 		// snippetdata.column_alias.push_back(doc["column_alias"][i].GetString());
// 	  // }
// 	  // // snippetdata.table_filter = doc["table_filter"];
// 	  // for(int i = 0; i < doc["column_filtering"].Size(); i++){
// 		// snippetdata.columnFiltering.push_back(doc["column_filtering"][i].GetString());
// 	  // }
// 	  // for(int i = 0; i < doc["order_by"].Size(); i++){
// 		// snippetdata.orderBy.push_back(doc["order_by"][i].GetString());
// 	  // }
// 	  // for(int i = 0; i < doc["group_by"].Size(); i++){
// 		// snippetdata.groupBy.push_back(doc["group_by"][i].GetString());
// 	  // }
//     // for(int i = 0; i < doc["tableFilter"].Size(); i++){
//     // snippetdata.table_filter =  doc["tableFilter"];
//     // }
// 	  // AppendProjection(snippetdata, doc["Projection"]);
// 	  snippetqueue.push(snippetdata);
//       stream->Write(empty);
//     }
//     //여기 매니저로 보내는거
//     // snippetmanager.NewQuery(snippetqueue,bufma,tblManager,csdscheduler,csdmanager);
//     return Status::OK;
//   }




void testrun(string dirname){
  // cout << "[Storage Engine Input Interface] Start gRPC Server" << endl;
  tblManager.init_TableManager();
  time_t st = time(0);
  queue<SnippetStruct> snippetqueue;
  // cout << dirname << endl;
  string tmpstring;
  tmpstring = split(dirname,'h')[1];
  if(tmpstring.size() == 1){
    tmpstring = "0" + tmpstring;
  }
  // SnippetStructQueue tmpque;
  cout << "-----------------------------------:: STEP 2 ::-----------------------------------" << endl;
  // for(int i = 0; i < 13; i++){
    // cout << dirname << endl;
    int filecount = 0;
  for(const auto & file : filesystem::directory_iterator(dirname)){
    filecount++;
  }
  for(int i = 0; i < filecount; i++){
    // cout << "[Storage Engine Input Interface] Snippet " << i << " Recived" << endl;
    // string filename = "newtestsnippet/tpch05-" + to_string(i) + ".json";
    string filename = dirname + "/tpch" + tmpstring + "-" +to_string(i) + ".json";
    // cout << filename << endl;
    cout << "[Storage Engine Input Interface] Recived Snippet" << tmpstring << "-" << i << endl;
    int json_fd;
	  string json = "";
    json_fd = open(filename.c_str(),O_RDONLY);
    int res;
    char buf;

    
    while(1){
      res = read(json_fd,&buf,1);
      if(res == 0){
        break;
      }
      json += buf;
    }
    close(json_fd);
    Document doc;
    // cout << 1 << endl;
    // cout << json << endl;
	  doc.Parse(json.c_str());
    // cout << 2 << endl;
    // bool isjoin = false;
    // if(doc["type"] == 2){
    //   isjoin = true;
    // }
    Value& document = doc["snippet"];
    // SnippetStruct snippetdata(document["tableFilter"], document["blockList"]);
    // SnippetStruct snippetdata(document["blockList"]);
    // cout << 2 << endl;
    SnippetStruct snippetdata;
    // cout << snippetdata.table_filter.Size() << endl;
    snippetdata.snippetType = doc["type"].GetInt();
    // cout << 2 << endl;
    // for(int i = 0; i < document["tableFilter"].Size(); i++){}
    AppendTableFilter(snippetdata,document["tableFilter"]);
    // cout << 2 << endl;

    if(snippetdata.snippetType == 6){
      if(document.HasMember("dependency")){
        if(document["dependency"].HasMember("dependencyProjection")){
          AppendDependencyProjection(snippetdata,document["dependency"]["dependencyProjection"]);
        }
        if(document["dependency"].HasMember("dependencyFilter")){
          AppendDependencyFilter(snippetdata,document["dependency"]["dependencyFilter"]);
        }
      }
    }
    // cout << 2 << endl;
    // snippetdata.table_filter = doc["tableFilter"];

    for(int i = 0; i < document["tableName"].Size(); i++){
		  snippetdata.tablename.push_back(document["tableName"][i].GetString());
	  }
    // cout << 2 << endl;
	  for(int i = 0; i < document["columnAlias"].Size(); i++){
		  snippetdata.column_alias.push_back(document["columnAlias"][i].GetString());
	  }
    // cout << 2 << endl;
	  for(int i = 0; i < document["columnFiltering"].Size(); i++){
		  snippetdata.columnFiltering.push_back(document["columnFiltering"][i].GetString());
	  }
    // cout << 2 << endl;
    // cout << document.HasMember("orderBy") << endl;
    // cout << document["orderBy"].GetType() << endl;
    // for(int i = 0; i < document["orderBy"].Size(); i++){
    if(document.HasMember("orderBy")){
      for(int j = 0; j < document["orderBy"]["columnName"].Size(); j++){
          snippetdata.orderBy.push_back(document["orderBy"]["columnName"][j].GetString());
          snippetdata.orderType.push_back(document["orderBy"]["ascending"][j].GetInt());
        }
    }
    // }
    // cout << 2 << endl;
	  for(int i = 0; i < document["groupBy"].Size(); i++){
		  snippetdata.groupBy.push_back(document["groupBy"][i].GetString());
	  }
    for(int i = 0; i < document["tableCol"].Size(); i++){
		  snippetdata.table_col.push_back(document["tableCol"][i].GetString());
	  }
    for(int i = 0; i < document["tableOffset"].Size(); i++){
		  snippetdata.table_offset.push_back(document["tableOffset"][i].GetInt());
	  }
    for(int i = 0; i < document["tableOfflen"].Size(); i++){
		  snippetdata.table_offlen.push_back(document["tableOfflen"][i].GetInt());
	  }
    for(int i = 0; i < document["tableDatatype"].Size(); i++){
		  snippetdata.table_datatype.push_back(document["tableDatatype"][i].GetInt());
	  }
    // cout << 1 << endl;
    snippetdata.tableAlias = document["tableAlias"].GetString();
    // cout << document.HasMember("queryID") << endl;
    snippetdata.query_id = document["queryID"].GetInt();
    // cout << 4 << endl;
    snippetdata.work_id = document["workID"].GetInt();
    // cout << 5 << endl;
    AppendProjection(snippetdata, document["columnProjection"]);
    // cout << 6 << endl;
    // cout << 2 << endl;
    snippetqueue.push(snippetdata);
    // cout << snippetqueue.front().table_filter.Size() << endl;
    // tmpque.enqueue(snippetdata);
  }
  string LQNAME = snippetqueue.back().tableAlias;
  int Queryid = snippetqueue.back().query_id;
  snippetmanager.NewQuery(snippetqueue,bufma,tblManager,csdscheduler,csdmanager);
  // snippetmanager.NewQuery(tmpque,bufma,tblManager,csdscheduler,csdmanager);
  // thread t1 = thread(&SnippetManager::NewQuery,&snippetmanager,snippetqueue,bufma,tblManager,csdscheduler,csdmanager);
  // t1.detach();
  // cout << 1 << endl;
  // unordered_map<string,vector<vectortype>> rrr = bufma.GetTableData(4,"snippet4-12").table_data;
  // cout << 2 << endl;
  unordered_map<string,vector<vectortype>> RetTable = bufma.GetTableData(Queryid,LQNAME).table_data;
  // unordered_map<string,vector<vectortype>> RetTable = snippetmanager.ReturnResult(4);
  cout << "+-------------------------+" << endl;
  cout << " ";
  for(auto i = RetTable.begin(); i != RetTable.end(); i++){
    pair<string,vector<vectortype>> tmppair = *i;
    cout << tmppair.first << "      ";
  }
  cout << endl;


  auto tmppair = *RetTable.begin();
    int ColumnCount = tmppair.second.size();
    for(int i = 0; i < ColumnCount; i++){
      string tmpstring = " ";
      for(auto j = RetTable.begin(); j != RetTable.end(); j++){
        pair<string,vector<vectortype>> tmppair = *j;
        // cout << tmppair.second[j] << " ";
        if(tmppair.second[i].type == 1){
          tmpstring += to_string(tmppair.second[i].intvec) + " ";
          // cout << tmppair.second[i].intvec << endl;
        }else if(tmppair.second[i].type == 2){
          // std::stringstream sstream;
          // sstream << tmppair.second[i].floatvec;
          tmpstring +=  to_string(tmppair.second[i].floatvec) + " ";
          // cout << tmppair.second[i].floatvec << endl;
        }else{
          tmpstring += tmppair.second[i].strvec + " ";
          // cout << tmppair.second[i].strvec << endl;
        }
      }
      cout << tmpstring << endl;
      
    }
    cout << "+-------------------------+" <<endl;
  time_t fi = time(0);
  cout << "Total Time : " << fi-st << "s"<< endl;;
}


void AppendTableFilter(SnippetStruct &snippetdata,Value &filterdata){
  for(int i = 0; i < filterdata.Size(); i++){
    filterstruct tmpfilterst;
    if(filterdata[i].HasMember("LV")){
      for(int j = 0; j < filterdata[i]["LV"]["type"].Size(); j++){
        lv tmplv;
        tmplv.type.push_back(filterdata[i]["LV"]["type"][j].GetInt());
        tmplv.value.push_back(filterdata[i]["LV"]["value"][j].GetString());
        tmpfilterst.LV = tmplv;
      }
    }
    tmpfilterst.filteroper = filterdata[i]["Operator"].GetInt();
    if(filterdata[i].HasMember("RV")){
      for(int j = 0; j < filterdata[i]["RV"]["type"].Size(); j++){
        rv tmprv;
        tmprv.type.push_back(filterdata[i]["RV"]["type"][j].GetInt());
        tmprv.value.push_back(filterdata[i]["RV"]["value"][j].GetString());
        tmpfilterst.RV = tmprv;
      }
    }
    snippetdata.table_filter.push_back(tmpfilterst);
  }
  // cout << snippetdata.table_filter.size() << endl;
}

void AppendDependencyFilter(SnippetStruct &snippetdata,Value &filterdata){
  for(int i = 0; i < filterdata.Size(); i++){
    filterstruct tmpfilterst;
    if(filterdata[i].HasMember("LV")){
      for(int j = 0; j < filterdata[i]["LV"]["type"].Size(); j++){
        lv tmplv;
        tmplv.type.push_back(filterdata[i]["LV"]["type"][j].GetInt());
        tmplv.value.push_back(filterdata[i]["LV"]["value"][j].GetString());
        tmpfilterst.LV = tmplv;
      }
    }
    tmpfilterst.filteroper = filterdata[i]["Operator"].GetInt();
    if(filterdata[i].HasMember("RV")){
      for(int j = 0; j < filterdata[i]["RV"]["type"].Size(); j++){
        rv tmprv;
        tmprv.type.push_back(filterdata[i]["RV"]["type"][j].GetInt());
        tmprv.value.push_back(filterdata[i]["RV"]["value"][j].GetString());
        tmpfilterst.RV = tmprv;
      }
    }
    snippetdata.dependencyFilter.push_back(tmpfilterst);
  }
}


void AppendProjection(SnippetStruct &snippetdata,Value &Projectiondata){
	for(int i = 0; i < Projectiondata.Size(); i++){
    // cout << i << endl;
		vector<Projection> tmpVec;
    if(Projectiondata[i]["selectType"] == KETI_COUNTALL){
      Projection tmpPro;
      tmpPro.value = "4";
      tmpVec.push_back(tmpPro);
    }else{
      for(int j = 0; j < Projectiondata[i]["value"].Size(); j++){
        if(j == 0){
          Projection tmpPro;
          tmpPro.value = to_string(Projectiondata[i]["selectType"].GetInt());
          tmpVec.push_back(tmpPro);
        }
        
        Projection tmpPro;
        tmpPro.type = Projectiondata[i]["valueType"][j].GetInt();
        tmpPro.value = Projectiondata[i]["value"][j].GetString();
        tmpVec.push_back(tmpPro);
      }
    }
		snippetdata.columnProjection.push_back(tmpVec);
	}
}

void AppendDependencyProjection(SnippetStruct &snippetdata,Value &Projectiondata){
	for(int i = 0; i < Projectiondata.Size(); i++){
		vector<Projection> tmpVec;
    if(Projectiondata[i]["selectType"] == KETI_COUNTALL){
      Projection tmpPro;
      tmpPro.value = "4";
      tmpVec.push_back(tmpPro);
    }else{
      for(int j = 0; j < Projectiondata[i]["value"].Size(); j++){
        if(j == 0){
          Projection tmpPro;
          tmpPro.value = to_string(Projectiondata[i]["selectType"].GetInt());
          tmpVec.push_back(tmpPro);
        }
        
        Projection tmpPro;
        tmpPro.type = Projectiondata[i]["valueType"][j].GetInt();
        tmpPro.value = Projectiondata[i]["value"][j].GetString();
        tmpVec.push_back(tmpPro);
      }
    }
		snippetdata.dependencyProjection.push_back(tmpVec);
	}
}


// void testsetdata(){
//   vector<any> table1;
//   vector<any> table2;
//   for(int i = 0; i < 100; i++){
//     table1.push_back(i);
//     table2.push_back(i);
//   }

//   vector<any> table3;
//   vector<any> table4;
//   for(int i = 0; i < 100; i++){
//     table3.push_back(to_string(i) + "a");
//     table4.push_back(to_string(5 * i) + "a");
//   }


//   unordered_map<string,vector<any>> testtable;
//   testtable.insert(make_pair("testcolumn",table1));
//   testtable.insert(make_pair("testcolumn111",table3));
//   unordered_map<string,vector<any>> testtable2;
//   testtable2.insert(make_pair("testcolumn2",table2));
//   testtable2.insert(make_pair("testcolumn222",table4));

//   vector<string> tname;
//   vector<int> tlen;
//   bufma.InitWork(0,0,"testtable",tname,tlen,tlen,0);
  
//   // sleep(1);
//   bufma.SaveTableData(0,"testtable",testtable);
//   bufma.InitWork(0,1,"testtable2",tname,tlen,tlen,0);
//   bufma.SaveTableData(0,"testtable2",testtable2);
//   bufma.InitWork(0,2,"testtable3",tname,tlen,tlen,0);
// }

// void testjoin(){
//   StringBuffer snippetbuf;
//   snippetbuf.Clear();
//   Writer<StringBuffer> writer(snippetbuf);
//   writer.StartObject();
//   writer.Key("Snippet");
//   writer.StartArray();
//   writer.StartObject();
//   writer.Key("LV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn");
//   writer.EndArray();
//   writer.EndObject();
//   writer.Key("RV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn2");
//   writer.EndArray();
//   writer.EndObject();
//   writer.EndObject();

//   writer.StartObject();
//   writer.Key("LV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn111");
//   writer.EndArray();
//   writer.EndObject();
//   writer.Key("RV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn222");
//   writer.EndArray();
//   writer.EndObject();
//   writer.EndObject();

//   writer.EndArray();
//   writer.EndObject();
//   // a.table_filter = writer.;
//   string tmpsni = snippetbuf.GetString();

//   Document doc;
//   doc.Parse(tmpsni.c_str());
//   // a.table_filter = doc["Snippet"];
//   testsetdata();
//     cout << tmpsni << endl;
//   SnippetStruct a(doc["Snippet"],doc["Snippet"]);
//   vector<string> tablename;
//   tablename.push_back("testtable");
//   tablename.push_back("testtable2");
//   a.tablename = tablename;
//   a.query_id = 0;
//   a.tableAlias = "testtable3";

//   // auto ccc = bufma.GetTableData(0,"testtable");
//   // for(int i = 0; i < ccc.table_data.ss)
//   // cout << ccc.table_data.size() << endl;
//  cout << bufma.CheckTableStatus(0,"testtable") << endl;
//  cout << bufma.CheckTableStatus(0,"testtable2") << endl;


//   JoinTable(a,bufma);


//   unordered_map<string,vector<any>> savedTable;
//   savedTable = bufma.GetTableData(0,"testtable3").table_data;

//     for(auto i = savedTable.begin(); i != savedTable.end(); i++){
//       pair<string,vector<any>> tmppair = *i;
//       cout << tmppair.first << " ";
//     }
//     cout << endl;
//     auto tmppair = *savedTable.begin();
//     int ColumnCount = tmppair.second.size();
//     for(int i = 0; i < ColumnCount; i++){
//       for(auto j = savedTable.begin(); j != savedTable.end(); j++){
//         pair<string,vector<any>> tmppair = *j;
//         // cout << tmppair.second[j] << " ";
//         if(tmppair.second[i].type() == typeid(int&)){
//           cout << any_cast<int>(tmppair.second[i]) << " ";
//         }else if(tmppair.second[i].type() == typeid(float&)){
//           cout << any_cast<float>(tmppair.second[i]) << " ";
//         }else{
//           cout << any_cast<string>(tmppair.second[i]) << " ";
//         }
//       }
//       cout << endl;
//     }
// }


// void testaggregation(){
//   testsetdata();

//   StringBuffer snippetbuf;
//   snippetbuf.Clear();
//   Writer<StringBuffer> writer(snippetbuf);
//   writer.StartObject();
//   writer.Key("Snippet");
//   writer.StartArray();
//   writer.StartObject();
//   writer.Key("LV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn");
//   writer.EndArray();
//   writer.EndObject();
//   writer.Key("RV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn2");
//   writer.EndArray();
//   writer.EndObject();
//   writer.EndObject();

//   writer.EndArray();
//   writer.EndObject();
//   // a.table_filter = writer.;
//   string tmpsni = snippetbuf.GetString();

//   Document doc;
//   doc.Parse(tmpsni.c_str());

//   SnippetStruct a(doc["Snippet"],doc["Snippet"]);
//   vector<string> tmpvec;
//   tmpvec.push_back("testtable");
//   a.tablename = tmpvec;
//   vector<Projection> tmpvep;
//   Projection tmppro;
//   tmppro.value = "1";
//   tmpvep.push_back(tmppro);
//   Projection tmppro1;
//   tmppro1.value = "testcolumn";
//   tmppro1.type = 3;
//   tmpvep.push_back(tmppro1);
//   vector<vector<Projection>> tmptmp;
//   tmptmp.push_back(tmpvep);
//   a.columnProjection = tmptmp;
//   a.tableAlias = "testtable3";
//   a.query_id = 0;
//   vector<string> tmpcolal;
//   tmpcolal.push_back("test0");
//   a.column_alias = tmpcolal;



//   Aggregation(a,bufma,1);

//   unordered_map<string,vector<any>> savedTable;
//   savedTable = bufma.GetTableData(0,"testtable3").table_data;

//     for(auto i = savedTable.begin(); i != savedTable.end(); i++){
//       pair<string,vector<any>> tmppair = *i;
//       cout << tmppair.first << " ";
//     }
//     cout << endl;
//     auto tmppair = *savedTable.begin();
//     int ColumnCount = tmppair.second.size();
//     for(int i = 0; i < ColumnCount; i++){
//       for(auto j = savedTable.begin(); j != savedTable.end(); j++){
//         pair<string,vector<any>> tmppair = *j;
//         // cout << tmppair.second[j] << " ";
//         cout << tmppair.second[i].type().name() << endl;
//         if(tmppair.second[i].type() == typeid(int&)){
//           cout << any_cast<int>(tmppair.second[i]) << " ";
//         }else if(tmppair.second[i].type() == typeid(float&)){
//           cout << any_cast<float>(tmppair.second[i]) << " ";
//         }else{
//           cout << any_cast<string>(tmppair.second[i]) << " ";
//         }
//       }
//       cout << endl;
//     }
// }