#pragma once
#include <unistd.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <filesystem>

#include <vector>
#include <unordered_map>
#include <iostream>
#include <thread>
#include <atomic>
#include <queue>

#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <grpcpp/grpcpp.h>

#include <grpcpp/grpcpp.h>
#include <google/protobuf/empty.pb.h>
#include "snippet_sample.grpc.pb.h"

#include "WalManager.h"


// using std::io_errc::stream;
using grpc::ServerReaderWriter;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using snippetsample::SnippetSample;
using snippetsample::Snippet;
using snippetsample::Result;
using snippetsample::Request;
using google::protobuf::Empty;
// using std::filesystem::directory_iterator;


#include "rapidjson/document.h"

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

//#include "TableManager.h"
#include "CSDScheduler.h"
#include "keti_type.h"
#include "buffer_manager.h"
#include "SnippetManager.h"
#include "mergequerykmc.h"
// #include "SnippetManager.h"
#include "CSDManager.h"
// #include "snippet_sample.pb.h"
// #include "snippet_sample.grpc.pb.h"

#define PORT 8080
#define LBA2PBAPORT 8081
#define MAXLINE 256

using namespace rapidjson;
using namespace std;


using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using snippetsample::SnippetSample;
using snippetsample::Snippet;
using snippetsample::SnippetRequest;
using snippetsample::Result;
using snippetsample::Request;
using google::protobuf::Empty;



TableManager tblManager;
CSDManager csdmanager;
Scheduler csdscheduler(csdmanager);
BufferManager bufma(csdscheduler);
// SnippetManager snippetmanager(tblManager,csdscheduler,csdmanager);
SnippetManager snippetmanager;


// sumtest sumt;

// atomic<int> WorkID;
// unordered_map<string,int> fullquerymap;
// int totalrow = 0;
// int key;

void accept_connection(int server_fd);
//int my_LBA2PBA(char* req,char* res);
int my_LBA2PBA(std::string &req_json,std::string &res_json);

// class SnippetSampleServiceImpl final : public SnippetSample::Service {
//   Status SetSnippet(ServerContext* context,
//                    ServerReaderWriter<Result, SnippetRequest>* stream) override {
//     SnippetRequest snippetrequest;
//     while (stream->Read(&snippetrequest)) {
//       //Snippet snippet = snippetrequest.snippet();
//       std::string test_json;
//       google::protobuf::util::JsonPrintOptions options;
//       options.always_print_primitive_fields = true;
//       options.always_print_enums_as_ints = true;
//       google::protobuf::util::MessageToJsonString(snippetrequest,&test_json,options);
//       std::cout << "Recv Snippet to JSON" << std::endl;
//       //std::cout << "Snippet Type : " << snippetrequest.type() << std::endl;
//       std::cout << test_json << std::endl << std::endl;
      
//       if(snippetrequest.type() == 0) {
//         WalManager test(snippetrequest.snippet());
//         test.run();
//       }
//     }
//     return Status::OK;
//   }
//   Status Run(ServerContext* context, const Request* request, Result* result) override {
//     std::cout << "Run" << std::endl;
//     std::cout << "req queryid :" << request->queryid() << std::endl << std::endl;
        
//     query_result = "Under Construct";

//     result->set_value(query_result);

//     query_result = "";
//     return Status::OK;
//   }
//   private:
//     std::unordered_map<int, std::vector<std::string>> map;
//     std::string query_result = "";
// };

// void RunServer();
void AppendProjection(SnippetStruct &snippetdata,Value &Projectiondata);
void AppendDependencyProjection(SnippetStruct &snippetdata,Value &Projectiondata);
void AppendTableFilter(SnippetStruct &snippetdata,Value &filterdata);
void AppendDependencyFilter(SnippetStruct &snippetdata,Value &filterdata);

void testrun(string dirname);

// void testsetdata();

// void testjoin();

// void testaggregation();


class Storage_Engine_Interface {
	public:
		Storage_Engine_Interface(std::shared_ptr<Channel> channel) : stub_(SnippetSample::NewStub(channel)) {}

		void OpenStream(){
			streamcontext.reset(new ClientContext());
			stream = stub_->SetSnippet(streamcontext.get());
		}
		void SendSnippet(const SnippetRequest &snippet) {
      		stream->Write(snippet);
			std::cout << "send snippet" << std::endl;
		}
		void GetReturn(){		
			Result result;		
			stream->Read(&result);
			std::cout << result.value() << std::endl;
		}
		void CloseStream(){
			stream->WritesDone();
			Status status = stream->Finish();
			
			if (!status.ok()) {
				std::cout << status.error_code() << ": " << status.error_message() << std::endl;
				std::cout << "RPC failed";
			}
		}

		void Run(int queryid) {
			Request request;
			request.set_queryid(queryid);
    		ClientContext context;
			Result result;
			
			Status status = stub_->Run(&context, request, &result);

			std::cout << std::endl << "result : " << result.value() << "\n";
			
	  		if (!status.ok()) {
				std::cout << status.error_code() << ": " << status.error_message() << std::endl;
				std::cout << "RPC failed";
			}
		}

	private:
		std::unique_ptr<SnippetSample::Stub> stub_;
		std::unique_ptr<grpc::ClientReaderWriter<snippetsample::SnippetRequest, snippetsample::Result>> stream;
		std::unique_ptr<ClientContext> streamcontext;
};
