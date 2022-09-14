#pragma once

#include "snippet_sample.grpc.pb.h"

using snippetsample::Snippet;
using snippetsample::SnippetRequest;

class WalManager {
public:
	WalManager(const Snippet snippet) : snippet_(snippet){}
    void run(){        
        WalScan();
    }
private:
    void WalScan(); // get dirty rows
    Snippet snippet_;
};