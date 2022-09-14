#pragma once

#include "snippet_sample.grpc.pb.h"

using snippetsample::Snippet;
using snippetsample::SnippetRequest;

class WalManager {
public:
	WalManager(const Snippet snippet) : snippet_(snippet){}
    void run(){        
        WalScan();
        //WalFilter();
        //WalMerge();
    }
private:
    void WalScan(); // get dirty rows
    void WalFilter(); // column and row filtering
    void WalMerge(); // projection
    bool hasVarcharMiddle();
    Snippet snippet_;
};