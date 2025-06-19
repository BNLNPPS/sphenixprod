#ifndef MACRO_GETENTRIES_C
#define MACRO_GETENTRIES_C

#ifdef OFFLINE_MAIN
#include <frog/FROG.h>
R__LOAD_LIBRARY(libFROG.so)
#else 
#include "TTree.h"
#include "TLeaf.h"
#include "TFile.h"
#endif
void GetNumbers(const std::string &file)
{
  // prevent root to start gdb-backtrace.sh
  // in case of crashes, it hangs the condor job
  for (int i = 0; i < kMAXSIGNALS; i++)
  {
    gSystem->IgnoreSignal((ESignals)i);
  }  
#ifdef OFFLINE_MAIN
  gSystem->Load("libFROG.so");
  gSystem->Load("libg4dst.so");
  FROG *fr = new FROG();
  TFile *f = TFile::Open(fr->location(file));
#else
  TFile *f = TFile::Open(file.c_str());
#endif
  gSystem->RedirectOutput("numbers.txt");
  TTree *T = (TTree *) f->Get("T");
  int nEntries = -1;
  if (T) {
    T->SetScanField(0);
    nEntries = T->GetEntries();
    cout << "nEntries " << nEntries << endl;
    T->Scan("eventnumber","","",1,0);
    T->Scan("eventnumber","","",1,nEntries-1);
    return;
  }

  // Fallback
  cout << " ***** T is null, dummy values" << endl;
  cout << "dummyEntries " << nEntries << endl;
  cout << "dummyFirst "   << -1 << endl;
  cout << "dummyLast "    << -1 << endl;						
};              
#endif
